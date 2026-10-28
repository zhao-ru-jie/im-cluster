package com.github.im.storage.jdbc;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.im.contract.RedisKeys;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.message.StoredMessage;
import com.github.im.server.shard.ShardRouter;
import com.github.im.storage.jdbc.entity.MessageDeleteEntity;
import com.github.im.storage.jdbc.entity.MessageEntity;
import com.github.im.storage.jdbc.mapper.MessageDeleteMapper;
import com.github.im.storage.jdbc.mapper.MessageMapper;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * MySQL 版消息存储（生产形态，`im.storage=jdbc`），基于 MyBatis-Plus。
 *
 * <p>表结构见 {@code db/schema.sql}，三条关键索引：
 * <ul>
 *   <li>{@code uk_msg_idem (from_user, client_msg_id)} —— 幂等的<b>最终防线</b>：
 *       Redis 幂等表 TTL 到期或丢数据时，靠唯一索引仍能拒绝重复落库；</li>
 *   <li>{@code uk_msg_conv_seq (conversation_id, seq)} —— 保序、漫游拉取、群聊离线补发的范围扫描主索引；</li>
 *   <li>{@code uk_msg_server_id (server_msg_id)} —— 断言 UUIDv7 生成器不重复。</li>
 * </ul>
 *
 * <p>为什么用 MyBatis-Plus 而不是 {@code JdbcTemplate}：三种读路径（最近 N 条、seq 之后 N 条、
 * 会话最大 seq）都是单表条件查询，{@code LambdaQueryWrapper} 能在编译期校验字段，
 * 列名/字段名对不上时是编译错误而不是运行期 SQL 异常；写路径的「合批提交」保留在
 * {@link JdbcBatchAppender}（见它的类注释），与 ORM 选型无关。
 *
 * <p>注意：{@code seq} 由 {@code SeqAllocator} 逐条 Redis 原子递增分配，并周期性用
 * {@link #maxSeq(String)} 读到的「已落库最大 seq」当下界。因此即使 Redis 里的序号计数器被清空，
 * 新号也从历史之后接着发，不会撞 {@code uk_msg_conv_seq}。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcMessageRepository implements MessageRepository, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JdbcMessageRepository.class);

    private final MessageMapper mapper;
    /** 本地删除记录（用户级）与撤回（im_message 上的一列）分开，理由见 schema.sql */
    private final MessageDeleteMapper deleteMapper;
    /** 会话分片路由；为 null 时按「未分片」处理（单表 im_message） */
    private final ShardRouter shardRouter;
    private final JdbcBatchAppender appender;

    public JdbcMessageRepository(MessageMapper mapper, MessageDeleteMapper deleteMapper,
                                 SqlSessionFactory sqlSessionFactory, boolean batchEnabled,
                                 ShardRouter shardRouter) {
        this.mapper = mapper;
        this.deleteMapper = deleteMapper;
        this.shardRouter = shardRouter;
        this.appender = new JdbcBatchAppender(sqlSessionFactory, mapper, batchEnabled);
    }

    /** 落库并在返回前确认已提交（批处理时提交整批后才返回） */
    @Override
    public void append(StoredMessage message) {
        if (log.isDebugEnabled() && shardRouter != null) {
            // 分片号在这里落脚：排查「同一会话的数据为什么分散在不同片」时，
            // 这一行同时给出 MySQL 分片与 Redis key（两者必须按同一个 conversationId 切）
            log.debug("[shard] conv={} seq={} mysql={} redis={}",
                    message.conversationId(), message.seq(),
                    shardRouter.messageTableOf(message.conversationId()),
                    RedisKeys.convMessages(message.conversationId()));
        }
        appender.append(message);
    }

    /** 关闭时把队列里还没提交的消息刷完，避免「已回 ack 但没落库」 */
    @Override
    public void close() {
        appender.close();
    }

    /**
     * 会话已落库的最大 seq。
     *
     * <p>走 {@code uk_msg_conv_seq (conversation_id, seq)} 的索引尾：{@code ORDER BY seq DESC LIMIT 1}
     * 是一次索引反向扫描，不需要 {@code MAX()} 聚合（也就不会回表）。
     */
    @Override
    public long maxSeq(String conversationId) {
        List<MessageEntity> rows = mapper.selectList(Wrappers.<MessageEntity>lambdaQuery()
                .select(MessageEntity::getSeq)
                .eq(MessageEntity::getConversationId, conversationId)
                .orderByDesc(MessageEntity::getSeq)
                .last("LIMIT 1"));
        if (rows.isEmpty() || rows.getFirst().getSeq() == null) {
            return 0L;
        }
        return rows.getFirst().getSeq();
    }

    /**
     * 向上翻页：{@code seq < beforeSeq} 的升序区间（{@code beforeSeq <= 0} = 取最新一页）。
     *
     * <p>SQL 先倒序取 limit 条再在内存里反转——{@code ORDER BY seq DESC LIMIT n} 才能让
     * {@code uk_msg_conv_seq} 直接从索引尾部往前扫；改成正序取前 n 条会变成「从会话开头扫」。
     */
    @Override
    public List<StoredMessage> before(String conversationId, long beforeSeq, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<MessageEntity> rows = mapper.selectList(Wrappers.<MessageEntity>lambdaQuery()
                .eq(MessageEntity::getConversationId, conversationId)
                .lt(beforeSeq > 0, MessageEntity::getSeq, beforeSeq)
                .orderByDesc(MessageEntity::getSeq)
                .last("LIMIT " + limit));
        List<StoredMessage> messages = new ArrayList<>(rows.size());
        for (MessageEntity row : rows) {
            messages.add(row.toStoredMessage());
        }
        Collections.reverse(messages);
        return messages;
    }

    /**
     * 群聊离线补发用：{@code seq > afterSeq} 的升序区间。
     *
     * <p>直接命中 {@code uk_msg_conv_seq} 的范围扫描，代价只与「本次要补多少条」相关，
     * 与会话总量无关。
     */
    @Override
    public List<StoredMessage> since(String conversationId, long afterSeq, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        List<MessageEntity> rows = mapper.selectList(Wrappers.<MessageEntity>lambdaQuery()
                .eq(MessageEntity::getConversationId, conversationId)
                .gt(MessageEntity::getSeq, afterSeq)
                .orderByAsc(MessageEntity::getSeq)
                .last("LIMIT " + limit));
        List<StoredMessage> messages = new ArrayList<>(rows.size());
        for (MessageEntity row : rows) {
            messages.add(row.toStoredMessage());
        }
        return messages;
    }

    // ==== 消息生命周期（撤回 / 删除） ====

    @Override
    public Optional<StoredMessage> find(String conversationId, long seq) {
        List<MessageEntity> rows = mapper.selectList(Wrappers.<MessageEntity>lambdaQuery()
                .eq(MessageEntity::getConversationId, conversationId)
                .eq(MessageEntity::getSeq, seq)
                .last("LIMIT 1"));
        return rows.isEmpty() ? Optional.empty() : Optional.of(rows.getFirst().toStoredMessage());
    }

    /**
     * 撤回：{@code recalled_at IS NULL} 让 UPDATE 自带幂等。
     *
     * <p>这也是为什么撤回是 {@code im_message} 上的一列而不是独立事件表——
     * 一次带条件的 UPDATE 就能同时回答「撤回了没」和「这次是不是首次」，
     * 而事件表要先查再插，多一次往返还多一处并发窗口。
     */
    @Override
    public boolean recall(String conversationId, long seq) {
        if (seq <= 0) {
            return false;
        }
        return mapper.markRecalled(conversationId, seq, System.currentTimeMillis()) > 0;
    }

    @Override
    public Set<Long> recalledSeqs(String conversationId, Collection<Long> seqs) {
        if (seqs == null || seqs.isEmpty()) {
            return Set.of();
        }
        List<MessageEntity> rows = mapper.selectList(Wrappers.<MessageEntity>lambdaQuery()
                .select(MessageEntity::getSeq)
                .eq(MessageEntity::getConversationId, conversationId)
                .in(MessageEntity::getSeq, seqs)
                .isNotNull(MessageEntity::getRecalledAt));
        Set<Long> recalled = new LinkedHashSet<>(rows.size());
        for (MessageEntity row : rows) {
            if (row.getSeq() != null) {
                recalled.add(row.getSeq());
            }
        }
        return recalled;
    }

    @Override
    public void markDeleted(String userId, String conversationId, long seq) {
        if (seq <= 0 || userId == null || userId.isBlank()) {
            return;
        }
        deleteMapper.insertIgnore(userId, conversationId, seq, System.currentTimeMillis());
    }

    @Override
    public Set<Long> deletedSeqs(String userId, String conversationId) {
        if (userId == null || userId.isBlank()) {
            return Set.of();
        }
        List<MessageDeleteEntity> rows = deleteMapper.selectList(
                Wrappers.<MessageDeleteEntity>lambdaQuery()
                        .select(MessageDeleteEntity::getSeq)
                        .eq(MessageDeleteEntity::getUserId, userId)
                        .eq(MessageDeleteEntity::getConversationId, conversationId));
        Set<Long> seqs = new LinkedHashSet<>(rows.size());
        for (MessageDeleteEntity row : rows) {
            if (row.getSeq() != null) {
                seqs.add(row.getSeq());
            }
        }
        return seqs;
    }
}
