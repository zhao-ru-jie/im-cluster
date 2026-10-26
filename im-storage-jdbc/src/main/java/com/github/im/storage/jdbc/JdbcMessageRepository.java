package com.github.im.storage.jdbc;

import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.message.StoredMessage;
import com.github.im.storage.jdbc.entity.MessageEntity;
import com.github.im.storage.jdbc.mapper.MessageMapper;
import org.apache.ibatis.session.SqlSessionFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    private final MessageMapper mapper;
    private final JdbcBatchAppender appender;

    public JdbcMessageRepository(MessageMapper mapper, SqlSessionFactory sqlSessionFactory, boolean batchEnabled) {
        this.mapper = mapper;
        this.appender = new JdbcBatchAppender(sqlSessionFactory, mapper, batchEnabled);
    }

    /** 落库并在返回前确认已提交（批处理时提交整批后才返回） */
    @Override
    public void append(StoredMessage message) {
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

    @Override
    public List<StoredMessage> recent(String conversationId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        // 先按 seq 倒序取最近 limit 条，再反转成升序返回
        List<MessageEntity> rows = mapper.selectList(Wrappers.<MessageEntity>lambdaQuery()
                .eq(MessageEntity::getConversationId, conversationId)
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
}
