package com.github.im.storage.mongodb;

import com.github.im.server.message.MessageRepository;
import com.github.im.server.message.StoredMessage;
import com.github.im.storage.mongodb.entity.MessageDeleteEntity;
import com.github.im.storage.mongodb.entity.MessageEntity;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * MongoDB 版消息存储（{@code im.storage=mongodb}），基于实体映射（{@code @Document} + {@code MongoTemplate}）。
 *
 * <pre>
 * im_message         一条文档 = 一条已受理消息，_id = serverMsgId（UUIDv7）
 * im_message_delete  一条文档 = 某人删掉了某会话的某条，_id = userId:conversationId:seq
 * </pre>
 *
 * <h2>身份是 serverMsgId，(conversationId, seq) 是定位</h2>
 *
 * <p>{@code _id = serverMsgId}：它才是这条消息的身份（受理响应、下行帧、客户端回执都认它），
 * 唯一性由 {@code _id} 索引天然保证，也因此不再需要额外的 {@code uk_msg_server_id}。
 * 理由与「为什么不用会话 + seq 拼 _id」见 {@code MessageEntity} 的类注释。
 *
 * <p>「同一 (conversationId, seq) 不会被写两次」由 {@code uk_msg_conv_seq} 唯一索引兜住——
 * 这条索引本来就必须存在（范围扫描用），于是「发号器不重复」和「消息不重复落库」
 * 是同一个索引保证的两件事，不再需要第二道冗余约束。
 *
 * <h2>与 MySQL 形态的差异</h2>
 *
 * <ul>
 *   <li>没有 {@code group commit}：MongoDB 的单文档写本身就是原子的，并发吞吐靠连接池与
 *       WiredTiger 的组提交，应用侧再合批只会增加「已 ack 未落库」的窗口；</li>
 *   <li>没有应用侧分片路由：MongoDB 的分片是集群能力，分片键取 {@code conversationId}
 *       （与 {@code ShardRouter} 的选择一致：消息读写全部是「按会话 + seq」），业务代码不感知分片。</li>
 * </ul>
 *
 * <p>{@code seq} 的语义与另两种形态完全一致：由 {@code SeqAllocator} 在 Redis 里原子递增分配，
 * 并周期性用 {@link #maxSeq(String)} 兜底，保证「序号只前进不回退」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class MongoMessageRepository implements MessageRepository {

    private final MongoTemplate mongo;

    public MongoMessageRepository(MongoTemplate mongo) {
        this.mongo = mongo;
    }

    // ==== 写路径 ====

    /**
     * 落库一条消息。
     *
     * <p>重复的 {@code serverMsgId}（撞 {@code _id}）或重复的 {@code (conversationId, seq)}
     * （撞 {@code uk_msg_conv_seq}）都会抛 {@code DuplicateKeyException}——
     * 这是<b>期望行为</b>：端口约定「同一 seq 重复写入视为实现层错误」，
     * 静默 upsert 会把发号器/幂等逻辑的 bug 藏起来（用户看到的是消息被悄悄改写）。
     */
    @Override
    public void append(StoredMessage message) {
        mongo.insert(MessageEntity.from(message));
    }

    // ==== 读路径 ====

    /** 走 {@code uk_msg_conv_seq} 的索引尾取最大 seq，不扫全集合、不做聚合 */
    @Override
    public long maxSeq(String conversationId) {
        Query query = Query.query(Criteria.where("conversationId").is(conversationId))
                .with(Sort.by(Sort.Direction.DESC, "seq"))
                .limit(1);
        query.fields().include("seq");
        MessageEntity last = mongo.findOne(query, MessageEntity.class);
        return last == null ? 0L : last.getSeq();
    }

    @Override
    public List<StoredMessage> recent(String conversationId, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        // 先按 seq 倒序取最近 limit 条，再反转成升序返回（与另两种形态的返回约定一致）
        Query query = Query.query(Criteria.where("conversationId").is(conversationId))
                .with(Sort.by(Sort.Direction.DESC, "seq"))
                .limit(limit);
        List<StoredMessage> messages = toMessages(mongo.find(query, MessageEntity.class));
        Collections.reverse(messages);
        return messages;
    }

    /**
     * {@code seq > afterSeq} 的升序区间（群聊离线补发用）。
     *
     * <p>命中 {@code uk_msg_conv_seq} 的范围扫描，代价只与「本次要补多少条」相关，
     * 与会话总量无关——这正是把索引定在 {@code (conversationId, seq)} 上的原因。
     */
    @Override
    public List<StoredMessage> since(String conversationId, long afterSeq, int limit) {
        if (limit <= 0) {
            return List.of();
        }
        Query query = Query.query(Criteria.where("conversationId").is(conversationId)
                        .and("seq").gt(afterSeq))
                .with(Sort.by(Sort.Direction.ASC, "seq"))
                .limit(limit);
        return toMessages(mongo.find(query, MessageEntity.class));
    }

    // ==== 消息生命周期（撤回 / 删除） ====

    /**
     * 按 {@code (conversationId, seq)} 定位单条：撤回校验（是不是发送方、是否还在时限内）
     * 只需要这一条。
     *
     * <p>走 {@code uk_msg_conv_seq} 的一次索引 seek（消息身份是 serverMsgId，不是这个组合键）。
     */
    @Override
    public Optional<StoredMessage> find(String conversationId, long seq) {
        if (conversationId == null || conversationId.isBlank() || seq <= 0) {
            return Optional.empty();
        }
        MessageEntity found = mongo.findOne(locate(conversationId, seq), MessageEntity.class);
        return found == null ? Optional.empty() : Optional.of(found.toStoredMessage());
    }

    /**
     * 撤回：用「带条件的单文档更新」把幂等做进数据库。
     *
     * <p>{@code recalledAt} 不存在或为 null 时才写，因此重复撤回返回 {@code false}
     * （不需要额外的「撤回事件表」），并发撤回也只有一次会赢。
     */
    @Override
    public boolean recall(String conversationId, long seq) {
        if (conversationId == null || conversationId.isBlank() || seq <= 0) {
            return false;
        }
        Query query = Query.query(Criteria.where("conversationId").is(conversationId)
                .and("seq").is(seq)
                .and("recalledAt").is(null));
        Update update = new Update().set("recalledAt", System.currentTimeMillis());
        return mongo.updateFirst(query, update, MessageEntity.class).getModifiedCount() > 0;
    }

    /** 批量判定是否已撤回：一次 {@code $in} + 非 null 判定，避免逐条往返 */
    @Override
    public Set<Long> recalledSeqs(String conversationId, Collection<Long> seqs) {
        if (conversationId == null || conversationId.isBlank() || seqs == null || seqs.isEmpty()) {
            return Set.of();
        }
        List<Long> wanted = new ArrayList<>(new LinkedHashSet<>(seqs));
        wanted.removeIf(seq -> seq == null || seq <= 0);
        if (wanted.isEmpty()) {
            return Set.of();
        }
        Query query = Query.query(Criteria.where("conversationId").is(conversationId)
                .and("seq").in(wanted)
                .and("recalledAt").ne(null));
        query.fields().include("seq");
        Set<Long> recalled = new LinkedHashSet<>();
        for (MessageEntity entity : mongo.find(query, MessageEntity.class)) {
            recalled.add(entity.getSeq());
        }
        return recalled;
    }

    /**
     * 用户级本地删除：{@code _id} 是确定性拼接，所以 {@code $setOnInsert} 天然幂等——
     * 已删过就什么都不改（不刷新删除时间），没删过才插入。
     */
    @Override
    public void markDeleted(String userId, String conversationId, long seq) {
        if (userId == null || userId.isBlank() || conversationId == null || conversationId.isBlank() || seq <= 0) {
            return;
        }
        Query query = Query.query(Criteria.where("id").is(MessageDeleteEntity.idOf(userId, conversationId, seq)));
        Update update = new Update()
                .setOnInsert("userId", userId)
                .setOnInsert("conversationId", conversationId)
                .setOnInsert("seq", seq)
                .setOnInsert("createdAt", System.currentTimeMillis());
        mongo.upsert(query, update, MessageDeleteEntity.class);
    }

    @Override
    public Set<Long> deletedSeqs(String userId, String conversationId) {
        if (userId == null || userId.isBlank() || conversationId == null || conversationId.isBlank()) {
            return Set.of();
        }
        Query query = Query.query(Criteria.where("userId").is(userId)
                .and("conversationId").is(conversationId));
        query.fields().include("seq");
        Set<Long> seqs = new LinkedHashSet<>();
        for (MessageDeleteEntity entity : mongo.find(query, MessageDeleteEntity.class)) {
            seqs.add(entity.getSeq());
        }
        return seqs;
    }

    /** 单条定位条件：{@code (conversationId, seq)} 命中唯一索引 {@code uk_msg_conv_seq} */
    private static Query locate(String conversationId, long seq) {
        return Query.query(Criteria.where("conversationId").is(conversationId).and("seq").is(seq));
    }

    private static List<StoredMessage> toMessages(List<MessageEntity> entities) {
        List<StoredMessage> messages = new ArrayList<>(entities.size());
        for (MessageEntity entity : entities) {
            messages.add(entity.toStoredMessage());
        }
        return messages;
    }
}
