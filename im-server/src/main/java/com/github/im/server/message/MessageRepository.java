package com.github.im.server.message;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * 消息存储端口（IM 服务独占）。
 *
 * <p>阶段 4 的存储迁移就是用这个端口做的：业务侧只依赖 {@code append / recent}，
 * 实现从 {@link RedisMessageRepository}（默认，零外部数据库依赖）切到
 * {@code com.github.im.storage.jdbc.JdbcMessageRepository}（MySQL，生产形态，独立模块
 * im-storage-jdbc）只需改 {@code im.storage}。
 *
 * <p><b>一致性约定</b>：{@code append} 必须先于下发（write-ahead）。
 * 实现抛异常 = 整条消息受理失败（返回可重试错误码），绝不允许「推成功但没落库」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface MessageRepository {

    /** 追加一条已受理消息（同 conversationId+seq 重复写入视为实现层错误） */
    void append(StoredMessage message);

    /** 取会话最近 limit 条，按 seq 升序返回 */
    List<StoredMessage> recent(String conversationId, int limit);

    /**
     * 取会话中 {@code seq > afterSeq} 的至多 {@code limit} 条，按 seq 升序返回。
     *
     * <p>群聊离线补发的数据来源：离线只记「这个人在这个会话上补到哪了」，
     * 真正要发的帧在补发时按 seq 区间从这里取出再重建，避免按人存帧导致的
     * 「一条群消息 = 不在线人数份」放大。
     *
     * <p>返回条数 &lt; {@code limit} 代表这段区间已经取完（补发方据此判断是否可以清掉指针）。
     */
    List<StoredMessage> since(String conversationId, long afterSeq, int limit);

    /**
     * 会话已落库的最大 seq；{@code 0} = 无历史。
     *
     * <p>这是 seq 发号器的<b>下界来源</b>：Redis 里的 {@code im:seq:{conversationId}} 只是
     * 加速器，可能丢；已落库分区的数据才是「发出去的号」的最终证据。
     * 因此发号前要周期性（每 100 条一次）用它兜底，保证序号只前进不回退。
     */
    long maxSeq(String conversationId);

    // ==== 消息生命周期（撤回 / 删除） ====

    /**
     * 按 {@code conversationId + seq} 取单条消息。
     *
     * <p>撤回要校验「操作者是不是发送方」「是否还在可撤回时限内」，两者都需要原文。
     * 取不到（已被 trim / 从未存在）返回 {@link Optional#empty()}。
     */
    Optional<StoredMessage> find(String conversationId, long seq);

    /**
     * 撤回一条消息（会话级，所有人可见）。
     *
     * <p>语义是<b>幂等</b>的：首次撤回返回 {@code true}，重复撤回返回 {@code false}——
     * 重复撤回不该再广播一次变更帧。
     */
    boolean recall(String conversationId, long seq);

    /** 这批 seq 里哪些已被撤回（渲染时统一脱敏，避免逐条查询） */
    Set<Long> recalledSeqs(String conversationId, Collection<Long> seqs);

    /** 本地删除一条消息（用户级：只影响这个用户自己的视图） */
    void markDeleted(String userId, String conversationId, long seq);

    /** 该用户在这个会话里删掉的全部 seq（读路径过滤用） */
    Set<Long> deletedSeqs(String userId, String conversationId);
}
