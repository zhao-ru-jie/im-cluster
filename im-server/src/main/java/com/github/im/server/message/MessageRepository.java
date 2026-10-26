package com.github.im.server.message;

import java.util.List;

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
}
