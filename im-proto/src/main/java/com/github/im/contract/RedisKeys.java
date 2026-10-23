package com.github.im.contract;

/**
 * Redis key 空间：跨服务共享的数据契约，集中定义避免两侧各写一份字符串。
 *
 * <p>所有权（谁写谁读）在这里固定下来，是拆分后「数据一致性」的落点：
 * <table border="1">
 *   <caption>key 归属</caption>
 *   <tr><th>key</th><th>写方</th><th>读方</th></tr>
 *   <tr><td>{@link #GATEWAY_USER_PREFIX}{userId}</td><td>长连接服务</td><td>IM 服务（下行路由）</td></tr>
 *   <tr><td>{@link #OFFLINE_PREFIX}{userId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #CONV_MESSAGES_PREFIX}{conversationId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #SEQ_PREFIX}{conversationId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #IDEM_PREFIX}{from}|{clientMsgId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #GROUP_MEMBERS_PREFIX}{groupId}</td><td>IM 服务</td><td>IM 服务（群聊扇出）</td></tr>
 * </table>
 */
public final class RedisKeys {

    /** 在线路由：gw 实例 ID，带 TTL，由网关心跳续期 */
    public static final String GATEWAY_USER_PREFIX = "im:gw:user:";
    /** 离线消息帧队列（List） */
    public static final String OFFLINE_PREFIX = "im:offline:";
    /** 会话消息存储（List，按 seq 追加） */
    public static final String CONV_MESSAGES_PREFIX = "im:conv:msgs:";
    /**
     * 会话内序号分配器（String 计数器，每条消息一次原子 {@code INCR}）。
     *
     * <p>注意：它是<b>加速器而非权威</b>——丢了可以重建，重建下界取
     * {@code im_message} / 会话消息里的最大 seq（见 {@code SeqAllocator}）。
     */
    public static final String SEQ_PREFIX = "im:seq:";
    /** 幂等表：{from}|{clientMsgId} -> seq */
    public static final String IDEM_PREFIX = "im:idem:";
    /** 群成员集合（Set），im.storage=redis 时的群成员存储 */
    public static final String GROUP_MEMBERS_PREFIX = "im:group:members:";

    private RedisKeys() {
    }
}
