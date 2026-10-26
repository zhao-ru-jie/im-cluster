package com.github.im.contract;

/**
 * Redis key 空间：跨服务共享的数据契约，集中定义避免两侧各写一份字符串。
 *
 * <p>所有权（谁写谁读）在这里固定下来，是拆分后「数据一致性」的落点：
 * <table border="1">
 *   <caption>key 归属</caption>
 *   <tr><th>key</th><th>写方</th><th>读方</th></tr>
 *   <tr><td>{@link #GATEWAY_USER_PREFIX}{userId}</td><td>长连接服务</td><td>IM 服务（下行路由，SET 集合：多端可能同时在多个实例上）</td></tr>
 *   <tr><td>{@link #OFFLINE_PREFIX}{userId}</td><td>IM 服务</td><td>IM 服务（单聊离线帧队列，带 TTL/上限）</td></tr>
 *   <tr><td>{@link #OFFLINE_PTR_PREFIX}{userId}</td><td>IM 服务</td><td>IM 服务（群聊补发指针，带 TTL）</td></tr>
 *   <tr><td>{@link #CONV_MESSAGES_PREFIX}{conversationId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #SEQ_PREFIX}{conversationId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #IDEM_PREFIX}{from}|{clientMsgId}</td><td>IM 服务</td><td>IM 服务</td></tr>
 *   <tr><td>{@link #GROUP_MEMBERS_PREFIX}{groupId}</td><td>IM 服务</td><td>IM 服务（群聊扇出）</td></tr>
 * </table>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class RedisKeys {

    /**
     * 在线路由：SET{网关实例 ID}，带 TTL，由网关心跳续期。
     *
     * <p>是集合而不是单值：多端时同一用户可能同时连在多个网关实例上，
     * 下行必须逐个实例推送，再由各实例分发给本机上的设备。
     */
    public static final String GATEWAY_USER_PREFIX = "im:gateway:user:";
    /**
     * 单聊离线消息帧队列（List，带 TTL 与长度上限）。
     *
     * <p>只放「一条消息只有一个收件人」的场景；群聊走 {@link #OFFLINE_PTR_PREFIX}。
     */
    public static final String OFFLINE_PREFIX = "im:offline:";
    /**
     * 群聊离线补发指针（Hash：{@code conversationId -> 待补发的 seq}，带 TTL）。
     *
     * <p>为什么不给群聊存帧：一条群消息要写给所有不在线成员，500 人群 9 成不在线时，
     * 存帧就是每条消息 450 份相同内容，且随消息数线性增长。改成指针后，每个
     * 「用户 × 会话」只占一个 field，后续消息只把同一个 field 往前推，
     * <b>占用不随消息数增长</b>——补发时再按 seq 从会话历史重建帧。
     *
     * <p><b>注意前缀包含关系</b>：本前缀以 {@link #OFFLINE_PREFIX} 开头，所以任何
     * {@code im:offline:*} 的批量扫描/迁移/清理都会把指针 key 一起算进来。
     * 需要批量处理时请显式区分这两种 key，不要图省事直接用外层前缀。
     */
    public static final String OFFLINE_PTR_PREFIX = "im:offline:ptr:";
    /** 会话消息存储（List，按 seq 追加） */
    public static final String CONV_MESSAGES_PREFIX = "im:conv:msgs:";
    /**
     * 会话内序号分配器（String 计数器，每条消息一次原子 {@code INCR}）。
     *
     * <p>注意：它是<b>加速器而非权威</b>——丢了可以重建，重建下界取
     * {@code im_message} / 会话消息里的最大 seq（见 {@code SeqAllocator}）。
     */
    public static final String SEQ_PREFIX = "im:seq:";
    /**
     * 幂等表：{@code {from}|{clientMsgId} -> "{serverMsgId}|{seq}"}（24h TTL）。
     *
     * <p>写入时机是<b>落库成功之后</b>；读取则与 seq 发号合并在同一个 Lua 里（见 {@code SeqAllocator}）。
     */
    public static final String IDEM_PREFIX = "im:idem:";
    /** 群成员集合（Set），im.storage=redis 时的群成员存储 */
    public static final String GROUP_MEMBERS_PREFIX = "im:group:members:";

    private RedisKeys() {
    }
}
