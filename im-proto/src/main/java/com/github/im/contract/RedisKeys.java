package com.github.im.contract;

/**
 * Redis key 空间：跨服务共享的数据契约，集中定义避免两侧各写一份字符串。
 *
 * <p>所有权（谁写谁读）在这里固定下来，是拆分后「数据一致性」的落点：
 * <table border="1">
 *   <caption>key 归属</caption>
 *   <tr><th>key</th><th>写方</th><th>读方</th></tr>
 *   <tr><td>{@link #GATEWAY_USER_PREFIX}{userId}</td><td>长连接服务</td><td>IM 服务（下行路由，SET 集合：多端可能同时在多个实例上）</td></tr>
 *   <tr><td>{@link #OFFLINE_PREFIX}{userId}</td><td>IM 服务</td><td>IM 服务（<b>已废弃</b>的 List 队列，仅升级窗口内读一次）</td></tr>
 *   <tr><td>{@link #OFFLINE_QUEUE_PREFIX}{userId}</td><td>IM 服务</td><td>IM 服务（单聊离线 ZSet，score=seq，读不删）</td></tr>
 *   <tr><td>{@link #OFFLINE_CURSOR_PREFIX}{userId}:{deviceId}</td><td>IM 服务（网关 ack 驱动）</td><td>IM 服务（每设备已确认补发到的 seq）</td></tr>
 *   <tr><td>{@link #CLIENT_WATERMARK_PREFIX}{userId}:{deviceId}</td><td>IM 服务</td><td>IM 服务（客户端亲口确认的已见 seq，delivered 口径）</td></tr>
 *   <tr><td>{@link #OFFLINE_PTR_PREFIX}{userId}</td><td>IM 服务（网关 ack 驱动）</td><td>IM 服务（群聊补发指针，带 TTL）</td></tr>
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
     *
     * @deprecated 已被 {@link #OFFLINE_QUEUE_PREFIX}（ZSet）取代。本结构的问题是
     *         「读即消费」（{@code LRANGE + LTRIM} 原子移走），网关拿到帧后写失败就丢，
     *         属于 at-most-once。新队列按 seq 建索引 + per-device 游标，可以读不删、确认后再推进。
     *         旧 key 仍被读取一次（升级窗口内把积压的帧发掉），之后自然过期。
     */
    @Deprecated
    public static final String OFFLINE_PREFIX = "im:offline:";
    /**
     * 单聊离线消息队列（<b>ZSet</b>：{@code score = seq}，{@code member = 完整下行帧}）。
     *
     * <p>用 ZSet 而不是 List 的三个理由：
     * <ol>
     *   <li><b>能按 seq 定位</b>：补发起点是「该设备已确认到的 seq」，{@code ZRANGEBYSCORE key (cursor +inf}
     *       是 O(log N + M)，List 只能按下标读，而 {@code LTRIM} 一裁下标就漂移；</li>
     *   <li><b>读不删</b>：读的时候不动数据，由 {@link #OFFLINE_CURSOR_PREFIX} 的游标表达「谁读到哪了」，
     *       于是「读到了但没发出去」不会丢；</li>
     *   <li><b>天然有序</b>：按 seq 升序补发，不再依赖入队顺序（旧 List 的补发顺序是到达顺序，可能乱序）。</li>
     * </ol>
     *
     * <p>裁剪仍按条数：写入时 {@code ZREMRANGEBYRANK 0 -(max+1)} 保留最新 {@code max} 条，
     * 语义与旧 List 的 {@code LTRIM} 一致——超期/超量的用户放弃补发，历史仍在消息存储里。
     *
     * <p>前缀刻意与 {@link #OFFLINE_PREFIX} 不同（多一段 {@code q:}）：ZSet 与 List 同 key 会触发
     * WRONGTYPE，换名字比「删掉重建」更安全（重建会把升级前积压的离线消息丢掉）。
     */
    public static final String OFFLINE_QUEUE_PREFIX = "im:offline:q:";
    /**
     * 单聊离线补发的<b>设备级游标</b>（String：该设备已确认收到的最大 seq）。
     *
     * <p>per-device 而不是 per-user：手机和网页各自补到哪互不相干。用户级单一游标的后果是
     * 「先上线的设备一确认，另一个设备就永远补不到那段」——多端场景下等于丢消息。
     *
     * <p>游标<b>只前进</b>（{@code CAS}：只有新旧值都大于当前值才写），回退会导致重复补发死循环。
     */
    public static final String OFFLINE_CURSOR_PREFIX = "im:offline:cur:";
    /**
     * 客户端消费水位（Hash：{@code conversationId -> 客户端已确认收到的最大 seq}，per user+device）。
     *
     * <p>与 {@link #OFFLINE_CURSOR_PREFIX} 的区别：游标是「网关写进 socket 成功」，
     * 水位是「客户端亲口说收到了」（{@code sync} 帧上报）。前者是<b>至少一次</b>的保证，
     * 后者是<b>delivered</b> 的唯一可信口径——两者都需要，不能互相替代。
     *
     * <p>落 Redis 而不是只留进程内：进程重启就清空的表撑不起「已送达」这个语义，
     * 而它同时还是 seq 发号器在「消息存储与计数器一起丢」时唯一的下界来源（见 {@code SeqAllocator}）。
     */
    public static final String CLIENT_WATERMARK_PREFIX = "im:wm:";
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
