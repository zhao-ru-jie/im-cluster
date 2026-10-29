package com.github.im.contract;

/**
 * Redis key 空间：跨服务共享的数据契约，<b>所有 key 的唯一构造点</b>。
 *
 * <h2>两级分片维度</h2>
 *
 * <p>每个 key 都带一个 <b>hash tag</b>，只有两个维度：
 * <ul>
 *   <li><b>会话维度</b> {@code {conversationId}}：{@code im:conv:{c}:meta} / {@code :msgs} /
 *       {@code :seq} / {@code :recalled} / {@code :idem:*}</li>
 *   <li><b>用户维度</b> {@code {userId}}：{@code im:user:{u}:convs} / {@code :read} /
 *       {@code :wm:*} / {@code :offline:*} / {@code :friends} …</li>
 * </ul>
 *
 * <p>为什么必须统一：Cluster 下多 key 脚本/pipeline 要求所有 key 落在同一 slot。
 * 改造前 {@code im:seq:{c}} 与 {@code im:idem:{from}|{msgId}} 都没有 tag，
 * 于是 {@code SeqAllocator} 的「幂等查询 + 发号」合并脚本在 Cluster 上会直接
 * {@code CROSSSLOT}（单机 Redis 无感，所以一直没暴露）。统一 tag 之后：
 *
 * <ul>
 *   <li>「幂等查询 + 发号」天然同 slot；</li>
 *   <li>{@code meta + msgs + seq + recalled} 同 slot，将来可以把「受理一条消息」
 *       （取号 → 追加 → 更新 meta → 记撤回）合并成一次 Lua，省掉 3 次往返；</li>
 *   <li>一个用户的所有离线相关 key 同 slot（队列/游标/指针经常在同一个脚本里出现）。</li>
 * </ul>
 *
 * <h2>租户（appId）维度</h2>
 *
 * <p>key 的最前面是<b>可配置的根</b>：默认 {@code im}，多租户部署时设
 * {@code im.tenant.app-id=acme} 得到 {@code acme:im:…}。
 *
 * <p>为什么是<b>进程级</b>配置而不是每条请求带 appId：
 * <ul>
 *   <li>同一个 Redis 里用前缀隔离租户是"伪隔离"——备份、迁移、慢查询、内存水位全都分不开，
 *       真多租户一定是按实例或按库隔离；</li>
 *   <li>进程级配置不需要把 appId 贯穿 proto、方法签名与所有异步线程（项目里的推送是
 *       虚拟线程池，ThreadLocal 上下文在那里会静默丢失，那是最难查的一类 bug）。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class RedisKeys {

    private static final String DEFAULT_ROOT = "im";

    /** 进程级根前缀，启动时由配置设置一次；默认与原行为一致 */
    private static volatile String root = DEFAULT_ROOT;

    private RedisKeys() {
    }

    /**
     * 设置租户根前缀（启动时调用一次）。
     *
     * @param appId 租户标识；空白表示回到默认根 {@code im}
     */
    public static void configure(String appId) {
        root = (appId == null || appId.isBlank()) ? DEFAULT_ROOT : appId.trim() + ":im";
    }

    /** 当前根前缀（观测/测试用） */
    public static String root() {
        return root;
    }

    // ==== 会话维度（tag = {conversationId}） ====

    /** 会话元信息（Hash：type / last_seq / last_time / last_from / last_digest / last_msg_id） */
    public static String convMeta(String conversationId) {
        return root + ":conv:" + tag(conversationId) + ":meta";
    }

    /** 会话消息存储（Redis 形态是 List，按 seq 追加；jdbc 形态见 im_message） */
    public static String convMessages(String conversationId) {
        return root + ":conv:" + tag(conversationId) + ":msgs";
    }

    /**
     * 会话内序号分配器（String 计数器）。
     *
     * <p>它是<b>加速器而非权威</b>：丢了可以重建，下界取 {@code im_message} 里的最大 seq。
     */
    public static String convSeq(String conversationId) {
        return root + ":conv:" + tag(conversationId) + ":seq";
    }

    /** 已撤回的 seq 集合（Set）；与 {@link #convMessages} 同 slot，读路径可以一次拿齐 */
    public static String convRecalled(String conversationId) {
        return root + ":conv:" + tag(conversationId) + ":recalled";
    }

    /**
     * 幂等表（String：{@code "{serverMsgId}|{seq}"}，24h TTL）。
     *
     * <p>挂在<b>会话</b>维度上而不是只按 {@code from|clientMsgId}：它要与 {@link #convSeq}
     * 出现在同一个发号脚本里，两者必须同 slot。语义上也没有损失——同一个 clientMsgId
     * 在同一个会话内重复才算重复提交。
     */
    public static String convIdem(String conversationId, String idempotencyKey) {
        return root + ":conv:" + tag(conversationId) + ":idem:" + idempotencyKey;
    }

    // ==== 用户维度（tag = {userId}） ====

    /** 用户的会话列表（ZSet：member=conversationId，score=最后一条消息时间） */
    public static String userConvs(String userId) {
        return root + ":user:" + tag(userId) + ":convs";
    }

    /** 用户在各会话的已读位置（Hash：conversationId -> 已读到的最大 seq） */
    public static String userRead(String userId) {
        return root + ":user:" + tag(userId) + ":read";
    }

    /** 用户在各会话的标记位（Hash：conversationId -> 位图；bit0 置顶、bit1 免打扰） */
    public static String userFlags(String userId) {
        return root + ":user:" + tag(userId) + ":flags";
    }

    /** 客户端水位（Hash：conversationId -> 客户端亲口确认收到的最大 seq），per user+device */
    public static String userWatermark(String userId, String deviceId) {
        return root + ":user:" + tag(userId) + ":wm:" + deviceId;
    }

    /** 用户注册过的设备集合（Set，网关写、IM 服务读，带 TTL） */
    public static String userDevices(String userId) {
        return root + ":user:" + tag(userId) + ":devices";
    }

    /**
     * 设备台账（Hash：deviceId -> {@code transport|gatewayInstance|loginAt|online})，per user。
     *
     * <p>与 {@link #userDevices} 的分工：那一张是「设备槽位」（Set，只回答有哪些设备、
     * 用于限制同账号登录台数），这里回答「每台设备最后一次是怎么登的、现在还在不在线」。
     * 两张表不能合并——槽位在断连后<b>故意保留</b>（30 天内重连仍是同一台设备），
     * 而台账里的 online 位必须随手断手写，两者的生命周期不同。
     *
     * <p>写入方只有 IM 服务（网关通过 ReportDevice 上报），读取方是设备列表接口。
     * 键上带 TTL 与槽位一致：用户长期不登录后台账自然消失，不必定期清理。
     */
    public static String userDeviceState(String userId) {
        return root + ":user:" + tag(userId) + ":device:state";
    }

    /** 用户加入的群反查（Set：userId -> groupId），大群降级后靠它补齐会话列表 */
    public static String userGroups(String userId) {
        return root + ":user:" + tag(userId) + ":groups";
    }

    /** 单聊离线帧队列（ZSet：score=seq，member=完整下行帧） */
    public static String userOfflineQueue(String userId) {
        return root + ":user:" + tag(userId) + ":offline";
    }

    /** 单聊离线队列的写入位置计数器（String），与队列同 slot */
    public static String userOfflinePosition(String userId) {
        return root + ":user:" + tag(userId) + ":offline:pos";
    }

    /** 单聊离线补发的设备级游标（String：该设备已确认补发到的最大 seq） */
    public static String userOfflineCursor(String userId, String deviceId) {
        return root + ":user:" + tag(userId) + ":offline:cur:" + deviceId;
    }

    /** 群聊补发的设备级游标（Hash：conversationId -> 该设备已确认补发到的 seq） */
    public static String userOfflineGroupCursor(String userId, String deviceId) {
        return root + ":user:" + tag(userId) + ":offline:gcur:" + deviceId;
    }

    /** 群聊补发锚点（Hash：conversationId -> 待补发的 seq，带 TTL） */
    public static String userOfflinePtr(String userId) {
        return root + ":user:" + tag(userId) + ":offline:ptr";
    }

    /** 用户本地删除的消息（Set：seq），按会话分开 */
    public static String userDeleted(String userId, String conversationId) {
        return root + ":user:" + tag(userId) + ":deleted:" + conversationId;
    }

    /** 好友集合（Set：userId -> 好友 userId，双向各写一份） */
    public static String userFriends(String userId) {
        return root + ":user:" + tag(userId) + ":friends";
    }

    /** 黑名单（Set：单向，只记「我拉黑了谁」） */
    public static String userBlocks(String userId) {
        return root + ":user:" + tag(userId) + ":blocks";
    }

    /**
     * 我收到的好友申请（Hash：field = 申请人 userId，value = 申请单 JSON）。
     *
     * <p>为什么用 Hash 而不是 Set：申请单不止「谁申请了」，还有附言、状态（PENDING /
     * ACCEPTED / REJECTED）和时间。Set 只装得下一个 userId，状态就得另找地方存，
     * 而「列表 + 单条状态」分两处存，迟早出现列表里有、状态查不到的行。
     */
    public static String userFriendRequestsIn(String userId) {
        return root + ":user:" + tag(userId) + ":friend-req:in";
    }

    /** 我发出的好友申请（Hash：field = 被申请人 userId，value 同上） */
    public static String userFriendRequestsOut(String userId) {
        return root + ":user:" + tag(userId) + ":friend-req:out";
    }

    /**
     * 已废弃的单聊离线 List 队列。
     *
     * <p>新的 ZSet 队列（{@link #userOfflineQueue}）取代它：旧结构的「读即消费」
     * （{@code LRANGE + LTRIM}）在网关写失败时就丢帧，属 at-most-once。
     * 这里仍保留读取路径，只为把升级窗口内积压的帧发掉。
     *
     * @deprecated 用 {@link #userOfflineQueue(String)}
     */
    @Deprecated
    public static String userLegacyOffline(String userId) {
        return root + ":user:" + tag(userId) + ":legacy-offline";
    }

    // ==== 群维度（tag = {groupId}） ====

    /** 群成员集合（Set），im.storage=redis 时的群成员存储 */
    public static String groupMembers(String groupId) {
        return root + ":group:" + tag(groupId) + ":members";
    }

    /** 群资料（Hash：name / owner_id / announcement / max_members / created_at / muted_all / dismissed） */
    public static String groupInfo(String groupId) {
        return root + ":group:" + tag(groupId) + ":info";
    }

    /**
     * 群内角色（Hash：{@code userId -> OWNER|ADMIN}）。
     *
     * <p>只存非 MEMBER 的角色：绝大多数人是普通成员，把他们也写进来会让这张表
     * 随群人数线性增长，而读路径只需要回答「这个人是不是管理员」——缺省即 MEMBER。
     */
    public static String groupRoles(String groupId) {
        return root + ":group:" + tag(groupId) + ":roles";
    }

    /**
     * 单人禁言到期时间（Hash：{@code userId -> untilEpochMillis}）。
     *
     * <p>存到期时间而不是布尔值：禁言天然带时长（10 分钟 / 1 天），
     * 存布尔就得再存一个「谁在什么时候解除」的定时任务——那是一个最容易没人维护的组件。
     * 存时间戳则读时比较即可，过期的字段还能顺手清掉。
     */
    public static String groupMutes(String groupId) {
        return root + ":group:" + tag(groupId) + ":mutes";
    }

    // ==== 路由（tag = {userId}） ====

    /** 在线路由：SET{网关实例 ID}，带 TTL，由网关心跳续期；多端时可能多个实例 */
    public static String gatewayUser(String userId) {
        return root + ":gateway:user:" + tag(userId);
    }

    /**
     * 最近一次下线时间（毫秒时间戳，String，带 TTL）：presence 查询里的 {@code lastSeen}。
     *
     * <p>只写「真的下线」这一刻，不写心跳：它回答的是「他上次在线是什么时候」，
     * 而不是「我们最后一次看见他是什么时候」。TTL 到期即视为「太久没上线，不必展示」——
     * 一个半年前的用户显示「5 分钟前在线」比不显示更误导。
     *
     * <p>用它而不是去扫路由/设备表：那两张表分别是「现在」和「有哪些端」，
     * 都不记录时间点，也都不该为了展示需求去改语义。
     */
    public static String presenceLastSeen(String userId) {
        return root + ":presence:seen:" + tag(userId);
    }

    /**
     * 在线路由 key 的 SCAN 匹配模式（用于清理「已下线实例」的残留绑定）。
     *
     * <p>Redis 的 glob 只把 {@code * ? [ ] \\} 当特殊字符，{@code {} 不需要转义，
     * 所以这里可以安全地带着 hash tag 做前缀匹配。
     */
    public static String gatewayUserPattern() {
        return root + ":gateway:user:{*";
    }

    /**
     * Cluster 的 hash tag：同一实体的所有 key 因此落在同一 slot。
     *
     * <p>只允许在 key 构造方法内部使用——外部拼 key 会让「唯一构造点」失效，
     * 那正是改造前 CROSSSLOT 的成因。
     */
    private static String tag(String id) {
        return "{" + id + "}";
    }
}
