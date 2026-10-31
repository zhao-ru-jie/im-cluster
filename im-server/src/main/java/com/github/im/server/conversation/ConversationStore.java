package com.github.im.server.conversation;

import com.github.im.contract.RedisKeys;
import com.github.im.contract.resilience.RedisDegrade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 会话模型：元信息、用户会话列表、已读位置。
 *
 * <h2>为什么需要这一层</h2>
 *
 * <p>在此之前「会话」只是一个字符串约定（{@code s:a:b} / {@code g:{id}}），没有任何实体。
 * 于是三件事做不了：<b>会话列表</b>（不知道用户有哪些会话、哪个最近活跃）、
 * <b>未读数</b>（不知道会话进行到哪）、<b>最后一条消息摘要</b>（列表里要显示）。
 *
 * <h2>未读数是算出来的，不是存出来的</h2>
 *
 * <p>{@code unread = lastSeq - readSeq}。存计数（每条消息给每个成员 INCR）在群聊里是
 * 消息数 × 成员数的写放大，而且一旦某次写入失败就永远和真实消息数不一致——
 * 而两个游标相减天然自愈：任何一端漏写，下一次读到的都是正确的差值。
 *
 * <h2>已读是用户级，同步水位是设备级</h2>
 *
 * <p>这两个概念经常被混为一谈，但它们的粒度必须不同：
 * <ul>
 *   <li><b>已读</b>（本类 {@code im:user:read:{userId}}）：人看到哪了，一个人只有一个答案。
 *       手机读完，网页的角标就该清零——多端合并在这是<b>免费</b>的，因为只有一份状态；</li>
 *   <li><b>同步水位</b>（{@code im:wm:{userId}:{deviceId}} / {@code im:offline:cur:*}）：
 *       这条数据推到哪个端了，传输语义，必须按设备分。</li>
 * </ul>
 *
 * <h2>新会话的已读基线</h2>
 *
 * <p>用户第一次出现在某个会话的列表里时，把已读位置初始化为<b>当时的</b> lastSeq，
 * 也就是「加入之前的消息不算未读」。否则一个十万条历史的群，新人一进来未读数就是 100000，
 * 而且客户端为了清零还得把整个历史标成已读。
 *
 * <p>这个初始化必须和「加入会话列表」原子完成，否则会出现「ZSet 进去了、基线没设」的窗口，
 * 那一瞬间的未读数就是全量历史。所以两者共用 {@code {userId}} 这个 hash tag，落在一个 Lua 里。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class ConversationStore {

    private static final Logger log = LoggerFactory.getLogger(ConversationStore.class);

    /**
     * 更新会话元信息：<b>只有 last_seq 前进时才覆盖</b>。
     *
     * <p>乱序到达（两条消息的 submit 并发，后分配的 seq 先落）会让较小 seq 的消息后写，
     * 无条件覆盖就把 {@code last_seq} 退回去了——会话列表的最后一条消息会闪回旧内容，
     * 未读数还会算成负数。
     *
     * <pre>
     * KEYS[1] = im:conv:meta:{convId}
     * ARGV[1] = seq, ARGV[2] = type, ARGV[3] = time, ARGV[4] = from,
     * ARGV[5] = digest, ARGV[6] = msgId, ARGV[7] = ttl 毫秒
     * </pre>
     *
     * @return 1 = 真的前进了；0 = 来晚了，丢弃
     */
    private static final byte[] ADVANCE_META = """
            local cur = tonumber(redis.call('HGET', KEYS[1], 'last_seq') or '0')
            if tonumber(ARGV[1]) > cur then
                redis.call('HSET', KEYS[1], 'type', ARGV[2])
                redis.call('HSET', KEYS[1], 'last_seq', ARGV[1])
                redis.call('HSET', KEYS[1], 'last_time', ARGV[3])
                redis.call('HSET', KEYS[1], 'last_from', ARGV[4])
                redis.call('HSET', KEYS[1], 'last_digest', ARGV[5])
                redis.call('HSET', KEYS[1], 'last_msg_id', ARGV[6])
                redis.call('PEXPIRE', KEYS[1], ARGV[7])
                return 1
            end
            return 0
            """.getBytes(StandardCharsets.UTF_8);

    /**
     * 把会话推进用户的列表，并在<b>首次出现</b>时给出已读基线。两件事必须原子。
     *
     * <pre>
     * KEYS[1] = im:user:convs:{userId}    (ZSet，score = 最后消息时间)
     * KEYS[2] = im:user:read:{userId}     (Hash，convId -> 已读 seq)
     * ARGV[1] = convId, ARGV[2] = lastTime, ARGV[3] = baselineSeq, ARGV[4] = ttl 毫秒
     * </pre>
     *
     * <ul>
     *   <li><b>只让排序往前</b>：不会被一条补发的旧消息把会话压下去。
     *       用 {@code ZSCORE} 先比较而不是 {@code ZADD GT}——后者要 Redis 6.2+，
     *       而本项目要兼容部署环境里常见的 3.x / 5.x；</li>
     *   <li>{@code HSETNX}：只初始化一次，之后由用户真正的已读上报来推进。</li>
     * </ul>
     */
    private static final byte[] TOUCH_USER_CONVERSATION = """
            local cur = redis.call('ZSCORE', KEYS[1], ARGV[1])
            if cur == false or tonumber(cur) < tonumber(ARGV[2]) then
                redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
            end
            redis.call('HSETNX', KEYS[2], ARGV[1], ARGV[3])
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            redis.call('PEXPIRE', KEYS[2], ARGV[4])
            return 1
            """.getBytes(StandardCharsets.UTF_8);

    /**
     * 已读位置：只前进，并且不超过会话当前的 lastSeq。
     *
     * <p>为什么要 cap：客户端可能上报一个未来的 seq（本地乐观计数、或者消息被撤回后
     * lastSeq 没有回退），不 cap 的话未读数会变成负数。
     *
     * <pre>KEYS[1] = im:user:read:{userId}；ARGV[1] = convId；ARGV[2] = seq；ARGV[3] = cap；ARGV[4] = ttl</pre>
     */
    private static final byte[] ADVANCE_READ = """
            local target = tonumber(ARGV[2])
            local cap = tonumber(ARGV[3])
            if cap >= 0 and target > cap then
                target = cap
            end
            local cur = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
            if target > cur then
                redis.call('HSET', KEYS[1], ARGV[1], target)
                redis.call('PEXPIRE', KEYS[1], ARGV[4])
                return target
            end
            redis.call('PEXPIRE', KEYS[1], ARGV[4])
            return cur
            """.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate redis;
    private final Duration listTtl;
    private final int fanoutLimit;

    /**
     * 单批查多少人的已读位置。
     *
     * <p>群上限 5000 人，一批 500 意味着最多 10 次流水线；单批再大就会让 Cluster 下某一节点
     * 一次收几千条命令，反而把延迟抖动放大到其它业务的头上。
     */
    private static final int READ_SEQ_BATCH = 500;

    public ConversationStore(StringRedisTemplate redis,
                             @Value("${im.conversation.list-ttl:30d}") Duration listTtl,
                             @Value("${im.conversation.fanout-limit:1000}") int fanoutLimit) {
        this.redis = redis;
        this.listTtl = listTtl;
        this.fanoutLimit = Math.max(fanoutLimit, 1);
        log.info("[conv] 会话列表 TTL {}，写扩散上限 {} 人（超过则只更新元信息，靠群反查表补齐）",
                listTtl, this.fanoutLimit);
    }

    /** 会话元信息。{@code lastSeq == 0} 表示这个会话还没有任何消息 */
    public record ConversationMeta(String conversationId, String type, long lastSeq, long lastTime,
                                   String lastFrom, String lastDigest, String lastMsgId) {
        public static ConversationMeta empty(String conversationId) {
            return new ConversationMeta(conversationId, "", 0L, 0L, "", "", "");
        }
    }

    // ==== 元信息 ====

    /**
     * 更新会话元信息（只前进）。
     *
     * @return true = 真的更新了；false = 这条消息的 seq 比已知的旧，丢弃
     */
    public boolean updateMeta(String conversationId, String type, long seq, long time,
                              String from, String digest, String msgId) {
        Long updated = RedisDegrade.call("conv.updateMeta",
                () -> redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                        ADVANCE_META, ReturnType.INTEGER, 1,
                        key(RedisKeys.convMeta(conversationId)),
                        String.valueOf(seq).getBytes(StandardCharsets.UTF_8),
                        bytes(type), String.valueOf(time).getBytes(StandardCharsets.UTF_8),
                        bytes(from), bytes(digest), bytes(msgId),
                        String.valueOf(listTtl.toMillis()).getBytes(StandardCharsets.UTF_8))),
                () -> null);
        // false 的语义是「这条消息的 seq 比已知的旧」——Redis 不可用时也返回 false，
        // 调用方（受理路径）本来就不靠它决定成败，会话列表滞后可由拉取接口补
        return updated != null && updated > 0;
    }

    public ConversationMeta meta(String conversationId) {
        Map<Object, Object> raw = redis.opsForHash().entries(RedisKeys.convMeta(conversationId));
        return toMeta(conversationId, raw);
    }

    /** 批量读元信息（会话列表每页几十个，逐个 HGETALL 就是几十次往返） */
    public Map<String, ConversationMeta> metas(Collection<String> conversationIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(conversationIds));
        if (ids.isEmpty()) {
            return Map.of();
        }
        List<Object> raw = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String conversationId : ids) {
                connection.hashCommands().hGetAll(key(RedisKeys.convMeta(conversationId)));
            }
            return null;
        });
        Map<String, ConversationMeta> result = new LinkedHashMap<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            result.put(ids.get(i), toMeta(ids.get(i), asMap(raw.get(i))));
        }
        return result;
    }

    // ==== 用户会话列表 ====

    /**
     * 把会话推进一个用户的列表（写扩散的单元）。
     *
     * @param baselineSeq 该用户首次看到这个会话时的 lastSeq（已读基线，历史不算未读）
     */
    public void touch(String userId, String conversationId, long lastTime, long baselineSeq) {
        // 会话列表是「可重建的投影」：写不进去只影响列表排序/未读，不影响消息本身
        RedisDegrade.run("conv.touch", () ->
                redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                        TOUCH_USER_CONVERSATION, ReturnType.INTEGER, 2,
                        key(convsKey(userId)), key(readKey(userId)),
                        bytes(conversationId),
                        String.valueOf(lastTime).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(baselineSeq).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(listTtl.toMillis()).getBytes(StandardCharsets.UTF_8))));
    }

    /**
     * 批量写扩散。
     *
     * <p>单聊 2 人、小群 N 人；超出 {@code fanoutLimit} 时<b>直接放弃</b>并返回 0，
     * 由调用方记 warn。放弃是安全的：大群成员的会话列表通过 {@link #recentConversations}
     * 的补齐逻辑（{@code im:group:user:{userId}} 反查）拿到，代价只是列表排序时间不准。
     *
     * @return 实际写扩散的人数
     */
    public int touchAll(Collection<String> userIds, String conversationId, long lastTime, long baselineSeq) {
        if (userIds == null || userIds.isEmpty()) {
            return 0;
        }
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        if (ids.size() > fanoutLimit) {
            log.warn("[conv] 会话 {} 成员 {} 超过写扩散上限 {}，跳过成员会话列表（靠群反查表补齐）",
                    conversationId, ids.size(), fanoutLimit);
            return 0;
        }
        byte[] convId = bytes(conversationId);
        byte[] time = String.valueOf(lastTime).getBytes(StandardCharsets.UTF_8);
        byte[] baseline = String.valueOf(baselineSeq).getBytes(StandardCharsets.UTF_8);
        byte[] ttl = String.valueOf(listTtl.toMillis()).getBytes(StandardCharsets.UTF_8);
        try {
            redis.executePipelined((RedisCallback<Object>) connection -> {
                for (String userId : ids) {
                    connection.scriptingCommands().eval(TOUCH_USER_CONVERSATION, ReturnType.INTEGER, 2,
                            key(convsKey(userId)), key(readKey(userId)), convId, time, baseline, ttl);
                }
                return null;
            });
            return ids.size();
        } catch (RuntimeException e) {
            // Cluster 下每个用户一个 hash tag = 不同 slot，pipeline 里仍可能整体失败
            log.warn("[conv] 批量写会话列表失败（{} 人），退化为逐条: {}", ids.size(), e.getMessage());
            int done = 0;
            for (String userId : ids) {
                try {
                    touch(userId, conversationId, lastTime, baselineSeq);
                    done++;
                } catch (RuntimeException single) {
                    log.error("[conv] 写会话列表失败 user={} conv={} err={}",
                            userId, conversationId, single.getMessage());
                }
            }
            return done;
        }
    }

    /**
     * 最近活跃的会话（按最后消息时间倒序）。
     *
     * <p>大群写扩散被降级后，成员的 ZSet 里可能没有这个群。这里用
     * {@code im:group:user:{userId}} 反查补齐：缺席的会话按元信息里的 lastTime 补进列表。
     * 补齐是<b>读时</b>做的，所以不需要给大群做消息级的扇出。
     */
    public List<String> recentConversations(String userId, int limit) {
        return recentConversations(userId, limit, Set.of());
    }

    public List<String> recentConversations(String userId, int limit, Set<String> groupIds) {
        int size = Math.max(limit, 1);
        Set<String> listed = redis.opsForZSet().reverseRange(convsKey(userId), 0, size - 1);
        List<String> result = new ArrayList<>(listed == null ? 0 : listed.size());
        if (listed != null) {
            result.addAll(listed);
        }
        if (groupIds != null && !groupIds.isEmpty()) {
            for (String groupId : groupIds) {
                String conversationId = "g:" + groupId;
                if (!result.contains(conversationId)) {
                    result.add(conversationId);
                }
            }
        }
        return result.size() > size ? result.subList(0, size) : result;
    }

    // ==== 会话标记（置顶 / 免打扰） ====

    /** 标记位：第 0 位置顶，第 1 位免打扰 */
    public static final int FLAG_PINNED = 1;
    public static final int FLAG_MUTED = 1 << 1;

    /**
     * 一个会话在某个用户列表里的偏好。
     *
     * <p>为什么不是「已读」那种游标：置顶/免打扰是<b>展示偏好</b>，不回影响任何投递语义，
     * 也不参与未读数计算（免打扰只是客户端不响铃/不打角标，未读数照样要算）。
     */
    public record ConversationFlag(boolean pinned, boolean muted) {
        public static final ConversationFlag NONE = new ConversationFlag(false, false);
    }

    /** 批量读标记位（会话列表一页内共用一次 pipeline） */
    public Map<String, ConversationFlag> flags(String userId, Collection<String> conversationIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(conversationIds));
        if (ids.isEmpty()) {
            return Map.of();
        }
        List<Object> raw = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String conversationId : ids) {
                connection.hashCommands().hGet(key(flagsKey(userId)), bytes(conversationId));
            }
            return null;
        });
        Map<String, ConversationFlag> result = new LinkedHashMap<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            result.put(ids.get(i), toFlag((int) parseLong(raw.get(i), 0L)));
        }
        return result;
    }

    public ConversationFlag flag(String userId, String conversationId) {
        Object raw = redis.opsForHash().get(flagsKey(userId), conversationId);
        return toFlag((int) parseLong(raw, 0L));
    }

    /**
     * 设置标记位。<b>只改传了的那一项</b>，另一项保持原值——
     * 设置页里「置顶」和「免打扰」是两个独立开关，不能因为一次只改一个就把另一个清掉。
     *
     * @param pinned {@code null} 表示不改
     * @param muted  {@code null} 表示不改
     * @return 设置后的标记
     */
    public ConversationFlag setFlags(String userId, String conversationId, Boolean pinned, Boolean muted) {
        if (!StringUtils.hasText(userId) || !StringUtils.hasText(conversationId)) {
            return ConversationFlag.NONE;
        }
        if (pinned == null && muted == null) {
            return flag(userId, conversationId);
        }
        int next = readFlags(userId, conversationId);
        if (pinned != null) {
            next = pinned ? (next | FLAG_PINNED) : (next & ~FLAG_PINNED);
        }
        if (muted != null) {
            next = muted ? (next | FLAG_MUTED) : (next & ~FLAG_MUTED);
        }
        if (next == 0) {
            // 全关就删字段：否则一张长期使用的表会留下大量值为 0 的行，
            // 而 0 与「没有设置过」语义完全一样
            redis.opsForHash().delete(flagsKey(userId), conversationId);
        } else {
            redis.opsForHash().put(flagsKey(userId), conversationId, String.valueOf(next));
            redis.expire(flagsKey(userId), listTtl);
        }
        return toFlag(next);
    }

    private int readFlags(String userId, String conversationId) {
        return (int) parseLong(redis.opsForHash().get(flagsKey(userId), conversationId), 0L);
    }

    private static ConversationFlag toFlag(int bits) {
        return new ConversationFlag((bits & FLAG_PINNED) != 0, (bits & FLAG_MUTED) != 0);
    }

    // ==== 已读位置 ====

    /**
     * 该用户在某个会话上已读到哪（0 = 从未读过，但通常已被基线初始化成首见时的 lastSeq）。
     *
     * <p>Redis 不可用时读成 0：未读数会偏大（可能显示「有一堆未读」），但不会把没读的判成已读。
     * 反方向的兜底才是危险的——它会让用户永远看不到那几条消息。
     */
    public long readSeq(String userId, String conversationId) {
        Object raw = RedisDegrade.call("conv.readSeq",
                () -> redis.opsForHash().get(readKey(userId), conversationId),
                () -> null);
        return parseLong(raw, 0L);
    }

    /** 批量读已读位置（会话列表一页内共用一次 pipeline） */
    public Map<String, Long> readSeqs(String userId, Collection<String> conversationIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(conversationIds));
        if (ids.isEmpty()) {
            return Map.of();
        }
        List<Object> raw = RedisDegrade.call("conv.readSeqs",
                () -> redis.executePipelined((RedisCallback<Object>) connection -> {
                    for (String conversationId : ids) {
                        connection.hashCommands().hGet(key(readKey(userId)), bytes(conversationId));
                    }
                    return null;
                }),
                List::of);
        Map<String, Long> result = new LinkedHashMap<>(ids.size());
        for (int i = 0; i < ids.size(); i++) {
            result.put(ids.get(i), parseLong(i < raw.size() ? raw.get(i) : null, 0L));
        }
        return result;
    }

    /**
     * 批量读「同一个会话、多个用户」的已读位置（群已读聚合用）。
     *
     * <p>这是 {@link #readSeqs(String, Collection)} 的<b>转置</b>：那个是「一个人看多个会话」，
     * 这个是「一个会话看多个人」。两者没法共用一次 pipeline——前者的 key 是同一个 Hash 的多个
     * field，后者的 key 是每个人的 Hash（{@code im:user:{u}:read}）。逐人 {@code HGET} 在万人群里
     * 就是一万次往返，所以这里按批 pipeline。
     *
     * <p>不同用户的 key 落在不同 slot（hash tag 是 {@code {userId}}），Cluster 下由客户端按节点分组；
     * 因此这里显式<b>分批</b>（{@link #READ_SEQ_BATCH}），避免一次流水线把几千条命令压到单个节点。
     *
     * @return userId → 该会话的已读 seq；从未上报过的人是 {@code 0}（与 {@link #readSeq} 一致）
     */
    public Map<String, Long> readSeqsOf(String conversationId, Collection<String> userIds) {
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        if (ids.isEmpty()) {
            return Map.of();
        }
        Map<String, Long> result = new LinkedHashMap<>(ids.size());
        for (int from = 0; from < ids.size(); from += READ_SEQ_BATCH) {
            List<String> batch = ids.subList(from, Math.min(from + READ_SEQ_BATCH, ids.size()));
            List<Object> raw = RedisDegrade.call("conv.readSeqsOf",
                    () -> redis.executePipelined((RedisCallback<Object>) connection -> {
                        for (String userId : batch) {
                            connection.hashCommands().hGet(key(readKey(userId)), bytes(conversationId));
                        }
                        return null;
                    }),
                    List::of);
            for (int i = 0; i < batch.size(); i++) {
                result.put(batch.get(i), parseLong(i < raw.size() ? raw.get(i) : null, 0L));
            }
        }
        return result;
    }

    /**
     * 上报已读（只前进，且不超过会话最后的 seq）。
     *
     * @param capSeq 会话当前 lastSeq，用来兜住客户端上报的「未来 seq」；传负数表示不限制
     * @return 推进后的已读位置
     */
    public long markRead(String userId, String conversationId, long seq, long capSeq) {
        Long current = RedisDegrade.call("conv.markRead",
                () -> redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                        ADVANCE_READ, ReturnType.INTEGER, 1,
                        key(readKey(userId)),
                        bytes(conversationId),
                        String.valueOf(Math.max(seq, 0)).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(capSeq).getBytes(StandardCharsets.UTF_8),
                        String.valueOf(listTtl.toMillis()).getBytes(StandardCharsets.UTF_8))),
                () -> null);
        // 0 = 「这次没有推进」。Redis 不可用时宁可让已读位置停在原处（用户下次上报会再推一次），
        // 也不能让上报把位置写坏
        return current == null ? 0L : current;
    }

    // ==== 内部 ====

    /**
     * 会话列表与已读表共用 {@code {userId}} hash tag，保证 Cluster 下同 slot——
     * {@link #TOUCH_USER_CONVERSATION} 会同时写这两个 key。
     */
    private static String convsKey(String userId) {
        return RedisKeys.userConvs(userId);
    }

    private static String readKey(String userId) {
        return RedisKeys.userRead(userId);
    }

    private static String flagsKey(String userId) {
        return RedisKeys.userFlags(userId);
    }

    /** key 的单一来源：测试清理也用它构造，避免 key 格式改了以后测试静默失效 */
    public static String convsKeyOf(String userId) {
        return convsKey(userId);
    }

    public static String readKeyOf(String userId) {
        return readKey(userId);
    }

    public static String flagsKeyOf(String userId) {
        return flagsKey(userId);
    }

    private static ConversationMeta toMeta(String conversationId, Map<Object, Object> raw) {
        if (raw == null || raw.isEmpty()) {
            return ConversationMeta.empty(conversationId);
        }
        return new ConversationMeta(
                conversationId,
                str(raw.get("type")),
                parseLong(raw.get("last_seq"), 0L),
                parseLong(raw.get("last_time"), 0L),
                str(raw.get("last_from")),
                str(raw.get("last_digest")),
                str(raw.get("last_msg_id")));
    }

    @SuppressWarnings("unchecked")
    private static Map<Object, Object> asMap(Object raw) {
        return raw instanceof Map<?, ?> map ? (Map<Object, Object>) map : Map.of();
    }

    /**
     * 值可能来自两条不同的读路径：{@code opsForHash()} 用 StringRedisSerializer 还原成 String，
     * 而 pipeline 里的 low-level {@code hGetAll} 拿到的是 byte[]。两种都要认。
     */
    private static String str(Object raw) {
        if (raw == null) {
            return "";
        }
        if (raw instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(raw);
    }

    private static long parseLong(Object raw, long fallback) {
        if (raw == null) {
            return fallback;
        }
        String text = raw instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : String.valueOf(raw);
        try {
            return Long.parseLong(text.trim());
        } catch (NumberFormatException e) {
            return fallback;
        }
    }

    private static byte[] bytes(String value) {
        return (value == null ? "" : value).getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] key(String redisKey) {
        return redisKey.getBytes(StandardCharsets.UTF_8);
    }
}
