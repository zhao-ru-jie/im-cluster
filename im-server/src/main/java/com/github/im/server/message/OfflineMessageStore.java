package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.connection.zset.Tuple;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

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
 * 离线存储（归属 IM 服务，不再放在连接层）。
 *
 * <h2>两条通道，对应两种「放大倍数」</h2>
 *
 * <p><b>单聊 —— 帧队列</b>{@code im:offline:q:{userId}}（ZSet）：一条消息只有一个收件人，
 * 存一份完整帧就是最省的形态。
 *
 * <p><b>群聊 —— 补发指针</b>{@code im:offline:ptr:{userId}}（Hash，{@code conversationId -> 待补发 seq}）：
 * 群消息的收件人是全体成员，若也按人存帧，一条消息的存储量 = 不在线人数。
 * 500 人群里 450 人不在线、50 人各发 5 条，就是 250 × 450 = 112,500 份相同帧（~30MB），
 * 而且会随消息数线性增长。改成指针后，每个「用户 × 会话」只占一个 field（几十字节），
 * 后续消息只把同一个 field 往前推——<b>占用不随消息数增长</b>，帧在补发时由
 * {@link ClientFrames#frameOf} 从 {@code im:conv:msgs:{conv}} / {@code im_message} 重建。
 *
 * <h2>补发是「至少一次」，不是「读即消费」</h2>
 *
 * <p>这是本类最重要的一条约定。旧实现用 {@code LRANGE + LTRIM} 在一个 Lua 里把帧原子移走，
 * 网关拿到之后才逐帧写 socket——只要有一帧写失败就 {@code break}，<b>已经移走但没发出的帧
 * 永远消失</b>，而单聊没有「从历史重建」的兜底路径，属于 at-most-once。
 *
 * <p>现在的顺序是：<b>读 → 写进连接 → 确认 → 下次从确认处继续</b>：
 * <ol>
 *   <li>{@link #peek} 只读不删，返回帧和「本批覆盖到的位置」；</li>
 *   <li>网关把这些帧全部写进客户端连接；</li>
 *   <li>全部成功才调 {@code MessageIngress.ackPending} → {@link #ackFrames} 推进游标；</li>
 *   <li>中途失败 / 进程崩溃 = 游标不动，下次重连从原位置重读（客户端按 seq 去重）。</li>
 * </ol>
 *
 * <h2>为什么是 ZSet 而不是 List</h2>
 *
 * <p>补发起点是「这台设备已经确认到的<b>位置</b>」，需要能按位置定位读：
 * {@code ZRANGEBYSCORE key (cursor +inf} 是 O(log N + M)。List 只能按下标读，而
 * {@code LTRIM} 一裁下标就整体漂移，游标立刻失效。
 *
 * <p>score <b>不是</b>消息的 seq——seq 是<b>会话内</b>单调的，同一个用户在不同会话里会出现
 * 相同的 seq（{@code s:a:b} 的 5 和 {@code s:a:c} 的 5），拿它当 score 会让「游标推进到 5」
 * 直接吃掉另一个会话的 5。所以 score 用 per-user 的入队序号（{@link #PUSH_FRAME} 里的
 * {@code INCR}），跨会话单调且唯一。
 *
 * <h2>多设备</h2>
 *
 * <p>游标是 per-device 的（{@code im:offline:cur:{userId}:{deviceId}}）：手机和网页各自补到哪
 * 互不相干。删数据只按「总条数超上限」裁，不按游标裁——按游标裁会让先确认的设备把另一个
 * 设备还没读的消息删掉。
 *
 * <h2>为什么两种通道都带 TTL 与上限</h2>
 *
 * <p>离线队列的写方是「消息」，读方却是「用户上线」——两者完全解耦，一个长期不上线的用户
 * 会让队列无界增长。所以两条通道都必须有兜底：帧队列有长度上限（{@code im.offline.max-frames}）
 * 与过期时间（{@code im.offline.frame-ttl}），指针有过期时间（{@code im.offline.pointer-ttl}）。
 * 超期未上线 = 放弃补发，历史仍在消息存储里，客户端可以主动拉历史。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class OfflineMessageStore {

    private static final Logger log = LoggerFactory.getLogger(OfflineMessageStore.class);

    /**
     * 每个 pipeline 装多少条命令。
     *
     * <p>ZADD / HSETNX 都很轻（不返回数据），单片控制在毫秒级即可，避免一条大群消息把 Redis 顶住。
     */
    private static final int PIPELINE_BATCH = 500;

    /** 没带 deviceId 的客户端共用这个槽位，与网关的 ConnectionManager 保持一致 */
    private static final String DEFAULT_DEVICE = "default";

    /**
     * 单聊落帧：取位置 → 写入 → 按条数裁剪 → 续期，一把原子完成。
     *
     * <pre>
     * KEYS[1] = im:offline:q:{userId}         (ZSet，score = 入队序号)
     * KEYS[2] = im:offline:q:{userId}:pos     (String 计数器)
     * ARGV[1] = 帧
     * ARGV[2] = 上限条数
     * ARGV[3] = TTL 毫秒
     * </pre>
     *
     * <p>计数器丢了（过期 / Redis 重启）时<b>从当前 ZSet 的最后一个 score 续上</b>，
     * 而不是从 1 重来——否则新帧的 score 会小于队列里已有帧，补发顺序直接错乱。
     *
     * <p>两个 key 共用 {@code {userId}} 这个 hash tag，保证 Cluster 下同 slot（Lua 跨 key）。
     */
    private static final byte[] PUSH_FRAME = """
            local base
            local counter = redis.call('GET', KEYS[2])
            if counter == false then
                local top = redis.call('ZRANGE', KEYS[1], -1, -1, 'WITHSCORES')
                if top[2] ~= nil then
                    base = tonumber(top[2])
                else
                    base = 0
                end
            else
                base = tonumber(counter)
            end
            local pos = base + 1
            redis.call('SET', KEYS[2], pos, 'PX', ARGV[3])
            redis.call('ZADD', KEYS[1], pos, ARGV[1])
            redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -tonumber(ARGV[2]) - 1)
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return pos
            """.getBytes(StandardCharsets.UTF_8);

    /**
     * 旧版 List 队列的一次性读取（升级兼容）。
     *
     * <p>旧结构无法参与「按位置确认」（它本来就没有位置语义），但里面可能压着升级前的离线消息，
     * 所以仍然整体读出来发一次，读完即删。属于过渡逻辑，可以随下一个大版本删除。
     */
    private static final byte[] DRAIN_LEGACY = """
            local frames = redis.call('LRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
            if #frames > 0 then
                redis.call('LTRIM', KEYS[1], #frames, -1)
            end
            return frames
            """.getBytes(StandardCharsets.UTF_8);

    /**
     * 推进某台设备的补发游标：<b>只前进</b>，并且顺带续期。
     *
     * <p>为什么不能回退：两台设备并发补发、或客户端快速重连时，旧连接携带的较小 ack 会晚到；
     * 允许回退就是「永远在原地重复补发」的死循环。
     *
     * <pre>KEYS[1] = im:offline:cur:{userId}:{deviceId}；ARGV[1] = 新位置；ARGV[2] = TTL 毫秒</pre>
     *
     * @return 推进后的游标
     */
    private static final byte[] ADVANCE_CURSOR = """
            local cur = tonumber(redis.call('GET', KEYS[1]) or '0')
            local next = tonumber(ARGV[1])
            if next > cur then
                redis.call('SET', KEYS[1], ARGV[1], 'PX', ARGV[2])
                return next
            end
            redis.call('PEXPIRE', KEYS[1], ARGV[2])
            return cur
            """.getBytes(StandardCharsets.UTF_8);

    /**
     * 记录「这个人在这个会话上还有未补发的消息」。
     *
     * <p>用 {@code HSETNX} 而不是 {@code HSET}：字段一旦存在就保留<b>更小的</b>那个 seq，
     * 也就是「最早的未补发位置」。若每次都覆盖成最新 seq，中间那段就永远补不回来了。
     */
    private static final byte[] RECORD_PENDING = """
            redis.call('HSETNX', KEYS[1], ARGV[1], ARGV[2])
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return 1
            """.getBytes(StandardCharsets.UTF_8);

    /**
     * 推进某个<b>设备</b>在某个会话上的补发游标（只前进）。
     *
     * <p>为什么必须是设备级：用户级单一指针时，手机上线补完一批就把它推过去了，
     * 网页再上线补发起点已经在前面——那段消息对网页就是永久缺失。
     *
     * <p>字段不存在时<b>不创建</b>（返回 0）：游标由「确认」创建，而不是由「读取」创建，
     * 否则一次失败的补发会留下一个假的进度。
     *
     * <pre>
     * KEYS[1] = im:offline:gcur:{userId}:{deviceId}；ARGV[1] = conversationId；ARGV[2] = 新位置；ARGV[3] = TTL 毫秒
     * </pre>
     *
     * @return 1 = 真的前进了；0 = 字段不存在或已经更靠前
     */
    private static final byte[] ADVANCE_GROUP_CURSOR = """
            local cur = redis.call('HGET', KEYS[1], ARGV[1])
            if cur == false then
                redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
                redis.call('PEXPIRE', KEYS[1], ARGV[3])
                return 1
            end
            if tonumber(ARGV[2]) > tonumber(cur) then
                redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
                redis.call('PEXPIRE', KEYS[1], ARGV[3])
                return 1
            end
            return 0
            """.getBytes(StandardCharsets.UTF_8);

    /** 确实没有待补数据时清掉该会话的指针（读路径上执行，安全） */
    private static final byte[] CLEAR_PENDING = """
            redis.call('HDEL', KEYS[1], ARGV[1])
            return 1
            """.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate redis;
    private final Duration frameTtl;
    private final int maxFramesPerUser;
    private final Duration pointerTtl;

    public OfflineMessageStore(StringRedisTemplate redis,
                               @Value("${im.offline.frame-ttl:7d}") Duration frameTtl,
                               @Value("${im.offline.max-frames:500}") int maxFramesPerUser,
                               @Value("${im.offline.pointer-ttl:7d}") Duration pointerTtl) {
        this.redis = redis;
        this.frameTtl = frameTtl;
        this.maxFramesPerUser = Math.max(maxFramesPerUser, 1);
        this.pointerTtl = pointerTtl;
        log.info("[offline] 单聊离线队列上限 {} 条/人、TTL {}（读不删 + 按设备游标确认）；群聊补发指针 TTL {}",
                this.maxFramesPerUser, frameTtl, pointerTtl);
    }

    /**
     * 一次补发读到的内容。
     *
     * @param frames       待下发的帧（按入队位置升序）
     * @param ackSeq       本批覆盖到的位置（0 = 没有需要确认的东西）
     * @param legacyFrames 旧 List 队列里迁出来的帧；没有位置语义，不参与确认
     */
    public record FrameBatch(List<String> frames, long ackSeq, List<String> legacyFrames) {

        public static final FrameBatch EMPTY = new FrameBatch(List.of(), 0L, List.of());

        public boolean isEmpty() {
            return frames.isEmpty() && legacyFrames.isEmpty();
        }
    }

    // ==== 单聊：帧队列 ====

    public void store(String userId, String frame) {
        redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                PUSH_FRAME, ReturnType.INTEGER, 2,
                queueKey(userId),
                positionKey(userId),
                frame.getBytes(StandardCharsets.UTF_8),
                String.valueOf(maxFramesPerUser).getBytes(StandardCharsets.UTF_8),
                String.valueOf(frameTtl.toMillis()).getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * 批量落帧：同一个帧写给多个用户。
     *
     * <p>群聊已经改走 {@link #recordGroupPending}，这里只剩「单聊 + 多设备」的场景（通常一个人），
     * 保留批量形态是为了升级期间与测试里的大批量调用仍然一次 pipeline 完成。
     *
     * @return 真正写入成功的用户数；批量失败会退化为逐条重试，仍失败的只记 error
     */
    public int storeAll(Collection<String> userIds, String frame) {
        if (userIds == null || userIds.isEmpty()) {
            return 0;
        }
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        byte[] payload = frame.getBytes(StandardCharsets.UTF_8);
        byte[] maxFrames = String.valueOf(maxFramesPerUser).getBytes(StandardCharsets.UTF_8);
        byte[] ttlMillis = String.valueOf(frameTtl.toMillis()).getBytes(StandardCharsets.UTF_8);
        int stored = 0;
        for (int from = 0; from < ids.size(); from += PIPELINE_BATCH) {
            List<String> batch = ids.subList(from, Math.min(from + PIPELINE_BATCH, ids.size()));
            try {
                redis.executePipelined((RedisCallback<Object>) connection -> {
                    for (String userId : batch) {
                        connection.scriptingCommands().eval(PUSH_FRAME, ReturnType.INTEGER, 2,
                                queueKey(userId), positionKey(userId), payload, maxFrames, ttlMillis);
                    }
                    return null;
                });
                stored += batch.size();
            } catch (RuntimeException e) {
                // Cluster 跨 slot（每个用户一个 hash tag，必然不同 slot）或 Redis 瞬时故障：
                // 退化为逐条写，慢但保证这批人不会静默丢消息
                log.warn("[offline] 批量落离线失败（{} 人），退化为逐条写入: {}", batch.size(), e.getMessage());
                for (String userId : batch) {
                    try {
                        store(userId, frame);
                        stored++;
                    } catch (RuntimeException single) {
                        log.error("[offline] 落离线失败 user={} err={}", userId, single.getMessage());
                    }
                }
            }
        }
        return stored;
    }

    /**
     * 读一批待补发的帧，<b>不删除任何数据</b>。
     *
     * <p>起点是该设备上一次确认到的位置（{@code (cursor +inf}），所以：
     * <ul>
     *   <li>上次写成功并确认过 → 从下一个位置继续，不重复；</li>
     *   <li>上次写失败 / 进程崩溃（没确认）→ 从同一位置重读，重复但不丢；</li>
     *   <li>游标已过期 → 从 0 开始整队重读，仍然不丢（只是重复，客户端按 seq 去重）。</li>
     * </ul>
     *
     * <p>顺带把旧 List 队列的残留读出来（一次性迁移），它没有位置语义所以不参与确认。
     */
    public FrameBatch peek(String userId, String deviceId, int max) {
        if (max <= 0) {
            return FrameBatch.EMPTY;
        }
        List<String> legacy = legacyDrain(userId, max);
        int budget = max - legacy.size();
        if (budget <= 0) {
            return new FrameBatch(List.of(), 0L, legacy);
        }
        long cursor = cursorOf(userId, deviceId);
        // score 是 per-user 入队序号（从 1 起），超过 2^53 才会丢精度，实际不可达
        Range<Double> range = Range.of(Range.Bound.exclusive((double) cursor), Range.Bound.unbounded());
        Set<Tuple> tuples = readFrames(queueKey(userId), range, Limit.limit().count(budget));
        if (tuples == null || tuples.isEmpty()) {
            return new FrameBatch(List.of(), 0L, legacy);
        }
        List<String> frames = new ArrayList<>(tuples.size());
        long ackSeq = 0L;
        for (Tuple tuple : tuples) {
            byte[] member = tuple.getValue();
            if (member == null) {
                continue;
            }
            frames.add(new String(member, StandardCharsets.UTF_8));
            Double score = tuple.getScore();
            if (score != null && (long) (double) score > ackSeq) {
                ackSeq = (long) (double) score;
            }
        }
        return new FrameBatch(frames, ackSeq, legacy);
    }

    /**
     * 确认本批帧已经写进客户端连接：把该设备的游标推到 {@code ackSeq}。
     *
     * @return 推进后的游标（调用方只用于日志对账）
     */
    public long ackFrames(String userId, String deviceId, long ackSeq) {
        if (ackSeq <= 0) {
            return cursorOf(userId, deviceId);
        }
        Long cursor = redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                ADVANCE_CURSOR, ReturnType.INTEGER, 1,
                key(cursorKey(userId, deviceId)),
                String.valueOf(ackSeq).getBytes(StandardCharsets.UTF_8),
                String.valueOf(frameTtl.toMillis()).getBytes(StandardCharsets.UTF_8)));
        return cursor == null ? 0L : cursor;
    }

    /** 某台设备已确认到的位置（0 = 从未确认过） */
    public long cursorOf(String userId, String deviceId) {
        String raw = redis.opsForValue().get(cursorKey(userId, deviceId));
        if (raw == null || raw.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(raw.trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 还有多少条<b>该设备尚未确认</b>的帧。
     *
     * <p>不能用 {@code zCard}（队列总条数）当 {@code remaining}：已经确认过的帧要等到被裁剪
     * 才消失，用总数会让客户端永远看到「还剩一堆」，从而无限重拉。这里是按游标数区间。
     */
    @SuppressWarnings("deprecation")
    public long pendingFrames(String userId, String deviceId) {
        long cursor = cursorOf(userId, deviceId);
        Range<Double> range = Range.of(Range.Bound.exclusive((double) cursor), Range.Bound.unbounded());
        Long queued = redis.execute((RedisCallback<Long>) connection ->
                connection.zSetCommands().zCount(queueKey(userId), range));
        Long legacy = redis.opsForList().size(RedisKeys.userLegacyOffline(userId));
        return (queued == null ? 0L : queued) + (legacy == null ? 0L : legacy);
    }

    /**
     * 旧 List 队列的一次性读取，读完即删。
     *
     * <p>只用于升级兼容：旧结构是「读即消费」，没有位置概念，无法纳入新的确认协议。
     * 读出来发一次就够——写失败也只是重复补发不到（本来它就没有这个保证），
     * 不会比升级前更差。
     */
    @SuppressWarnings("deprecation")
    private List<String> legacyDrain(String userId, int max) {
        String key = RedisKeys.userLegacyOffline(userId);
        try {
            List<byte[]> raw = redis.execute((RedisCallback<List<byte[]>>) connection ->
                    connection.scriptingCommands().eval(DRAIN_LEGACY, ReturnType.MULTI, 1,
                            key(key), String.valueOf(max).getBytes(StandardCharsets.UTF_8)));
            if (raw == null || raw.isEmpty()) {
                return List.of();
            }
            List<String> frames = new ArrayList<>(raw.size());
            for (byte[] frame : raw) {
                frames.add(new String(frame, StandardCharsets.UTF_8));
            }
            log.info("[offline] 从旧 List 队列迁出 {} 条离线帧 user={}", frames.size(), userId);
            return frames;
        } catch (RuntimeException e) {
            // 新结构是 ZSet、旧结构是 List：同前缀不同 key 不会撞类型，这里只是兜 Redis 故障
            log.warn("[offline] 读取旧离线队列失败（忽略）user={} err={}", userId, e.getMessage());
            return List.of();
        }
    }

    // ==== 群聊：补发指针 ====

    /**
     * 记录「这批人在这个会话上还有未补发的消息」，值是最早的未补发位置（<b>锚点</b>）。
     *
     * <p>每条群消息对每个不在线成员只写一个 field（几十字节），而不是整帧（几百字节），
     * 且同一会话的后续消息只把 field 往前推，<b>总占用不随消息数增长</b>。
     *
     * <p><b>锚点不表示任何一台设备的具体进度</b>：它只负责回答「这台从没补过的设备该从哪开始」。
     * 每台设备自己补到哪记在 {@code im:offline:gcur:{userId}:{deviceId}} 里
     * （见 {@link #ackGroupPending}），补发起点取两者的较大值。
     * 这样手机补完不会把网页的起点推走，也是这一类「用户级状态 + 多设备」问题的通用解法：
     * <b>共享的只留下界，进度按端分开</b>。
     *
     * @return 写入成功的用户数（失败的用户名只记 error，下次上线拉历史仍能补齐）
     */
    public int recordGroupPending(Collection<String> userIds, String conversationId, long seq) {
        if (userIds == null || userIds.isEmpty() || conversationId == null || conversationId.isBlank()) {
            return 0;
        }
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        byte[] conversation = conversationId.getBytes(StandardCharsets.UTF_8);
        byte[] seqValue = String.valueOf(seq).getBytes(StandardCharsets.UTF_8);
        byte[] ttlMillis = String.valueOf(pointerTtl.toMillis()).getBytes(StandardCharsets.UTF_8);
        int stored = 0;
        for (int from = 0; from < ids.size(); from += PIPELINE_BATCH) {
            List<String> batch = ids.subList(from, Math.min(from + PIPELINE_BATCH, ids.size()));
            try {
                redis.executePipelined((RedisCallback<Object>) connection -> {
                    for (String userId : batch) {
                        connection.scriptingCommands().eval(RECORD_PENDING, ReturnType.INTEGER, 1,
                                key(RedisKeys.userOfflinePtr(userId)), conversation, seqValue, ttlMillis);
                    }
                    return null;
                });
                stored += batch.size();
            } catch (RuntimeException e) {
                log.warn("[offline] 批量记录群补发指针失败（{} 人），退化为逐条写入: {}", batch.size(), e.getMessage());
                for (String userId : batch) {
                    try {
                        redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                                RECORD_PENDING, ReturnType.INTEGER, 1,
                                key(RedisKeys.userOfflinePtr(userId)), conversation, seqValue, ttlMillis));
                        stored++;
                    } catch (RuntimeException single) {
                        log.error("[offline] 记录群补发指针失败 user={} conv={} err={}",
                                userId, conversationId, single.getMessage());
                    }
                }
            }
        }
        return stored;
    }

    /**
     * 当前待补发的会话指针（按会话 ID 字典序取前 {@code limit} 个）。
     *
     * <p>排序是为了多次 pull 之间能稳定推进：每次只处理前 N 个，处理完的会被确认推进或清理，
     * 保证不会永远卡在同一批会话上。
     *
     * @return conversationId → 待补发 seq（最早的未补发位置）
     */
    public Map<String, Long> pendingConversations(String userId, int limit) {
        if (limit <= 0) {
            return Map.of();
        }
        Map<Object, Object> entries = redis.opsForHash().entries(RedisKeys.userOfflinePtr(userId));
        if (entries.isEmpty()) {
            return Map.of();
        }
        List<String> conversations = new ArrayList<>(entries.size());
        for (Object field : entries.keySet()) {
            conversations.add(String.valueOf(field));
        }
        conversations.sort(String::compareTo);
        Map<String, Long> result = new LinkedHashMap<>();
        for (String conversationId : conversations) {
            if (result.size() >= limit) {
                break;
            }
            Long pendingFrom = parseSeq(entries.get(conversationId));
            if (pendingFrom != null) {
                result.put(conversationId, pendingFrom);
            }
        }
        return result;
    }

    /** 仍滞留待补发的会话数（用于 {@code remaining} 提示，不做精确到条数的估算） */
    public long pendingConversationCount(String userId) {
        Long size = redis.opsForHash().size(RedisKeys.userOfflinePtr(userId));
        return size == null ? 0L : size;
    }

    /**
     * 推进某台设备在某个会话上的补发游标（由 {@code AckPending} 调用，即「帧已经写进连接」之后）。
     *
     * @param deviceId 哪台设备确认的；不同设备各有一条独立进度
     * @param nextSeq  补到哪了（已下发最后一条的 seq + 1）
     * @return true = 真的前进了
     */
    public boolean ackGroupPending(String userId, String deviceId, String conversationId, long nextSeq) {
        if (conversationId == null || conversationId.isBlank() || nextSeq <= 0) {
            return false;
        }
        Long moved = redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                ADVANCE_GROUP_CURSOR, ReturnType.INTEGER, 1,
                key(groupCursorKey(userId, deviceId)),
                conversationId.getBytes(StandardCharsets.UTF_8),
                String.valueOf(nextSeq).getBytes(StandardCharsets.UTF_8),
                String.valueOf(pointerTtl.toMillis()).getBytes(StandardCharsets.UTF_8)));
        return moved != null && moved > 0;
    }

    /**
     * 某台设备在某个会话上已确认补发到的位置（0 = 该设备从未补过这个会话）。
     *
     * <p>这个值抬高了补发起点：设备已经补到 20，就不该再从锚点 7 重新补 7~19。
     */
    public long groupCursor(String userId, String deviceId, String conversationId) {
        if (conversationId == null || conversationId.isBlank()) {
            return 0L;
        }
        Object raw = redis.opsForHash().get(groupCursorKey(userId, deviceId), conversationId);
        Long seq = parseSeq(raw);
        return seq == null ? 0L : seq;
    }

    /**
     * 该用户登记过的设备（网关在建连时写入，见 {@code GatewayRegistry#bindUser}）。
     *
     * <p>空集合意味着「不知道有哪些端」——调用方必须保守处理（不要清理共享状态），
     * 因为那可能是旧版网关还没写过设备表。
     */
    public Set<String> knownDevices(String userId) {
        Set<String> members = redis.opsForSet().members(RedisKeys.userDevices(userId));
        return members == null ? Set.of() : members;
    }

    /**
     * 是否<b>所有</b>已登记设备都已经补到 {@code nextSeq} 或之后。
     *
     * <p>这是「能不能清掉群聊补发锚点」的唯一安全判据：
     * <ul>
     *   <li>还有设备没补到 → 锚点必须留着（新位置要对齐到更早的那台设备）；</li>
     *   <li>设备表为空 / 查不到 → 保守返回 false，宁可让锚点留到 TTL 过期，也不能误删；</li>
     *   <li>某台设备根本没有该会话的游标（= 0）→ 也算没补到。</li>
     * </ul>
     *
     * @param devices 预取的设备集合（一次 ack 里多个会话共用，避免逐个 SMEMBERS）
     */
    public boolean allDevicesReached(Set<String> devices, String userId,
                                     String conversationId, long nextSeq) {
        if (devices == null || devices.isEmpty() || conversationId == null || conversationId.isBlank()) {
            return false;
        }
        for (String device : devices) {
            if (groupCursor(userId, device, conversationId) < nextSeq) {
                return false;
            }
        }
        return true;
    }

    /**
     * 清掉某个会话的补发<b>锚点</b>（{@code im:offline:ptr:{userId}} 的一个 field）。
     *
     * <p>只有两种时机是安全的：
     * <ol>
     *   <li><b>读路径 + 起点就是锚点 + 查不到任何历史</b>：锚点之后对所有设备都没有数据；</li>
     *   <li><b>确认路径 + 所有已登记设备都已补到这个位置</b>（{@link #allDevicesReached}）：
     *       没有任何端还需要从更早的位置补。</li>
     * </ol>
     *
     * <p>反过来「某台设备补完就删」是不安全的：其它端会失去锚点——轻则从 seq 1 重拉整个
     * 会话历史（新设备上线时），重则把还没补的那段永久标记成已补发。
     */
    public void clearPending(String userId, String conversationId) {
        redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                CLEAR_PENDING, ReturnType.INTEGER, 1,
                key(RedisKeys.userOfflinePtr(userId)),
                conversationId.getBytes(StandardCharsets.UTF_8)));
    }

    // ==== 内部 ====

    /**
     * 按位置区间读帧（只读命令，不删除任何数据）。
     *
     * <p>用 low-level 的 {@code zRangeByScoreWithScores} 而不是 {@code opsForZSet()}：
     * 只有前者同时支持「开区间下界」和「LIMIT」，而开区间正是「游标之后」的语义
     * （闭区间会把上一次已经确认过的那一条再读一遍）。
     */
    private Set<Tuple> readFrames(byte[] queueKey, Range<Double> range, Limit limit) {
        return redis.execute((RedisCallback<Set<Tuple>>) connection ->
                connection.zSetCommands().zRangeByScoreWithScores(queueKey, range, limit));
    }

    /**
     * 队列 key 带 {@code {userId}} hash tag：同一个用户的两个 key（队列 + 位置计数器）
     * 必须落在同一个 slot，否则 Cluster 下 {@link #PUSH_FRAME} 会报 CROSSSLOT。
     */
    private static byte[] queueKey(String userId) {
        return key(queueKeyOf(userId));
    }

    /** key 的单一来源：测试清理也用它构造，避免 key 格式改了以后测试静默失效 */
    static String queueKeyOf(String userId) {
        return RedisKeys.userOfflineQueue(userId);
    }

    private static byte[] positionKey(String userId) {
        return key(RedisKeys.userOfflinePosition(userId));
    }

    private static String cursorKey(String userId, String deviceId) {
        return RedisKeys.userOfflineCursor(userId, normalizeDevice(deviceId));
    }

    /** 包级可见：供测试清理游标 key */
    static String cursorKeyOf(String userId, String deviceId) {
        return cursorKey(userId, deviceId);
    }

    /** 群聊补发的设备级游标 key（Hash：conversationId → 已补发到的 seq） */
    private static String groupCursorKey(String userId, String deviceId) {
        return RedisKeys.userOfflineGroupCursor(userId, normalizeDevice(deviceId));
    }

    /** 包级可见：供测试清理群聊游标 key */
    static String groupCursorKeyOf(String userId, String deviceId) {
        return groupCursorKey(userId, deviceId);
    }

    /** 空 deviceId 与网关的 default 槽位对齐，避免同一台「无 deviceId 客户端」产生两套游标 */
    public static String normalizeDevice(String deviceId) {
        return deviceId == null || deviceId.isBlank() ? DEFAULT_DEVICE : deviceId;
    }

    private static byte[] key(String redisKey) {
        return redisKey.getBytes(StandardCharsets.UTF_8);
    }

    private static Long parseSeq(Object raw) {
        if (raw == null) {
            return null;
        }
        try {
            return Long.parseLong(String.valueOf(raw).trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
