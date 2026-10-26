package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReturnType;
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

/**
 * 离线存储（归属 IM 服务，不再放在连接层）。
 *
 * <h2>两条通道，对应两种「放大倍数」</h2>
 *
 * <p><b>单聊 —— 帧队列</b>{@code im:offline:{userId}}（List）：一条消息只有一个收件人，
 * 存一份完整帧就是最省的形态，直接存，上线按 FIFO 补发。
 *
 * <p><b>群聊 —— 补发指针</b>{@code im:offline:ptr:{userId}}（Hash，{@code conversationId -> 待补发 seq}）：
 * 群消息的收件人是全体成员，若也按人存帧，一条消息的存储量 = 不在线人数。
 * 500 人群里 450 人不在线、50 人各发 5 条，就是 250 × 450 = 112,500 份相同帧（~30MB），
 * 而且会随消息数线性增长。改成指针后，每个「用户 × 会话」只占一个 field（几十字节），
 * 后续消息只把同一个 field 往前推——<b>占用不随消息数增长</b>，帧在补发时由
 * {@link ClientFrames#frameOf} 从 {@code im:conv:msgs:{conv}} / {@code im_message} 重建。
 *
 * <h2>为什么两种都带 TTL 与上限</h2>
 *
 * <p>离线队列的写方是「消息」，读方却是「用户上线」——两者完全解耦，一个长期不上线的用户
 * 会让队列无界增长。所以两条通道都必须有兜底：帧队列有长度上限（{@code im.offline.max-frames}）
 * 与过期时间（{@code im.offline.frame-ttl}），指针有过期时间（{@code im.offline.pointer-ttl}）。
 * 超期未上线 = 放弃补发，历史仍在消息存储里，客户端可以主动拉历史。
 *
 * <h2>写路径全部是原子的</h2>
 *
 * <p>「追加 + 截断 + 续期」「HSETNX + 续期」「推进/清理指针」各自是一个 Lua 脚本：
 * 分成多条命令时，进程崩在中间会留下没有 TTL 的 key（永久泄漏），或把指针推过一个还没发出的 seq。
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
     * <p>RPUSH / HSETNX 都很轻（不返回数据），单片控制在毫秒级即可，避免一条大群消息把 Redis 顶住。
     */
    private static final int PIPELINE_BATCH = 500;

    /** 帧队列：追加 + 截断到上限 + 续期，一把原子完成（详见类注释） */
    private static final byte[] PUSH_FRAME = """
            redis.call('RPUSH', KEYS[1], ARGV[1])
            redis.call('LTRIM', KEYS[1], -tonumber(ARGV[2]), -1)
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            return 1
            """.getBytes(StandardCharsets.UTF_8);

    /** 取走最多 N 条：LRANGE + LTRIM 必须在同一脚本里，否则并发 pull 会重复取走并二次截断（丢消息） */
    private static final byte[] DRAIN_FRAMES = """
            local frames = redis.call('LRANGE', KEYS[1], 0, tonumber(ARGV[1]) - 1)
            if #frames > 0 then
                redis.call('LTRIM', KEYS[1], #frames, -1)
            end
            return frames
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
     * 推进或清理补发指针（原子）。
     *
     * <pre>
     * KEYS[1] = im:offline:ptr:{userId}
     * ARGV[1] = conversationId
     * ARGV[2] = 本次读到的旧指针（期望值）
     * ARGV[3] = 新指针（已补到的最后一条 seq + 1）
     * ARGV[4] = '1' 表示本次已追平（历史里没有更多了）
     * </pre>
     *
     * <p>三种结果，都不能丢消息：
     * <ul>
     *   <li>追平且期间没有新消息（值仍是期望值）→ 删掉 field，key 空则自然消失；</li>
     *   <li>追平但期间又落了新消息（值被改小以外的写改过）→ 只前进不删除，
     *       下一轮 pull 会把多出来的补上；</li>
     *   <li>没追平 → 单调前进，<b>绝不回退</b>（回退会导致重复补发甚至死循环）。</li>
     * </ul>
     */
    private static final byte[] ADVANCE_PENDING = """
            local cur = redis.call('HGET', KEYS[1], ARGV[1])
            if cur == false then
                return 0
            end
            if ARGV[4] == '1' and cur == ARGV[2] then
                return redis.call('HDEL', KEYS[1], ARGV[1])
            end
            if tonumber(ARGV[3]) > tonumber(cur) then
                redis.call('HSET', KEYS[1], ARGV[1], ARGV[3])
                return 1
            end
            return 0
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
        log.info("[offline] 单聊帧队列上限 {} 条/人、TTL {}；群聊补发指针 TTL {}",
                this.maxFramesPerUser, frameTtl, pointerTtl);
    }

    // ==== 单聊：帧队列 ====

    public void store(String userId, String frame) {
        redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                PUSH_FRAME, ReturnType.INTEGER, 1,
                key(RedisKeys.OFFLINE_PREFIX + userId),
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
                        connection.scriptingCommands().eval(PUSH_FRAME, ReturnType.INTEGER, 1,
                                key(RedisKeys.OFFLINE_PREFIX + userId), payload, maxFrames, ttlMillis);
                    }
                    return null;
                });
                stored += batch.size();
            } catch (RuntimeException e) {
                // Cluster 跨 slot（每个用户一个 key，必然不同 slot）或 Redis 瞬时故障：
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
     * 取走最多 {@code max} 条离线帧（单聊 + 升级前遗留的群聊帧）。
     *
     * <p>{@code LRANGE} 与 {@code LTRIM} 在一个 Lua 里完成：分成两条命令时，
     * 两个并发 pull（重连风暴、多端同时上线）会各自读到同一批、再各自截断一次，直接丢消息。
     */
    public List<String> drain(String userId, int max) {
        if (max <= 0) {
            return List.of();
        }
        List<byte[]> raw = redis.execute((RedisCallback<List<byte[]>>) connection ->
                connection.scriptingCommands().eval(DRAIN_FRAMES, ReturnType.MULTI, 1,
                        key(RedisKeys.OFFLINE_PREFIX + userId),
                        String.valueOf(max).getBytes(StandardCharsets.UTF_8)));
        if (raw == null || raw.isEmpty()) {
            return List.of();
        }
        List<String> frames = new ArrayList<>(raw.size());
        for (byte[] frame : raw) {
            frames.add(new String(frame, StandardCharsets.UTF_8));
        }
        return frames;
    }

    public long size(String userId) {
        Long size = redis.opsForList().size(RedisKeys.OFFLINE_PREFIX + userId);
        return size == null ? 0L : size;
    }

    // ==== 群聊：补发指针 ====

    /**
     * 记录「这批人在这个会话上还有未补发的消息」。
     *
     * <p>每条群消息对每个不在线成员只写一个 field（几十字节），而不是整帧（几百字节），
     * 且同一会话的后续消息只把 field 往前推，<b>总占用不随消息数增长</b>。
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
                                key(RedisKeys.OFFLINE_PTR_PREFIX + userId), conversation, seqValue, ttlMillis);
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
                                key(RedisKeys.OFFLINE_PTR_PREFIX + userId), conversation, seqValue, ttlMillis));
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
     * <p>排序是为了多次 pull 之间能稳定推进：每次只处理前 N 个，处理完的会被推进或删除，
     * 保证不会永远卡在同一批会话上。
     *
     * @return conversationId → 待补发 seq（最早的未补发位置）
     */
    public Map<String, Long> pendingConversations(String userId, int limit) {
        if (limit <= 0) {
            return Map.of();
        }
        Map<Object, Object> entries = redis.opsForHash().entries(RedisKeys.OFFLINE_PTR_PREFIX + userId);
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
        Long size = redis.opsForHash().size(RedisKeys.OFFLINE_PTR_PREFIX + userId);
        return size == null ? 0L : size;
    }

    /**
     * 推进（或清理）补发指针，语义见 {@link #ADVANCE_PENDING}。
     *
     * @param expectedFrom 本次拉取开始前读到的指针值（用于判断期间是否又被写入）
     * @param nextSeq      补到哪了（已补发最后一条的 seq + 1）
     * @param caughtUp     本次是否已经把历史补完
     */
    public void advancePending(String userId, String conversationId, long expectedFrom, long nextSeq, boolean caughtUp) {
        redis.execute((RedisCallback<Long>) connection -> connection.scriptingCommands().eval(
                ADVANCE_PENDING, ReturnType.INTEGER, 1,
                key(RedisKeys.OFFLINE_PTR_PREFIX + userId),
                conversationId.getBytes(StandardCharsets.UTF_8),
                String.valueOf(expectedFrom).getBytes(StandardCharsets.UTF_8),
                String.valueOf(nextSeq).getBytes(StandardCharsets.UTF_8),
                (caughtUp ? "1" : "0").getBytes(StandardCharsets.UTF_8)));
    }

    // ==== 内部 ====

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
