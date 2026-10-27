package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 客户端消费水位表：{@code conversationId -> 该会话中客户端已见过的最大 seq}。
 *
 * <h2>它同时承担两件不同的事</h2>
 *
 * <p><b>一、seq 发号器的第三道下界</b>（{@link SeqAllocator} 用）。
 * 发号器的下界原本只有两个来源：本进程已知的最大 seq、以及 {@code MessageRepository.maxSeq}。
 * 当消息存储与发号器<b>共用同一个 Redis</b> 且整体丢失时（无持久化重启、FLUSH、回滚到旧快照），
 * 这两个来源会同时失效：下界变 0，seq 从 1 重新开始。此时库里没有冲突，但客户端
 * {@code lastSeq} 已经推进到 N，服务端却重新发 1、2、3，客户端会把它们判成「已消费过的历史」
 * 直接丢弃，且水位不再前进——表现为<b>新消息永久不显示</b>，直到服务端 seq 追上 N。
 * 这一路走<b>进程内内存</b>，因为它恰恰要在「Redis 不可用」时仍然有效。
 *
 * <p><b>二、delivered 的唯一可信口径</b>。
 * 网关写 {@code writeAndFlush} 成功只代表「进了 socket 缓冲区」，客户端可能早就断网了；
 * 只有客户端自己上报的水位才算「真的收到了」。这一路必须<b>持久化到 Redis</b>
 * （{@code im:wm:{userId}:{deviceId}}）：进程重启就清空的表撑不起「已送达」这个语义，
 * 而且它还是排查「用户说没收到消息」时唯一能对账的数据。
 *
 * <h2>粒度</h2>
 *
 * <p>按 {@code userId + deviceId} 分开存：手机和网页各自看到哪一条互不相干。
 * 用用户级单一水位会让「一端已读」把另一端也标成已读，未读数直接算错。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class ClientWatermarkStore {

    private static final Logger log = LoggerFactory.getLogger(ClientWatermarkStore.class);

    /**
     * 只前进地写一个会话的水位。
     *
     * <p>为什么必须只前进：客户端重连时会上报一批水位，网络乱序 + 多设备并发会让较小的值晚到；
     * 允许回退会让「已读位置」来回跳，未读数跟着抖。
     *
     * <p>返回值是「这次真的前进了吗」（1/0）：重复上报（sync 与增量 ACK 重叠）是常态，
     * 下游据此决定要不要给对端推一条 {@code msg.state} 已送达回执——没前进就没必要推。
     *
     * <pre>KEYS[1] = im:wm:{userId}:{deviceId}；ARGV[1] = conversationId；ARGV[2] = seq；ARGV[3] = TTL 毫秒</pre>
     */
    private static final byte[] ADVANCE_WATERMARK = """
            local cur = tonumber(redis.call('HGET', KEYS[1], ARGV[1]) or '0')
            redis.call('PEXPIRE', KEYS[1], ARGV[3])
            if tonumber(ARGV[2]) > cur then
                redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
                return 1
            end
            return 0
            """.getBytes(StandardCharsets.UTF_8);

    /** 水位只增不减：同一会话多次上报取最大值（进程内快路径） */
    private final Map<String, Long> watermarks = new ConcurrentHashMap<>();

    /** 可空：单测里直接 new 出来只用进程内那一路 */
    private final StringRedisTemplate redis;
    private final Duration ttl;

    public ClientWatermarkStore() {
        this(null, Duration.ofDays(30));
    }

    @Autowired
    public ClientWatermarkStore(StringRedisTemplate redis,
                                @Value("${im.watermark.ttl:30d}") Duration ttl) {
        this.redis = redis;
        this.ttl = ttl;
    }

    /**
     * 合并一批上报的水位（按会话取较大值），<b>只知道会话维度</b>。
     *
     * <p>保留这个重载是给 {@link SeqAllocator} 与单测用的：发号器只关心「这个会话的下界」，
     * 不关心是谁上报的。真正需要落 Redis 的调用请走
     * {@link #observe(String, String, Map)}。
     */
    public void observe(Map<String, Long> reported) {
        if (reported == null || reported.isEmpty()) {
            return;
        }
        reported.forEach((conversationId, seq) -> {
            if (invalid(conversationId, seq)) {
                return;
            }
            watermarks.merge(conversationId, seq, Math::max);
        });
    }

    /**
     * 记录某台设备上报的水位：进程内 + Redis 双写。
     *
     * <p>Redis 写失败只记 warn 不抛：水位是「兜底增益」而不是关键路径，
     * 让一次 Redis 抖动把整次 {@code pullPending} 打挂，等于用次要信息换掉了主流程。
     *
     * @return 本次<b>真的前进了</b>的会话（conversationId → 新水位）。
     *         重复上报（sync 与增量 ACK 重叠、多端各报一次）不会出现在结果里——
     *         端到端回执只应该在状态真的变化时推给对端，否则每条消息都会放大成 N 条回执。
     */
    public Map<String, Long> observe(String userId, String deviceId, Map<String, Long> reported) {
        observe(reported);
        if (redis == null || userId == null || userId.isBlank() || reported == null || reported.isEmpty()) {
            return Map.of();
        }
        List<Map.Entry<String, Long>> valid = new ArrayList<>(reported.size());
        for (Map.Entry<String, Long> entry : reported.entrySet()) {
            if (!invalid(entry.getKey(), entry.getValue())) {
                valid.add(entry);
            }
        }
        if (valid.isEmpty()) {
            return Map.of();
        }
        String key = keyOf(userId, deviceId);
        byte[] ttlMillis = String.valueOf(ttl.toMillis()).getBytes(StandardCharsets.UTF_8);
        try {
            List<Object> results = redis.executePipelined((RedisCallback<Object>) connection -> {
                for (Map.Entry<String, Long> entry : valid) {
                    connection.scriptingCommands().eval(ADVANCE_WATERMARK, ReturnType.INTEGER, 1,
                            key.getBytes(StandardCharsets.UTF_8),
                            entry.getKey().getBytes(StandardCharsets.UTF_8),
                            String.valueOf(entry.getValue()).getBytes(StandardCharsets.UTF_8),
                            ttlMillis);
                }
                return null;
            });
            Map<String, Long> advanced = new LinkedHashMap<>();
            for (int i = 0; i < valid.size() && i < results.size(); i++) {
                // pipeline 里的脚本返回值是 1/0；只收集前进的那些（回执只在这些会话上推）
                if (results.get(i) instanceof Number number && number.longValue() > 0) {
                    advanced.put(valid.get(i).getKey(), valid.get(i).getValue());
                }
            }
            return advanced;
        } catch (RuntimeException e) {
            log.warn("[watermark] 持久化客户端水位失败（不影响补发与发号）user={} device={} err={}",
                    userId, deviceId, e.getMessage());
            return Map.of();
        }
    }

    /**
     * 该设备在某个会话上<b>亲口确认收到</b>的最大 seq（0 = 从未上报）。
     *
     * <p>这是「已送达」的可查询口径：网关侧的游标只说明「写进了 socket」，这里才说明
     * 「客户端真的收到了」。
     */
    public long deliveredSeq(String userId, String deviceId, String conversationId) {
        if (redis == null || userId == null || conversationId == null) {
            return 0L;
        }
        Object raw = redis.opsForHash().get(keyOf(userId, deviceId), conversationId);
        if (raw == null) {
            return 0L;
        }
        try {
            return Long.parseLong(String.valueOf(raw).trim());
        } catch (NumberFormatException e) {
            return 0L;
        }
    }

    /**
     * 批量读该设备已确认的水位（只查给定的会话）。
     *
     * <p>补发起点要用它：客户端已经 ACK 过的 seq 不该再补一遍。之所以只查给定的会话而不是
     * 整张 Hash——一个长期用户可能有几百个会话，补发一次只关心其中二十几个（单轮处理上限）。
     *
     * @return conversationId → 已确认到的 seq；没确认过的不会出现在结果里
     */
    public Map<String, Long> deliveredSeqs(String userId, String deviceId, Collection<String> conversationIds) {
        if (redis == null || userId == null || conversationIds == null || conversationIds.isEmpty()) {
            return Map.of();
        }
        List<String> ids = List.copyOf(new LinkedHashSet<>(conversationIds));
        byte[] key = keyOf(userId, deviceId).getBytes(StandardCharsets.UTF_8);
        List<Object> raw = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (String conversationId : ids) {
                connection.hashCommands().hGet(key, conversationId.getBytes(StandardCharsets.UTF_8));
            }
            return null;
        });
        Map<String, Long> result = new LinkedHashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            Object value = raw.get(i);
            if (value == null) {
                continue;
            }
            try {
                result.put(ids.get(i), Long.parseLong(String.valueOf(value).trim()));
            } catch (NumberFormatException ignored) {
                // 脏值当作「没确认过」，宁可多补一遍也不要把起点抬错
            }
        }
        return result;
    }

    /** @return 该会话已知的客户端水位（进程内）；{@code 0} = 从未上报过 */
    public long maxSeqOf(String conversationId) {
        return watermarks.getOrDefault(conversationId, 0L);
    }

    /** 仅用于可观测性 / 测试断言 */
    public int size() {
        return watermarks.size();
    }

    /** key 的单一来源：测试清理也用它构造，避免 key 格式改了以后测试静默失效 */
    public static String keyOf(String userId, String deviceId) {
        return RedisKeys.userWatermark(userId, OfflineMessageStore.normalizeDevice(deviceId));
    }

    private static boolean invalid(String conversationId, Long seq) {
        return conversationId == null || conversationId.isBlank() || seq == null || seq <= 0;
    }
}
