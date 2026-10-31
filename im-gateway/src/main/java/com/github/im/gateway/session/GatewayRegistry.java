package com.github.im.gateway.session;

import com.github.im.contract.RedisKeys;
import com.github.im.contract.resilience.RedisDegrade;
import com.github.im.gateway.node.NodeAddressResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.ReturnType;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 基于 Redis 的「userId → 网关实例集合」在线路由表（拆分前叫 UserNodeRegistry）。
 *
 * <p>拆分后 key 语义更精确：
 * <pre>
 *   im:gateway:user:{userId} = SET{ gatewayInstanceId, ... }   // 带 TTL，靠心跳续期
 * </pre>
 * 值从「单个实例」变成「实例集合」是为了支持多端：同一用户可能手机连在网关 A、
 * 网页连在网关 C，下行推送必须同时打到 A 和 C，再由各自把帧分给本机上的设备
 * （{@code ConnectionManager.send} 负责最后一跳的多设备扇出）。
 *
 * <p>只有网关才知道会话在哪个进程里，所以这张表由网关写、IM 服务只读。
 *
 * <p><b>一致性约定</b>：本表是最终一致（TTL 30s / 续期 10s），允许秒级不一致。
 * 不一致窗口内的重复推送由 IM 服务按 {@code PushResult.failed_user_ids} 落离线兜底，
 * 不做分布式锁强一致——代价与收益不成比例。
 *
 * <p><b>类型迁移</b>：set 化之前这个 key 是 String（GET/SET）。滚动升级时可能读到旧类型，
 * Redis 会报 WRONGTYPE；所有写路径都做了「删掉重建」的自愈（见 {@link #safeSadd}），
 * 读路径遇到旧类型按「不在线」处理（TTL 只有 30s，很快自愈）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class GatewayRegistry {

    private static final Logger log = LoggerFactory.getLogger(GatewayRegistry.class);

    private static final Duration TTL = Duration.ofSeconds(30);

    /**
     * 设备集合的保留时长，比路由 TTL 长得多。
     *
     * <p>路由表是「现在谁在线」的实时缓存，30s 过期正是它想要的语义；
     * 设备集合是「这个人有哪些端」的历史事实，要覆盖「离线几天后上线补发」的场景。
     * 30 天与客户端水位（{@code im.watermark.ttl}）对齐：超过这个时间还没上线的端，
     * 本来也不该再收到那么早的消息。
     */
    private static final Duration DEVICE_TTL = Duration.ofDays(30);

    /**
     * SCAN 每批的 COUNT 提示值。
     *
     * <p>太小 → 往返次数多、清理慢；太大 → 单次命令阻塞 Redis 时间变长（Redis 单线程，
     * 清理动作不该拖慢正常读写）。500 是「一次几十万 key 也能在秒级扫完」的常用折中。
     */
    private static final int SCAN_BATCH = 500;

    /**
     * 「摘掉一个成员；摘空了就删 key」合并成一次原子执行。
     *
     * <p>为什么必须原子：拆成 {@code SREM → SCARD → (0 才 DEL)} 时，SCARD 与 DEL 之间若有客户端
     * 在同一个 key 上重连并 {@code SADD}，DEL 会把刚写好的新路由一起删掉——那个用户被判离线，
     * 消息落离线队列，直到下一轮 10s 心跳续期才写回。
     *
     * <p>为什么还需要「删空 key」这段：Redis 在集合被摘空时会自动删除 key，因此正常数据下
     * SCARD 会直接返回 0/键不存在，DEL 是空操作。保留它是为了不依赖该实现细节
     * （换成不自动删空的结构或存储时仍然正确），代价只有一次 EVAL 往返。
     *
     * <p>{@code KEYS[1]} = {@code im:gateway:user:{userId}}，{@code ARGV[1]} = 死实例 ID；
     * 返回真正摘掉的成员数（0 或 1），避免为统计再多一次往返。
     */
    private static final byte[] SREM_AND_DELETE_IF_EMPTY = """
            local removed = redis.call('SREM', KEYS[1], ARGV[1])
            if removed == 1 and redis.call('SCARD', KEYS[1]) == 0 then
                redis.call('DEL', KEYS[1])
            end
            return removed
            """.getBytes(StandardCharsets.UTF_8);

    private final StringRedisTemplate redis;
    private final ConnectionManager connectionManager;
    private final String localInstanceId;
    /** 批量路由查询的分片并发执行器：纯阻塞等 Redis，虚拟线程足够 */
    private final ExecutorService routeShards = Executors.newVirtualThreadPerTaskExecutor();

    public GatewayRegistry(StringRedisTemplate redis,
                           ConnectionManager connectionManager,
                           NodeAddressResolver addressResolver) {
        this.redis = redis;
        this.connectionManager = connectionManager;
        this.localInstanceId = addressResolver.localInstanceId();
    }

    /** 心跳：续期所有本地在线用户的路由，防止网关崩溃后残留脏数据 */
    @Scheduled(fixedDelay = 10_000)
    public void heartbeat() {
        List<String> userIds = connectionManager.userIds();
        if (userIds.isEmpty()) {
            return;
        }
        for (String userId : userIds) {
            safeSadd(RedisKeys.gatewayUser(userId), localInstanceId);
        }
        log.debug("heartbeat renewed {} routing entries on {}", userIds.size(), localInstanceId);
    }

    /** 用户在本实例上建连：把自己加进该用户的实例集合 */
    public void bindUser(String userId) {
        bindUser(userId, null);
    }

    /**
     * 用户在本实例上建连：登记「实例路由」+「设备」。
     *
     * <p>设备集合是<b>多端同步</b>的前提：IM 服务侧要靠它判断「是不是所有端都已经补过某个位置」，
     * 从而安全地清掉群聊补发锚点。少了它，锚点只能靠 TTL 过期，期间每轮补发都为这些会话
     * 白查一次历史。
     *
     * <p><b>断开时不摘设备</b>（与 {@link #unbindUser} 刻意不对称）：离线补发需要知道
     * 「这个用户用过哪些端」，摘掉就等于把离线期间的消息只留给最后一个在线的端。
     * 换机留下的历史 deviceId 由 {@link #DEVICE_TTL} 自然回收。
     */
    public void bindUser(String userId, String deviceId) {
        safeSadd(RedisKeys.gatewayUser(userId), localInstanceId);
        if (deviceId != null && !deviceId.isBlank()) {
            safeSadd(RedisKeys.userDevices(userId), deviceId, DEVICE_TTL);
        }
    }

    /**
     * 用户在<b>本实例上已无任何设备</b>时调用：只摘掉自己这个实例。
     *
     * <p>不能直接 DEL：同一用户可能还有别的设备连在其它网关实例上，DEL 会把它们一起判成离线。
     */
    public void unbindUser(String userId) {
        String key = RedisKeys.gatewayUser(userId);
        RedisDegrade.run("route.unbind", () -> {
            try {
                Long removed = redis.opsForSet().remove(key, localInstanceId);
                if (removed != null && removed > 0 && isSetEmpty(key)) {
                    redis.delete(key);
                }
            } catch (RuntimeException e) {
                // 旧类型 key（String）或 Redis 瞬时故障：删掉让它自愈，不要让断开回调抛异常
                log.warn("[route] 清理路由失败，删除重建：key={} err={}", key, e.getMessage());
                redis.delete(key);
            }
        });
    }

    /**
     * 查某用户当前连在哪些网关实例上；空集合 = 不在线（或路由已过期）。
     *
     * <p>多端时可能返回多个实例，调用方必须逐个推送，不能只取第一个。
     */
    public Set<String> instancesOf(String userId) {
        String key = RedisKeys.gatewayUser(userId);
        try {
            Set<String> members = redis.opsForSet().members(key);
            return members == null ? Set.of() : members;
        } catch (RuntimeException e) {
            log.debug("[route] 路由 key 类型不兼容（旧 String 格式），按离线处理：key={}", key);
            return Set.of();
        }
    }

    /**
     * 批量查路由：{@code userId → 实例集合}，不在线的用户对应空集合。
     *
     * <p>单体形态下 {@code GatewayPushPort} 用它把「群聊按人查路由」从 N 次串行往返
     * 压成「分片 pipeline + 并发」。切片理由与读侧（{@code GatewayRouteReader}）一致：
     * Redis 单线程，一次塞几千条 SMEMBERS 会把它顶住 25~75ms。
     */
    public Map<String, Set<String>> instancesOfAll(Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Map.of();
        }
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        Map<String, Set<String>> routes = new ConcurrentHashMap<>(Math.max(16, ids.size() * 2));
        List<List<String>> shards = new ArrayList<>((ids.size() + SCAN_BATCH - 1) / SCAN_BATCH);
        for (int from = 0; from < ids.size(); from += SCAN_BATCH) {
            shards.add(ids.subList(from, Math.min(from + SCAN_BATCH, ids.size())));
        }
        if (shards.size() == 1) {
            queryShard(shards.getFirst(), routes);
            return routes;
        }
        List<Future<?>> futures = new ArrayList<>(shards.size());
        for (List<String> shard : shards) {
            futures.add(routeShards.submit(() -> queryShard(shard, routes)));
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[route] 批量路由查询被中断，未完成的部分按离线处理");
            } catch (ExecutionException e) {
                Throwable cause = e.getCause() == null ? e : e.getCause();
                log.warn("[route] 批量路由分片失败，该分片按离线处理: {}", cause.getMessage());
            }
        }
        return routes;
    }

    private void queryShard(List<String> ids, Map<String, Set<String>> out) {
        List<byte[]> keys = new ArrayList<>(ids.size());
        for (String id : ids) {
            keys.add(RedisKeys.gatewayUser(id).getBytes(StandardCharsets.UTF_8));
        }
        try {
            List<Object> raw = redis.executePipelined((RedisCallback<Object>) connection -> {
                for (byte[] key : keys) {
                    connection.setCommands().sMembers(key);
                }
                return null;
            });
            for (int i = 0; i < ids.size(); i++) {
                Object value = raw != null && i < raw.size() ? raw.get(i) : null;
                out.put(ids.get(i), toInstanceSet(value));
            }
        } catch (RuntimeException e) {
            // Cluster 跨 slot 或旧 String 类型 key：逐条退化，慢但不会让整条群消息全落离线
            log.debug("[route] 批量路由不可用（{}），退化为逐条查询", e.getMessage());
            for (String id : ids) {
                out.put(id, instancesOf(id));
            }
        }
    }

    /** pipeline 结果可能是 {@code Set<String>}，也可能是原始 {@code Set<byte[]>} */
    private static Set<String> toInstanceSet(Object value) {
        if (!(value instanceof Set<?> members) || members.isEmpty()) {
            return Set.of();
        }
        Set<String> instances = new LinkedHashSet<>(members.size());
        for (Object member : members) {
            if (member instanceof String text) {
                instances.add(text);
            } else if (member instanceof byte[] bytes) {
                instances.add(new String(bytes, StandardCharsets.UTF_8));
            }
        }
        return instances;
    }

    /** 只在「集合存在且为空」时返回 true，避免把还有成员（别的实例）的 key 删掉 */
    private boolean isSetEmpty(String key) {
        Long size = redis.opsForSet().size(key);
        return size == null || size == 0;
    }

    /**
     * SADD + 续期，遇到旧的 String 类型 key 时删掉重建。
     *
     * <p>滚动升级期间同一个人可能被新旧两版网关各写一次：新版写 SET、旧版写 String，
     * 于是双方都会间歇性吃到 WRONGTYPE。这里选择「删掉重建」而不是抛错，
     * 因为路由表本来就是可重建的缓存（TTL 30s），丢一次最多让一个用户短暂被判离线，
     * 由离线队列兜底，代价远小于连接建不起来。
     */
    private void safeSadd(String key, String instanceId) {
        safeSadd(key, instanceId, TTL);
    }

    private void safeSadd(String key, String member, Duration ttl) {
        // 第二次 add / expire 也必须受保护：原来 Redis 真宕机时这两句会把异常抛回连接建立流程，
        // 结果是「网关因为路由表写不进去而连不上」——路由只是可重建的缓存，不该有这个权力。
        boolean written = RedisDegrade.run("route.bind", () -> {
            try {
                redis.opsForSet().add(key, member);
            } catch (RuntimeException e) {
                log.warn("[route] key 类型不兼容，删除重建：key={} err={}", key, e.getMessage());
                redis.delete(key);
                redis.opsForSet().add(key, member);
            }
        });
        if (!written) {
            log.debug("[route] 路由登记降级（Redis 不可用），本实例仍是连接的权威：key={}", key);
            return;
        }
        RedisDegrade.run("route.bind.ttl", () -> redis.expire(key, ttl));
    }

    /** 本网关实例 ID（= 本实例 gRPC 可达地址 host:port） */
    public String localInstanceId() {
        return localInstanceId;
    }

    /**
     * 清除「指向某个已下线网关实例」的全部用户路由。
     *
     * <p>由 {@code GatewayNacosDiscovery} 的实例变更监听触发：当某网关实例被 Nacos
     * 标记为不健康，健康实例扫描 Redis 摘掉指向它的条目，使这些用户被视为离线，
     * 后续消息不再推往死实例，而由 IM 服务落离线队列。
     *
     * <p><b>调用方负责去重</b>：Nacos 会把「XX 不健康」推给每个订阅者，而本方法是一次
     * O(全部在线用户) 的全库扫描。如果 N-1 个活实例各扫一遍，正好砸在「刚掉一个节点、
     * 客户端批量重连」的时刻。所以由 {@code GatewayNacosDiscovery} 按确定性规则选出唯一
     * owner 才调用；漏清不致命——死实例的路由不再续期，30s TTL 自然过期，期间多出来的
     * 无效推送由 IM 服务的 {@code failed_user_ids} 落离线兜底。
     *
     * <p><b>为什么用 SCAN 而不是 KEYS</b>：{@code KEYS} 是 O(N) 且<b>一次性阻塞</b> Redis 单线程，
     * 在线用户上百万时会把整库读写卡住几百毫秒——清理一个死节点不该拖垮全局。
     * SCAN 用游标分批（每批 {@value #SCAN_BATCH} 个提示值），把阻塞摊到多轮，且不占用大块内存。
     *
     * <p><b>摘成员与删空 key 在同一个 Lua 脚本里完成</b>，因为分成两条命令不是原子的：
     * 见 {@link #SREM_AND_DELETE_IF_EMPTY}。
     *
     * <p>代价：本条路径是 O(全部在线用户)，因为「nodeId → 用户集合」没有索引。若要变成
     * O(受影响用户数)，需要额外维护 {@code im:gateway:node:{instanceId}} 集合（只在 bind/unbind 时改），
     * 但那样又多一份需要保持一致的状态；当前规模下 SCAN 足够。
     * 每个 key 一次 EVAL（脚本体很小，随命令一起发送，不做 EVALSHA 缓存）。
     */
    public void removeInstanceBindings(String deadInstanceId) {
        byte[] deadInstanceBytes = deadInstanceId.getBytes(StandardCharsets.UTF_8);
        ScanOptions options = ScanOptions.scanOptions()
                .match(RedisKeys.gatewayUserPattern())
                .count(SCAN_BATCH)
                .build();

        // 整段扫描共用一个连接：游标是「有状态的迭代」，跨多次 execute 会丢游标
        Integer removed = redis.execute((RedisCallback<Integer>) connection -> {
            int removedMembers = 0;
            // 用 try-with-resources 保证游标（以及它持有的连接）一定关闭
            try (Cursor<byte[]> cursor = connection.keyCommands().scan(options)) {
                while (cursor.hasNext()) {
                    byte[] key = cursor.next();
                    // 集合语义：只把死实例从集合里摘掉，不要整条删掉——
                    // 同一用户还有设备连在其它健康实例上时必须留着。
                    // 「摘空后删 key」必须和 SREM 在同一个脚本里，不能拆成 SCARD + DEL。
                    Long srem = connection.scriptingCommands()
                            .eval(SREM_AND_DELETE_IF_EMPTY, ReturnType.INTEGER, 1, key, deadInstanceBytes);
                    if (srem != null && srem > 0) {
                        removedMembers++;
                    }
                }
            }
            return removedMembers;
        });

        if (removed != null && removed > 0) {
            log.info("removed {} stale routing entries for dead gateway {}", removed, deadInstanceId);
        }
    }
}
