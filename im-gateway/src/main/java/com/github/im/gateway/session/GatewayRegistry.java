package com.github.im.gateway.session;

import com.github.im.contract.RedisKeys;
import com.github.im.gateway.node.NodeAddressResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 基于 Redis 的「userId → 网关实例集合」在线路由表（拆分前叫 UserNodeRegistry）。
 *
 * <p>拆分后 key 语义更精确：
 * <pre>
 *   im:gw:user:{userId} = SET{ gatewayInstanceId, ... }   // 带 TTL，靠心跳续期
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
     * SCAN 每批的 COUNT 提示值。
     *
     * <p>太小 → 往返次数多、清理慢；太大 → 单次命令阻塞 Redis 时间变长（Redis 单线程，
     * 清理动作不该拖慢正常读写）。500 是「一次几十万 key 也能在秒级扫完」的常用折中。
     */
    private static final int SCAN_BATCH = 500;

    private final StringRedisTemplate redis;
    private final ConnectionManager connectionManager;
    private final String localInstanceId;

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
            safeSadd(RedisKeys.GATEWAY_USER_PREFIX + userId, localInstanceId);
        }
        log.debug("heartbeat renewed {} routing entries on {}", userIds.size(), localInstanceId);
    }

    /** 用户在本实例上建连：把自己加进该用户的实例集合 */
    public void bindUser(String userId) {
        safeSadd(RedisKeys.GATEWAY_USER_PREFIX + userId, localInstanceId);
    }

    /**
     * 用户在<b>本实例上已无任何设备</b>时调用：只摘掉自己这个实例。
     *
     * <p>不能直接 DEL：同一用户可能还有别的设备连在其它网关实例上，DEL 会把它们一起判成离线。
     */
    public void unbindUser(String userId) {
        String key = RedisKeys.GATEWAY_USER_PREFIX + userId;
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
    }

    /**
     * 查某用户当前连在哪些网关实例上；空集合 = 不在线（或路由已过期）。
     *
     * <p>多端时可能返回多个实例，调用方必须逐个推送，不能只取第一个。
     */
    public Set<String> instancesOf(String userId) {
        String key = RedisKeys.GATEWAY_USER_PREFIX + userId;
        try {
            Set<String> members = redis.opsForSet().members(key);
            return members == null ? Set.of() : members;
        } catch (RuntimeException e) {
            log.debug("[route] 路由 key 类型不兼容（旧 String 格式），按离线处理：key={}", key);
            return Set.of();
        }
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
        try {
            redis.opsForSet().add(key, instanceId);
        } catch (RuntimeException e) {
            log.warn("[route] 路由 key 类型不兼容，删除重建：key={} err={}", key, e.getMessage());
            redis.delete(key);
            redis.opsForSet().add(key, instanceId);
        }
        redis.expire(key, TTL);
    }

    /** 本网关实例 ID（= 本实例 gRPC 可达地址 host:port） */
    public String localInstanceId() {
        return localInstanceId;
    }

    /**
     * 清除「指向某个已下线网关实例」的全部用户路由。
     *
     * <p>由 {@code GatewayNacosDiscovery} 的实例变更监听触发：当某网关实例被 Nacos
     * 标记为不健康，其它健康实例扫描 Redis 删掉指向它的条目，使这些用户被视为离线，
     * 后续消息不再推往死实例，而由 IM 服务落离线队列。
     *
     * <p><b>为什么用 SCAN 而不是 KEYS</b>：{@code KEYS} 是 O(N) 且<b>一次性阻塞</b> Redis 单线程，
     * 在线用户上百万时会把整库读写卡住几百毫秒——清理一个死节点不该拖垮全局。
     * SCAN 用游标分批（每批 {@value #SCAN_BATCH} 个提示值），把阻塞摊到多轮，且不占用大块内存。
     *
     * <p>扫描过程中删掉已处理的 key 是安全的：SCAN 只保证「整个扫描期间一直存在的元素」必被返回，
     * 处理完就删的条目本来也不需要再见到第二次。
     *
     * <p>代价：本条路径是 O(全部在线用户)，因为「nodeId → 用户集合」没有索引。若要变成
     * O(受影响用户数)，需要额外维护 {@code im:gw:node:{instanceId}} 集合（只在 bind/unbind 时改），
     * 但那样又多一份需要保持一致的状态；当前规模下 SCAN 足够。
     */
    public void removeInstanceBindings(String deadInstanceId) {
        byte[] deadInstanceBytes = deadInstanceId.getBytes(StandardCharsets.UTF_8);
        ScanOptions options = ScanOptions.scanOptions()
                .match(RedisKeys.GATEWAY_USER_PREFIX + "*")
                .count(SCAN_BATCH)
                .build();

        // 整段扫描共用一个连接：游标是「有状态的迭代」，跨多次 execute 会丢游标
        Integer removed = redis.execute((RedisCallback<Integer>) connection -> {
            int removedMembers = 0;
            List<byte[]> emptyKeys = new ArrayList<>(SCAN_BATCH);
            // 用 try-with-resources 保证游标（以及它持有的连接）一定关闭
            try (Cursor<byte[]> cursor = connection.keyCommands().scan(options)) {
                while (cursor.hasNext()) {
                    byte[] key = cursor.next();
                    // 集合语义：只把死实例从集合里摘掉，不要整条删掉——
                    // 同一用户还有设备连在其它健康实例上时必须留着
                    Long srem = connection.setCommands().sRem(key, deadInstanceBytes);
                    if (srem == null || srem == 0) {
                        continue;
                    }
                    removedMembers++;
                    Long remaining = connection.setCommands().sCard(key);
                    if (remaining == null || remaining == 0) {
                        emptyKeys.add(key);
                    }
                    if (emptyKeys.size() >= SCAN_BATCH) {
                        deleteBatch(connection, emptyKeys);
                    }
                }
            }
            deleteBatch(connection, emptyKeys);
            return removedMembers;
        });

        if (removed != null && removed > 0) {
            log.info("removed {} stale routing entries for dead gateway {}", removed, deadInstanceId);
        }
    }

    /** 批量删除并清空待删列表；返回真正被删掉的条数（过期消失的 key 不计入） */
    private static int deleteBatch(RedisConnection connection, List<byte[]> pending) {
        if (pending.isEmpty()) {
            return 0;
        }
        Long deleted = connection.keyCommands().del(pending.toArray(byte[][]::new));
        pending.clear();
        return deleted == null ? 0 : deleted.intValue();
    }
}
