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
import java.util.Arrays;
import java.util.List;

/**
 * 基于 Redis 的「userId → 网关实例」在线路由表（拆分前叫 UserNodeRegistry）。
 *
 * <p>拆分后 key 语义更精确：
 * <pre>
 *   im:gw:user:{userId} = gatewayInstanceId   // 带 TTL，靠心跳续期
 * </pre>
 * 值的含义从「IM 节点地址」变成「长连接网关实例地址」，因为只有网关才知道
 * WebSocket 会话在哪个进程里。IM 服务只读这张表，不写。
 *
 * <p><b>一致性约定</b>：本表是最终一致（TTL 30s / 续期 10s），允许秒级不一致。
 * 不一致窗口内的重复推送由 IM 服务按 {@code PushResult.failed_user_ids} 落离线兜底，
 * 不做分布式锁强一致——代价与收益不成比例。
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
            redis.opsForValue().set(RedisKeys.GATEWAY_USER_PREFIX + userId, localInstanceId, TTL);
        }
        log.debug("heartbeat renewed {} routing entries on {}", userIds.size(), localInstanceId);
    }

    public void bindUser(String userId) {
        redis.opsForValue().set(RedisKeys.GATEWAY_USER_PREFIX + userId, localInstanceId, TTL);
    }

    public void unbindUser(String userId) {
        redis.delete(RedisKeys.GATEWAY_USER_PREFIX + userId);
    }

    /**
     * 查某用户当前连在哪个网关实例上；null = 不在线（或路由已过期）。
     */
    public String instanceOf(String userId) {
        return redis.opsForValue().get(RedisKeys.GATEWAY_USER_PREFIX + userId);
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
            int deleted = 0;
            List<byte[]> pending = new ArrayList<>(SCAN_BATCH);
            // 用 try-with-resources 保证游标（以及它持有的连接）一定关闭
            try (Cursor<byte[]> cursor = connection.keyCommands().scan(options)) {
                while (cursor.hasNext()) {
                    byte[] key = cursor.next();
                    byte[] value = connection.stringCommands().get(key);
                    if (Arrays.equals(deadInstanceBytes, value)) {
                        pending.add(key);
                    }
                    if (pending.size() >= SCAN_BATCH) {
                        deleted += deleteBatch(connection, pending);
                    }
                }
            }
            deleted += deleteBatch(connection, pending);
            return deleted;
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
