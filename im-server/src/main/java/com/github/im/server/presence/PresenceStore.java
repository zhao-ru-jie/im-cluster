package com.github.im.server.presence;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 在线状态的<b>读</b>侧（IM 服务只读，写入方是网关）。
 *
 * <h2>「在线」到底怎么判定</h2>
 *
 * <p>只有一条规则：网关写的在线路由集合 {@code im:gateway:user:{userId}} 非空。
 * 不额外维护一张「在线表」，理由是那张表必须和连接状态对账，而对账一定会漂移：
 * 网关崩溃、Redis 抖动、滚动升级期间的 key 类型迁移，任何一个都会让两张表分叉，
 * 于是出现「presence 说在线、消息却推不到」这种自相矛盾的状态。
 *
 * <p>路由表本身是为下行推送存在的：它一旦不准，消息就已经投不到了，
 * presence 准不准反而次要——所以复用它不引入新的不一致来源。
 *
 * <h2>为什么 fields 一次 pipeline 取</h2>
 *
 * <p>一次 presence 查询通常来自「打开好友列表」，批量几十到几百人。逐人 3 条命令
 * 就是上千次往返，客户端等的是「一屏头像的亮度」，不该为此付出秒级延迟。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class PresenceStore {

    private static final Logger log = LoggerFactory.getLogger(PresenceStore.class);

    private final StringRedisTemplate redis;
    /** lastSeen 的保留期：超过就当作「太久没上线」不展示，避免显示半年前的在线时间 */
    private final Duration lastSeenTtl;

    public PresenceStore(StringRedisTemplate redis,
                         @Value("${im.presence.last-seen-ttl:7d}") Duration lastSeenTtl) {
        this.redis = redis;
        this.lastSeenTtl = lastSeenTtl;
    }

    /**
     * 一个用户的在线快照。
     *
     * @param gateways 在线网关实例数（0 = 离线）
     * @param devices  该用户注册过的设备数（含离线设备，网关侧 TTL 30 天）
     * @param lastSeen 最近一次下线时间（毫秒，0 = 从未下线 / 已过保留期）
     */
    public record Snapshot(int gateways, int devices, long lastSeen) {

        public boolean online() {
            return gateways > 0;
        }

        static Snapshot offline() {
            return new Snapshot(0, 0, 0);
        }
    }

    /**
     * 批量取快照：每个用户 3 条命令（路由基数 / 设备基数 / lastSeen）合成一次 pipeline。
     *
     * <p>失败时退化为「全部按离线返回」而不是抛异常：presence 是展示信息，
     * 查询失败让整个好友列表报错，比显示一片灰更糟。
     */
    public Map<String, Snapshot> snapshotsOf(Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Map.of();
        }
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        Map<String, Snapshot> result = new ConcurrentHashMap<>(Math.max(16, ids.size() * 2));
        List<byte[]> routeKeys = new ArrayList<>(ids.size());
        List<byte[]> deviceKeys = new ArrayList<>(ids.size());
        List<byte[]> seenKeys = new ArrayList<>(ids.size());
        for (String id : ids) {
            routeKeys.add(bytes(RedisKeys.gatewayUser(id)));
            deviceKeys.add(bytes(RedisKeys.userDevices(id)));
            seenKeys.add(bytes(RedisKeys.presenceLastSeen(id)));
        }
        try {
            List<Object> raw = redis.executePipelined((RedisCallback<Object>) connection -> {
                for (int i = 0; i < ids.size(); i++) {
                    connection.setCommands().sCard(routeKeys.get(i));
                    connection.setCommands().sCard(deviceKeys.get(i));
                    connection.stringCommands().get(seenKeys.get(i));
                }
                return null;
            });
            for (int i = 0; i < ids.size(); i++) {
                int base = i * 3;
                result.put(ids.get(i), new Snapshot(
                        number(raw, base),
                        number(raw, base + 1),
                        millis(raw, base + 2)));
            }
        } catch (RuntimeException e) {
            // Cluster 跨 slot / 旧 String 类型 key / Redis 瞬断：整体降级为离线，
            // 逐条退化在这里没有收益——多打几百次注定失败的往返，只会把故障放大
            log.debug("[presence] 批量查询失败，按离线返回 users={} err={}", ids.size(), e.getMessage());
            for (String id : ids) {
                result.put(id, Snapshot.offline());
            }
        }
        return result;
    }

    /** 单个用户是否在线（不关心设备数与 lastSeen 的调用方用这个，少两条命令） */
    public boolean online(String userId) {
        if (userId == null || userId.isBlank()) {
            return false;
        }
        try {
            Long size = redis.opsForSet().size(RedisKeys.gatewayUser(userId));
            return size != null && size > 0;
        } catch (RuntimeException e) {
            log.debug("[presence] 在线判定失败，按离线处理 user={} err={}", userId, e.getMessage());
            return false;
        }
    }

    /**
     * 记一次「真的下线了」。
     *
     * <p>只在下线边沿写，不在心跳里写：这个字段回答的是「他上次在线到什么时候」，
     * 若跟着心跳刷新，它就会退化成「我们最后一次看见他」，两者在排障时结论完全相反。
     */
    public void touchLastSeen(String userId, long ts) {
        if (userId == null || userId.isBlank() || ts <= 0) {
            return;
        }
        String key = RedisKeys.presenceLastSeen(userId);
        try {
            redis.opsForValue().set(key, Long.toString(ts));
            redis.expire(key, lastSeenTtl);
        } catch (RuntimeException e) {
            // lastSeen 只是展示信息：写失败不该让「下线事件已经处理完」这个结论变成失败
            log.debug("[presence] lastSeen 写入失败 user={} err={}", userId, e.getMessage());
        }
    }

    private static byte[] bytes(String key) {
        return key.getBytes(StandardCharsets.UTF_8);
    }

    private static int number(List<Object> raw, int index) {
        Object value = raw != null && index < raw.size() ? raw.get(index) : null;
        return value instanceof Number count ? count.intValue() : 0;
    }

    private static long millis(List<Object> raw, int index) {
        Object value = raw != null && index < raw.size() ? raw.get(index) : null;
        if (value == null) {
            return 0L;
        }
        try {
            // StringRedisTemplate 的 GET 结果是 String（byte[] 取决于序列化器），两种都兜住
            String text = value instanceof byte[] data
                    ? new String(data, StandardCharsets.UTF_8)
                    : value.toString();
            return Long.parseLong(text);
        } catch (NumberFormatException e) {
            return 0L;
        }
    }
}
