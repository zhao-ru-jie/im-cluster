package com.github.im.rpc.push;

import com.github.im.contract.RedisKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.nio.charset.StandardCharsets;
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
 * 在线路由表的<b>只读</b>视图：{@code im:gateway:user:{userId}} → 网关实例集合。
 *
 * <p>写入权只属于长连接服务（{@code GatewayRegistry}）。IM 服务通过本类读取，
 * 用来把「推给 userId」翻译成「推到哪些网关实例」。这条约束保证路由表只有一个写者，
 * 不会出现两侧互相覆盖导致的路由抖动。
 *
 * <p>多端语义：同一用户可能同时连在多个网关实例上（手机在 A、网页在 C），
 * 所以这里返回的是<b>集合</b>，调用方必须每个实例都推一次；只取一个会让部分设备收不到实时消息。
 *
 * <p><b>为什么有 {@link #instancesOfAll} 这个批量入口</b>：群聊扇出要按人查路由，
 * 5000 人群逐个查就是 5000 次串行往返（1~2.5s），是群消息延迟的全部来源。
 * 批量入口把它压成「分片 pipeline」：每批 500 个 key 一次往返并发出去，总体 10ms 量级。
 * 单单一个用户的下行仍然走 {@link #instancesOf}，不值得为它多一层包装。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class GatewayRouteReader {

    private static final Logger log = LoggerFactory.getLogger(GatewayRouteReader.class);

    /**
     * 每个 pipeline 分片装多少个 key。
     *
     * <p>Redis 是单线程：一次塞进 5000 条 SMEMBERS 会把这 5000 条连续执行完（25~75ms），
     * 期间其它客户端的命令都得排队。切成 500 一片并发发出去，单批只占 Redis 5~8ms，
     * 既拿到 pipeline 的「一次往返」收益，又不至于让路由查询把库顶住。
     */
    private static final int SHARD_SIZE = 500;

    private final StringRedisTemplate redis;
    /** 分片并发用虚拟线程：纯阻塞等 Redis，数量受「人数 / SHARD_SIZE」约束，不会膨胀 */
    private final ExecutorService shards = Executors.newVirtualThreadPerTaskExecutor();

    public GatewayRouteReader(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * @return 该用户当前所在的网关实例集合；空集合 = 不在线（或路由已 TTL 过期）
     */
    public Set<String> instancesOf(String userId) {
        String key = RedisKeys.gatewayUser(userId);
        try {
            Set<String> members = redis.opsForSet().members(key);
            return members == null ? Set.of() : members;
        } catch (RuntimeException e) {
            // 滚动升级期间可能遇到旧版写入的 String 类型 key：按离线处理，
            // 由 IM 服务的离线队列兜底；网关会很快用 SET 重写这个 key（TTL 30s）
            return Set.of();
        }
    }

    /**
     * 批量查路由：{@code userId → 网关实例集合}，不在线的用户对应空集合。
     *
     * <p>群聊扇出必须用这个入口。除了省往返，它还顺带把「不在线」的人筛出来了——
     * 5000 人群常常只有几百人在线，调用方据此只为在线成员构造 gRPC 分组。
     *
     * @param userIds 目标用户（内部会去重）
     */
    public Map<String, Set<String>> instancesOfAll(Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return Map.of();
        }
        // 去重同时保持稳定顺序：便于日志对照，也避免重复 key 白占 pipeline 位置
        List<String> ids = List.copyOf(new LinkedHashSet<>(userIds));
        Map<String, Set<String>> routes = new ConcurrentHashMap<>(Math.max(16, ids.size() * 2));
        List<List<String>> partitions = partition(ids);
        if (partitions.size() == 1) {
            queryShard(partitions.getFirst(), routes);
            return routes;
        }
        List<Future<?>> futures = new ArrayList<>(partitions.size());
        for (List<String> shard : partitions) {
            futures.add(shards.submit(() -> queryShard(shard, routes)));
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

    /** 一个分片：一次 pipeline 发出全部 SMEMBERS，结果按命令顺序回填 */
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
            // 两种情况会走到这里：Redis Cluster 下跨 slot（CROSSSLOT），或旧 String 类型 key。
            // 逐条退化的代价是慢，但绝不能让整条群消息因为路由查询失败而全部落离线。
            log.debug("[route] 批量路由不可用（{}），退化为逐条查询", e.getMessage());
            for (String id : ids) {
                out.put(id, instancesOf(id));
            }
        }
    }

    /** pipeline 结果可能是 {@code Set<String>}，也可能是原始 {@code Set<byte[]>}，两种都认 */
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

    private static List<List<String>> partition(List<String> ids) {
        List<List<String>> partitions = new ArrayList<>((ids.size() + SHARD_SIZE - 1) / SHARD_SIZE);
        for (int from = 0; from < ids.size(); from += SHARD_SIZE) {
            partitions.add(ids.subList(from, Math.min(from + SHARD_SIZE, ids.size())));
        }
        return partitions;
    }
}
