package com.example.im.connection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * 基于 Redis 的「用户 -> 节点」路由表。
 *
 * 节点 ID 不再手动指定，而是自动由「gRPC 监听地址」构成： nodeId = grpcHost:grpcPort
 * （即服务启动的 IP + 端口）。由于节点 ID 本身就是一个可达的 gRPC 地址，
 * 目标用户的路由值（im:user:node:{userId}）即可直接作为转发目标，无需额外的「节点 -> 地址」映射。
 *
 * 关键结构：
 *   im:user:node:{userId} = grpcHost:grpcPort   (带 TTL，靠心跳续期；值本身就是可直接转发的 gRPC 地址)
 */
@Component
public class UserNodeRegistry {

    private static final Logger log = LoggerFactory.getLogger(UserNodeRegistry.class);

    private static final Duration TTL = Duration.ofSeconds(30);
    private static final String USER_KEY = "im:user:node:";

    private final StringRedisTemplate redis;
    /** 节点 ID = 本节点对外可达地址（host:port），由 NodeAddressResolver 自动探测（K8s 下为 POD_IP） */
    private final String localNodeId;

    private final ConnectionManager connectionManager;

    public UserNodeRegistry(StringRedisTemplate redis,
                            ConnectionManager connectionManager,
                            NodeAddressResolver addressResolver) {
        this.redis = redis;
        this.connectionManager = connectionManager;
        this.localNodeId = addressResolver.localNodeId();
    }

    /** 心跳：续期所有本地在线用户的路由，防止节点崩溃后残留脏数据 */
    @Scheduled(fixedDelay = 10_000)
    public void heartbeat() {
        for (String userId : connectionManager.userIds()) {
            redis.opsForValue().set(USER_KEY + userId, localNodeId, TTL);
        }
    }

    public void bindUser(String userId) {
        redis.opsForValue().set(USER_KEY + userId, localNodeId, TTL);
    }

    public void unbindUser(String userId) {
        redis.delete(USER_KEY + userId);
    }

    /**
     * 清除「指向某个（已不健康 / 已下线）节点的」全部用户路由缓存。
     *
     * <p>由 {@code NacosNodeDiscovery} 的不健康监听触发：当某节点被 Nacos 标记为不健康，
     * 其它健康节点会扫描 Redis，把 {@code im:user:node:{userId}} 中值等于 {@code deadNodeId} 的
     * 条目删掉，使这些用户被视为「离线」，后续消息不再转发到死节点，而是落入离线队列。
     * 节点恢复、用户重连后会重新注册路由。
     *
     * <p>注意：demo 用 {@code keys()} 全量扫描，规模大时请改用 SCAN 游标遍历。
     */
    public void removeNodeBindings(String deadNodeId) {
        Set<String> keys = redis.keys(USER_KEY + "*");
        if (keys == null || keys.isEmpty()) {
            return;
        }
        List<String> toDelete = new ArrayList<>();
        for (String key : keys) {
            String nodeId = redis.opsForValue().get(key);
            if (deadNodeId.equals(nodeId)) {
                toDelete.add(key);
            }
        }
        if (!toDelete.isEmpty()) {
            redis.delete(toDelete);
            log.info("removed {} stale routing entries for dead node {}", toDelete.size(), deadNodeId);
        }
    }

    /**
     * 查某用户当前连在哪个节点上；返回值即为该节点的 gRPC 地址（host:port），null = 不在线。
     */
    public String nodeOf(String userId) {
        return redis.opsForValue().get(USER_KEY + userId);
    }

    /** 本节点 ID（= 本节点 gRPC 可达地址 host:port） */
    public String localNodeId() {
        return localNodeId;
    }
}
