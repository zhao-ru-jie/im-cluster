package com.github.im.rpc.push;

import com.github.im.contract.RedisKeys;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 批量路由查询的行为约定：<b>多端要全返回、离线要空集、大群要分片而不是发一条超大 pipeline</b>。
 *
 * <p>需要本机 Redis；不可用时自动跳过（与 {@code SeqAllocatorTest} 一致）。
 * 只操作 {@link #userIds} 里自己造的用户键，不碰其它数据。
 */
class GatewayRouteReaderTest {

    private final List<String> userIds = new ArrayList<>();
    private LettuceConnectionFactory factory;
    private StringRedisTemplate redis;

    @AfterEach
    void cleanup() {
        if (redis != null && !userIds.isEmpty()) {
            userIds.forEach(id -> redis.delete(RedisKeys.gatewayUser(id)));
        }
        if (factory != null) {
            factory.destroy();
        }
    }

    @Test
    @DisplayName("批量查询：多端返回全部实例，离线用户返回空集合")
    void returnsAllInstancesPerUserAndEmptyForOffline() {
        GatewayRouteReader reader = new GatewayRouteReader(redisOrSkip());

        String multiDevice = newUser("gateway-a", "gateway-c");
        String singleDevice = newUser("gateway-b");
        String offline = newUser();

        Map<String, Set<String>> routes = reader.instancesOfAll(List.of(multiDevice, singleDevice, offline));

        assertEquals(Set.of("gateway-a", "gateway-c"), routes.get(multiDevice), "多端用户必须返回全部实例");
        assertEquals(Set.of("gateway-b"), routes.get(singleDevice));
        assertTrue(routes.get(offline).isEmpty(), "离线用户应为空集合，而不是缺 key");
    }

    @Test
    @DisplayName("超过分片阈值时仍然正确（覆盖分片 + 并发路径）")
    void handlesMoreThanOneShard() {
        GatewayRouteReader reader = new GatewayRouteReader(redisOrSkip());

        // 600 个用户 → 500 + 100 两个分片；其中每 10 个里只有 1 个在线
        List<String> targets = new ArrayList<>();
        List<String> online = new ArrayList<>();
        for (int i = 0; i < 600; i++) {
            if (i % 10 == 0) {
                String userId = newUser("gateway-" + (i % 7));
                targets.add(userId);
                online.add(userId);
            } else {
                targets.add(newUser());
            }
        }

        Map<String, Set<String>> routes = reader.instancesOfAll(targets);

        assertEquals(600, routes.size(), "每个输入用户都应有一个结果（在线的带集合、离线的空集）");
        for (String id : online) {
            assertEquals(1, routes.get(id).size(), "在线用户应查到它的实例");
        }
        long offlineCount = routes.values().stream().filter(Set::isEmpty).count();
        assertEquals(600 - online.size(), offlineCount, "离线用户必须是空集合");
    }

    @Test
    @DisplayName("空输入不发命令，直接返回空 Map")
    void emptyInputReturnsEmptyMap() {
        GatewayRouteReader reader = new GatewayRouteReader(redisOrSkip());
        assertTrue(reader.instancesOfAll(List.of()).isEmpty());
    }

    // ==== 辅助 ====

    /** 造一个唯一用户；instances 为空表示「不写路由」= 离线 */
    private String newUser(String... instances) {
        String userId = "route-test-" + UUID.randomUUID();
        userIds.add(userId);
        for (String instance : instances) {
            if (instance != null) {
                redis().opsForSet().add(RedisKeys.gatewayUser(userId), instance);
            }
        }
        return userId;
    }

    private StringRedisTemplate redisOrSkip() {
        try {
            if (redis == null) {
                RedisStandaloneConfiguration config = new RedisStandaloneConfiguration("127.0.0.1", 6379);
                factory = new LettuceConnectionFactory(config);
                factory.afterPropertiesSet();
                redis = new StringRedisTemplate(factory);
                redis.afterPropertiesSet();
            }
            // 用一次真实命令探活（模板自己管理连接的获取与归还）
            redis.hasKey("route-reader-test-probe");
            return redis;
        } catch (RuntimeException e) {
            Assumptions.assumeTrue(false, "本机 Redis 不可用，跳过：" + e.getMessage());
            throw e;
        }
    }

    private StringRedisTemplate redis() {
        return redisOrSkip();
    }
}
