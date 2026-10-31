package com.github.im.rpc.push;

import com.github.im.contract.resilience.RedisDegrade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.SetOperations;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link GatewayRouteReader} 在 Redis 不可用时的路由降级。
 *
 * <p>要防的是一条很具体的雪崩链：路由查不到 -> 目标判成离线 -> 消息全量涌进离线队列 ->
 * 而离线队列同样在 Redis 上。所以这里缓存最近成功的路由，Redis 不可用时拿它顶上
 * （最坏是一次推送到已断开的实例，那次失败才走离线）。
 */
class GatewayRouteReaderDegradeTest {

    @BeforeEach
    void setUp() {
        RedisDegrade.reset();
        RedisDegrade.configure(1, 200);
    }

    @AfterEach
    void tearDown() {
        RedisDegrade.reset();
        RedisDegrade.configure(3, 2_000);
    }

    @Test
    @DisplayName("Redis 挂掉后用最近一次查到的路由，而不是把所有人判成离线")
    void staleRouteIsUsedWhenRedisFails() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        SetOperations<String, String> sets = mock(SetOperations.class);
        when(redis.opsForSet()).thenReturn(sets);
        when(sets.members(anyString())).thenReturn(Set.of("gw-1:7001"));

        GatewayRouteReader reader = new GatewayRouteReader(redis);
        assertEquals(Set.of("gw-1:7001"), reader.instancesOf("alice"));

        // Redis 开始失败（同一个实例上通常表现为连接失败异常）
        when(redis.opsForSet()).thenThrow(new RedisConnectionFailureException("redis down"));

        assertEquals(Set.of("gw-1:7001"), reader.instancesOf("alice"),
                "降级期应当继续用最近一次的路由");
    }

    @Test
    @DisplayName("从没成功查到过路由时降级为空集合（等价「不在线」，由离线队列兜底）")
    void noCacheMeansOffline() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.opsForSet()).thenThrow(new RedisConnectionFailureException("redis down"));

        GatewayRouteReader reader = new GatewayRouteReader(redis);

        assertTrue(reader.instancesOf("bob").isEmpty());
    }

    @Test
    @DisplayName("批量查询失败时也走缓存，不再逐条重放 N 次失败")
    void queryShardFallsBackToCache() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        SetOperations<String, String> sets = mock(SetOperations.class);
        when(redis.opsForSet()).thenReturn(sets);
        when(sets.members(anyString())).thenReturn(Set.of("gw-2:7001"));

        GatewayRouteReader reader = new GatewayRouteReader(redis);
        // 先让缓存里有数据
        assertEquals(Set.of("gw-2:7001"), reader.instancesOf("carol"));

        when(redis.opsForSet()).thenThrow(new RedisConnectionFailureException("redis down"));
        when(redis.executePipelined(any(RedisCallback.class)))
                .thenThrow(new RedisConnectionFailureException("redis down"));

        var result = reader.instancesOfAll(List.of("carol"));
        assertEquals(Set.of("gw-2:7001"), result.get("carol"));
    }
}
