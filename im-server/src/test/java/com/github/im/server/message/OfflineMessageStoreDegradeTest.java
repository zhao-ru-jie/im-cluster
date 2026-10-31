package com.github.im.server.message;

import com.github.im.contract.resilience.RedisDegrade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link OfflineMessageStore} 在 <b>Redis 不可用</b>时的降级行为。
 *
 * <p>离线队列是 Redis 上最「不能丢」的东西：它承载的正好是「用户不在线」这段窗口。
 * 所以降级目标不是「报个错就算降级」，而是：
 *
 * <ol>
 *   <li>帧写不进 Redis 时进入本进程的降级缓冲，<b>不能静默丢</b>；</li>
 *   <li>{@link OfflineMessageStore#peek} 仍然把这些帧发出去（用户此刻重连就有内容）；</li>
 *   <li>缓冲区有界：降级期不能把堆撑爆。</li>
 * </ol>
 */
class OfflineMessageStoreDegradeTest {

    private static final Duration TTL = Duration.ofHours(1);
    private static final String USER = "u-degraded";
    private static final String DEVICE = "web";

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
    @DisplayName("Redis 写不进去时帧进降级缓冲，peek 仍然能发出来（不丢、不乱序）")
    void framesSurviveRedisOutageInLocalBuffer() {
        OfflineMessageStore store = store(50);

        store.store(USER, "frame-1");
        store.store(USER, "frame-2");

        assertEquals(2, store.localBufferedTotal(), "写不进去的帧必须留在本地缓冲里");

        OfflineMessageStore.FrameBatch batch = store.peek(USER, DEVICE, 10);

        assertEquals(List.of("frame-1", "frame-2"), batch.frames(), "降级缓冲要按入队顺序下发");
        assertEquals(0L, batch.ackSeq(), "本地帧没有 Redis score，不该伪造确认位点");
    }

    @Test
    @DisplayName("pendingFrames 把降级缓冲算进去：否则界面显示「0 条待补」但 peek 里还有内容")
    void pendingFramesIncludesLocalBuffer() {
        OfflineMessageStore store = store(50);
        store.store(USER, "frame-1");

        assertTrue(store.pendingFrames(USER, DEVICE) >= 1L);
    }

    @Test
    @DisplayName("降级缓冲有界：单用户超过上限丢最老的，不会无限涨")
    void localBufferIsBoundedPerUser() {
        OfflineMessageStore store = store(50);

        for (int i = 0; i < 150; i++) {
            store.store(USER, "frame-" + i);
        }

        assertEquals(100, store.localBufferedTotal(), "单用户上限是 100 条");

        OfflineMessageStore.FrameBatch batch = store.peek(USER, DEVICE, 200);
        assertEquals("frame-50", batch.frames().get(0), "最老的 50 条应当被挤掉");
        assertEquals("frame-149", batch.frames().get(batch.frames().size() - 1));
    }

    private static OfflineMessageStore store(int maxFrames) {
        return new OfflineMessageStore(deadRedis(), TTL, maxFrames, TTL);
    }

    @SuppressWarnings("unchecked")
    private static StringRedisTemplate deadRedis() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.execute(any(RedisCallback.class)))
                .thenThrow(new RedisConnectionFailureException("redis down"));
        when(redis.opsForValue()).thenThrow(new RedisConnectionFailureException("redis down"));
        when(redis.opsForList()).thenThrow(new RedisConnectionFailureException("redis down"));
        when(redis.opsForHash()).thenThrow(new RedisConnectionFailureException("redis down"));
        when(redis.opsForSet()).thenThrow(new RedisConnectionFailureException("redis down"));
        when(redis.opsForZSet()).thenThrow(new RedisConnectionFailureException("redis down"));
        return redis;
    }
}
