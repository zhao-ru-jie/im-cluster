package com.github.im.server.resilience;

import com.github.im.contract.resilience.RedisDegrade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link RedisDegrade} 的行为测试：不连 Redis，纯粹验证「失败 -> 兜底 -> 短路 -> 恢复」这套状态机。
 *
 * <p>熔断状态是进程级静态的，所以每个用例后都要 {@link RedisDegrade#reset()}：
 * 否则一个用例打开熔断，会让 JVM 里跑在后面的其它用例（包括真实 Redis 的集成测试）莫名其妙地走兜底。
 */
class RedisDegradeTest {

    @BeforeEach
    void setUp() {
        RedisDegrade.reset();
        RedisDegrade.configure(3, 2_000);
    }

    @AfterEach
    void tearDown() {
        RedisDegrade.reset();
        RedisDegrade.configure(3, 2_000);
    }

    @Test
    @DisplayName("Redis 抛异常时返回兜底值，而不是把异常抛给调用方")
    void failureReturnsFallback() {
        String value = RedisDegrade.call("t.fail", () -> {
            throw new IllegalStateException("redis down");
        }, () -> "fallback");

        assertEquals("fallback", value);
        assertEquals(1L, RedisDegrade.failureCount());
        assertEquals(1L, RedisDegrade.fallbackCount());
    }

    @Test
    @DisplayName("连续失败达到阈值后短路：窗口期内根本不再发请求（这是「不雪崩」的关键）")
    void circuitOpensAfterThresholdAndSkipsRedis() {
        RedisDegrade.configure(2, 5_000);
        AtomicInteger attempts = new AtomicInteger();

        for (int i = 0; i < 5; i++) {
            RedisDegrade.call("t.short", () -> {
                attempts.incrementAndGet();
                throw new IllegalStateException("redis down");
            }, () -> "fallback");
        }

        assertTrue(RedisDegrade.isOpen("t.short"), "连续失败 2 次后应当进入短路");
        assertEquals(2, attempts.get(), "短路期内不应该再真的去碰 Redis");
    }

    @Test
    @DisplayName("成功一次就把失败计数清零：偶发抖动不该把热点路径短路掉")
    void successResetsFailureCount() {
        RedisDegrade.configure(2, 5_000);
        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger alive = new AtomicInteger(1);

        for (int i = 0; i < 4; i++) {
            RedisDegrade.call("t.blip", () -> {
                attempts.incrementAndGet();
                if (alive.get() == 1) {
                    throw new IllegalStateException("blip");
                }
                return "ok";
            }, () -> "fallback");
            alive.set(0);
        }

        assertFalse(RedisDegrade.isOpen("t.blip"), "中间成功过就不该短路");
        assertEquals(4, attempts.get(), "每次都应该继续尝试 Redis");
    }

    @Test
    @DisplayName("同一个操作短路不影响别的操作（熔断按操作名分粒度）")
    void breakerIsPerOperation() {
        RedisDegrade.configure(1, 5_000);
        RedisDegrade.call("t.opA", () -> {
            throw new IllegalStateException("down");
        }, () -> null);

        assertTrue(RedisDegrade.isOpen("t.opA"));
        assertFalse(RedisDegrade.isOpen("t.opB"), "别的操作不能被连坐");
        assertEquals("ok", RedisDegrade.call("t.opB", () -> "ok", () -> "fallback"));
    }

    @Test
    @DisplayName("run() 用返回值表达「有没有做成」，写路径降级时调用方据此决定要不要本地兜底")
    void runReportsDegrade() {
        assertTrue(RedisDegrade.run("t.write", () -> {
        }));
        assertFalse(RedisDegrade.run("t.write", () -> {
            throw new IllegalStateException("down");
        }));
    }
}
