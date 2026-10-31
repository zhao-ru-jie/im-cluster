package com.github.im.server.metrics;

import com.github.im.contract.resilience.RedisDegrade;
import io.micrometer.core.instrument.FunctionCounter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 把 {@link RedisDegrade} 的累计计数接到 Micrometer 上。
 *
 * <p>为什么用 {@link FunctionCounter} 而不是 Gauge：这两个值只增不减，是计数器语义。
 * 用 Gauge 的话 Prometheus 侧拿到的是「当前值」，{@code rate()} 会算错；
 * FunctionCounter 在 Prometheus 上会导出成 {@code im_redis_degrade_fallback_total}。
 *
 * <p>为什么必须导出：Redis 健康检查被有意关掉了（Redis 挂不该让实例被判 DOWN 而被摘掉），
 * 那么「现在是降级还是正常」就只能靠指标和 {@code [redis-degrade]} 日志判断。
 * 没有这个指标，关掉健康检查就等于蒙上眼睛。
 */
@Component
public class RedisDegradeMetrics {

    public RedisDegradeMetrics(@Autowired(required = false) MeterRegistry registry) {
        if (registry == null) {
            // 单测/无 actuator 环境：不注册任何东西
            return;
        }
        // FunctionCounter.builder 要求「对象 + 取值函数」，这里让对象只是个占位（值是静态累计量）
        FunctionCounter.builder("im.redis.degrade.fallback", RedisDegrade.class, ignored -> RedisDegrade.fallbackCount())
                .description("Redis 调用走了本地兜底的累计次数（持续增长 = 正在降级）")
                .register(registry);
        FunctionCounter.builder("im.redis.degrade.failure", RedisDegrade.class, ignored -> RedisDegrade.failureCount())
                .description("观察到的 Redis 调用失败累计次数（含未触发熔断的偶发失败）")
                .register(registry);
    }
}
