package com.github.im.server.config;

import com.github.im.contract.resilience.RedisDegrade;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 把 {@link RedisDegrade} 的熔断参数接到配置上。
 *
 * <p>Redis 的「健康」没有统一口径：同一个集群，小作业可能要求「宁可报错也别降级」，
 * 探活期则希望「尽量别把请求打到已经挂了的实例上」。所以阈值和窗口都给到配置：
 *
 * <pre>
 * im:
 *   redis:
 *     degrade:
 *       fail-threshold: 3      # 连续失败多少次后短路
 *       open-millis: 2000      # 短路窗口；窗口结束后放一个请求去试探，成功即恢复
 * </pre>
 *
 * <p>{@code fail-threshold} 不能设太小：Redis 偶发的单次超时如果立刻短路，
 * 热点路径会频繁进出降级；也不能太大，否则每次故障都要先失败 N 次才开始短路。
 */
@Component
public class RedisDegradeConfig {

    private static final Logger log = LoggerFactory.getLogger(RedisDegradeConfig.class);

    private final int failThreshold;
    private final long openMillis;

    public RedisDegradeConfig(@Value("${im.redis.degrade.fail-threshold:3}") int failThreshold,
                              @Value("${im.redis.degrade.open-millis:2000}") long openMillis) {
        this.failThreshold = failThreshold;
        this.openMillis = openMillis;
    }

    @PostConstruct
    void apply() {
        RedisDegrade.configure(failThreshold, openMillis);
        log.info("[redis-degrade] 熔断参数：连续失败 {} 次后短路 {}ms", RedisDegrade.failThreshold(), RedisDegrade.openMillis());
    }
}
