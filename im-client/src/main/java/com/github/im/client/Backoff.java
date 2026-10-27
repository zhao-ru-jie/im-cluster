package com.github.im.client;

import java.util.random.RandomGenerator;

/**
 * 指数退避 + <b>带下界</b>的随机抖动。
 *
 * <pre>
 * 上限 ceiling(n) = min(maxMs, baseMs * 2^n)
 * 下界 floor(n)   = ceiling(n) / 5          ← 「带下界」指的就是这一行
 * 实际延迟        = floor(n) + random(0, ceiling(n) - floor(n))
 * </pre>
 *
 * <h2>为什么不用纯 full jitter</h2>
 *
 * <p>经典 full jitter 是 {@code random(0, ceiling)}，好处是重连峰值被摊得最平，
 * 但它允许「连续抽到接近 0ms」——那等于<b>没有退避</b>。服务端正在重启时，
 * 客户端会把重连风暴打成持续的高频探测，比不加抖动更糟。
 * 保留 20% 下界，既保住了摊峰效果，也让「第 n 次重连至少等 floor(n)」成立。
 *
 * <h2>为什么要抖动</h2>
 *
 * <p>没有抖动时，一次网关重启会让所有客户端在<b>同一毫秒</b>重连（它们的心跳几乎同步、
 * 失败时刻也几乎同步）。抖动把这一批连接摊到一个时间窗里，
 * 让「连接数从 0 爬回满」变成一条斜坡而不是一根垂直的针。
 *
 * <p>本类无状态、线程安全（每次调用自己算），可被重连与待发消息重投共用。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class Backoff {

    /** 2^n 的 n 上限：{@code 1 << 20} 之后 ceiling 必然已被 maxMs 截断，再大只会溢出 */
    private static final int MAX_SHIFT = 20;
    /** 下界占上限的比例（1/5 = 20%） */
    private static final int FLOOR_DIVISOR = 5;

    private final long baseMs;
    private final long maxMs;
    private final RandomGenerator random;

    public Backoff(long baseMs, long maxMs) {
        this(baseMs, maxMs, RandomGenerator.getDefault());
    }

    /**
     * @param random 注入随机源：单测需要可重复的序列，线上用默认实现
     */
    public Backoff(long baseMs, long maxMs, RandomGenerator random) {
        if (baseMs <= 0 || maxMs <= 0) {
            throw new IllegalArgumentException("baseMs/maxMs 必须为正数：" + baseMs + "/" + maxMs);
        }
        if (baseMs > maxMs) {
            throw new IllegalArgumentException("baseMs 不能大于 maxMs：" + baseMs + " > " + maxMs);
        }
        this.baseMs = baseMs;
        this.maxMs = maxMs;
        this.random = random == null ? RandomGenerator.getDefault() : random;
    }

    /**
     * 第 {@code attempt} 次重试要等多久。
     *
     * @param attempt 0 表示第一次重试（刚断开还没重试过）
     */
    public long delayMs(int attempt) {
        long ceiling = ceilingMs(attempt);
        long floor = ceiling / FLOOR_DIVISOR;
        if (ceiling <= floor) {
            return ceiling;
        }
        // nextLong(bound) 是左闭右开，+1 让它取到天花板
        return floor + random.nextLong(ceiling - floor + 1);
    }

    /** 第 {@code attempt} 次重试的退避上限（不含抖动），用于日志/监控对账 */
    public long ceilingMs(int attempt) {
        int n = Math.clamp(attempt, 0, MAX_SHIFT);
        long ceiling = baseMs;
        // 逐步倍增而不是 base << n：后者在 n 稍大时会先溢出再被截断，结果是负延迟
        for (int i = 0; i < n && ceiling < maxMs; i++) {
            ceiling = ceiling > maxMs / 2 ? maxMs : ceiling * 2;
        }
        return Math.min(ceiling, maxMs);
    }

    public long baseMs() {
        return baseMs;
    }

    public long maxMs() {
        return maxMs;
    }
}
