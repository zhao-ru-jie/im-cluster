package com.github.im.client;

import java.util.random.RandomGenerator;

/**
 * 重连策略：连续失败用指数退避，被服务端摘流（{@code session.migrate}）用服务端给的提示。
 *
 * <h2>为什么「稳定连接」要单独判</h2>
 *
 * <p>只按下线次数累加会踩两个坑：
 * <ul>
 *   <li><b>永远退不出来</b>：网络时好时坏时，每次连上几秒就断，attempt 一直累加，
 *       退避涨到上限之后，明明网络已经恢复，客户端却还要等 30s；</li>
 *   <li><b>被「连上即被踢」拖成雪崩</b>：同一 deviceId 有另一台设备在抢连接、
 *       或 token 即将过期时，重连会立刻成功又立刻断开，这种「秒断」必须继续退避。</li>
 * </ul>
 * 所以用「这次连接活了多久」区分两者：活过 {@code stableAfterMs} 说明链路是真的通了，
 * 计数归零；否则继续累加。判据是<b>连接时长</b>而不是「连上了就算成功」。
 *
 * <h2>为什么 migrate 不复用退避</h2>
 *
 * <p>摘流/缩容是<b>服务端主动</b>安排的迁移，{@code retryAfterMs} 就是它给出的
 * 「另一个实例已经准备好接你」的时间。此时按自己的退避表退（可能 30s）纯属自伤；
 * 但也不能直接用 retryAfterMs——几千个连接会在同一毫秒涌回来，所以
 * <b>必须以客户端自己的抖动收尾</b>（服务端无法知道每个客户端的随机源）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class ReconnectPolicy {

    public static final long DEFAULT_BASE_MS = 500;
    public static final long DEFAULT_MAX_MS = 30_000;
    /** 连接活过这么久，就认为「链路是通的」，下一次断线从第 0 次退避重来 */
    public static final long DEFAULT_STABLE_AFTER_MS = 30_000;

    private final Backoff backoff;
    private final long stableAfterMs;
    private final RandomGenerator random;

    public ReconnectPolicy() {
        this(DEFAULT_BASE_MS, DEFAULT_MAX_MS, DEFAULT_STABLE_AFTER_MS);
    }

    public ReconnectPolicy(long baseMs, long maxMs, long stableAfterMs) {
        this(baseMs, maxMs, stableAfterMs, RandomGenerator.getDefault());
    }

    public ReconnectPolicy(long baseMs, long maxMs, long stableAfterMs, RandomGenerator random) {
        this.backoff = new Backoff(baseMs, maxMs, random);
        this.stableAfterMs = stableAfterMs;
        this.random = random == null ? RandomGenerator.getDefault() : random;
    }

    /** 第 {@code attempt} 次失败后等多久重连 */
    public long delayMs(int attempt) {
        return backoff.delayMs(attempt);
    }

    /**
     * 被摘流之后等多久重连：{@code retryAfterMs} 打底，再加 {@code [0, min(max, retryAfter/2)]} 抖动。
     *
     * @param retryAfterMs 服务端 {@code session.migrate} 里给的建议等待（≤0 视为没给）
     */
    public long delayAfterMigrate(long retryAfterMs) {
        if (retryAfterMs <= 0) {
            return backoff.delayMs(0);
        }
        long jitterCeiling = Math.min(backoff.maxMs(), Math.max(1, retryAfterMs / 2));
        return retryAfterMs + random.nextLong(jitterCeiling + 1);
    }

    /** 这条连接活够久了吗（久到可以认为「不是秒断」） */
    public boolean isStable(long connectedDurationMs) {
        return connectedDurationMs >= stableAfterMs;
    }

    public long stableAfterMs() {
        return stableAfterMs;
    }

    public Backoff backoff() {
        return backoff;
    }
}
