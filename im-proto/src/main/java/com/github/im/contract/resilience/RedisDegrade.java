package com.github.im.contract.resilience;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Redis 调用的<b>降级壳</b>：快速失败 + 短路熔断 + 统一兜底。
 *
 * <h2>要解决的两个不同问题</h2>
 *
 * <p>1. <b>挂死</b>：Lettuce 的默认命令超时是 60s 级。Redis 主机被摘掉（网络黑洞，不是 RST）时，
 * 每条命令都要等满超时，上行 gRPC 的 300ms~10s deadline 早就过了，线程/虚拟线程全堵在等 Redis 上，
 * 于是「Redis 抖动」被放大成「整个 IM 服务不可用」。这里做的是<b>短路</b>：连续失败到阈值后，
 * 在一个窗口期内<b>根本不发</b>请求，直接走兜底，让故障面止于「降级」而不是「雪崩」。
 *
 * <p>2. <b>无兜底</b>：调用方原本要么不 catch（异常冒到 gRPC 变成 INTERNAL），要么 catch 后随便给个值。
 * 这里把「兜底值」变成显式参数，强制每个调用点回答「Redis 不可用时这一项该返回什么」。
 *
 * <h2>为什么是静态工具而不是 Bean</h2>
 *
 * <p>调用点分布在 {@code im-server}、{@code im-gateway}、{@code im-rpc} 三个模块，各自的装配代码
 * 与单元测试都直接 {@code new} 这些 Store（不经过 Spring）。做成静态单例可以让每个调用点只多一层
 * 包装、不改构造签名——否则为了传一个降级器要改十几个构造函数和它们的测试。
 * 代价是状态是进程级的，测试里需要 {@link #reset()}。
 *
 * <h2>熔断粒度</h2>
 *
 * <p>按<b>操作名</b>分别统计（{@code seq.next}、{@code offline.peek}、{@code route.read}…）。
 * 不分操作的话，一个冷门操作的偶发错误会把热点路径一起短路掉；反过来全局熔断又太粗。
 *
 * <h2>不做的事</h2>
 *
 * <p>不做重试。命令超时后立刻重试只会把 Redis 打得更死，而且多数调用点（发号、幂等）重试是
 * 语义不安全的——重试的幂等性必须由业务自己保证。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class RedisDegrade {

    private static final Logger log = LoggerFactory.getLogger(RedisDegrade.class);

    /** 连续失败多少次后短路 */
    private static volatile int failThreshold = 3;
    /** 短路窗口：这段时间内不再尝试 Redis，直接走兜底 */
    private static volatile long openMillis = 2_000;

    private static final Map<String, Breaker> BREAKERS = new ConcurrentHashMap<>();
    private static final AtomicLong FALLBACK_COUNT = new AtomicLong();
    private static final AtomicLong FAILURE_COUNT = new AtomicLong();

    private RedisDegrade() {
    }

    /**
     * 执行一次 Redis 调用，失败时返回兜底值。
     *
     * @param op      操作名（熔断粒度），建议用「模块.动作」的短名
     * @param redis   真正的 Redis 调用
     * @param fallback 兜底值；<b>必须是无副作用的常量</b>（它会在熔断期内被反复调用）
     */
    public static <T> T call(String op, Supplier<T> redis, Supplier<T> fallback) {
        Breaker breaker = BREAKERS.computeIfAbsent(op, _ -> new Breaker());
        if (breaker.isOpen()) {
            FALLBACK_COUNT.incrementAndGet();
            return fallback.get();
        }
        try {
            T value = redis.get();
            breaker.onSuccess();
            return value;
        } catch (RuntimeException e) {
            recordFailure(breaker, op, e);
            FALLBACK_COUNT.incrementAndGet();
            return fallback.get();
        }
    }

    /**
     * 只关心「有没有做成」的写操作。
     *
     * @return true = Redis 侧写成功；false = 已降级（调用方通常只需要记一条日志）
     */
    public static boolean run(String op, Runnable redis) {
        Breaker breaker = BREAKERS.computeIfAbsent(op, _ -> new Breaker());
        if (breaker.isOpen()) {
            FALLBACK_COUNT.incrementAndGet();
            return false;
        }
        try {
            redis.run();
            breaker.onSuccess();
            return true;
        } catch (RuntimeException e) {
            recordFailure(breaker, op, e);
            FALLBACK_COUNT.incrementAndGet();
            return false;
        }
    }

    /** 该操作当前是否处于短路期（供需要「换个策略」而不只是换个返回值的调用点判断） */
    public static boolean isOpen(String op) {
        Breaker breaker = BREAKERS.get(op);
        return breaker != null && breaker.isOpen();
    }

    /** 立即让某个操作进入短路期（例如上游已经确认 Redis 不可用） */
    public static void trip(String op) {
        BREAKERS.computeIfAbsent(op, _ -> new Breaker()).trip();
    }

    /** 累计走过的兜底次数（指标用） */
    public static long fallbackCount() {
        return FALLBACK_COUNT.get();
    }

    /** 累计观察到的 Redis 失败次数（指标用） */
    public static long failureCount() {
        return FAILURE_COUNT.get();
    }

    /** 测试用：清空熔断状态与计数 */
    public static void reset() {
        BREAKERS.clear();
        FALLBACK_COUNT.set(0);
        FAILURE_COUNT.set(0);
    }

    /** 启动时可调（{@code im.redis.degrade.*}）；非法值保持默认，不让配置错误阻断启动 */
    public static void configure(int threshold, long openMillis) {
        if (threshold > 0) failThreshold = threshold;
        if (openMillis > 0) RedisDegrade.openMillis = openMillis;
    }

    public static int failThreshold() {
        return failThreshold;
    }

    public static long openMillis() {
        return openMillis;
    }

    private static void recordFailure(Breaker breaker, String op, RuntimeException e) {
        FAILURE_COUNT.incrementAndGet();
        // 只在「刚进入短路」时打 warn：Redis 真挂时每个请求都打日志会把磁盘写满
        if (breaker.onFailure()) {
            log.warn("[redis-degrade] {} 连续失败 {} 次，接下来 {}ms 直接走本地兜底（err={}）",
                    op, failThreshold, openMillis, e.getMessage());
        } else if (log.isDebugEnabled()) {
            log.debug("[redis-degrade] {} 失败（未达短路阈值）: {}", op, e.getMessage());
        }
    }

    /** 单个操作的短路状态：连续失败计数 + 短路截止时间 */
    private static final class Breaker {

        private final AtomicInteger consecutive = new AtomicInteger();
        private final AtomicLong openUntil = new AtomicLong();

        boolean isOpen() {
            long until = openUntil.get();
            return until > 0 && System.currentTimeMillis() < until;
        }

        void onSuccess() {
            if (consecutive.getAndSet(0) != 0) {
                openUntil.set(0L);
            }
        }

        /** @return true = 这一次刚好触发短路（调用方据此决定是否打 warn） */
        boolean onFailure() {
            int failed = consecutive.incrementAndGet();
            if (failed >= failThreshold) {
                openUntil.set(System.currentTimeMillis() + openMillis);
                consecutive.set(0);
                return true;
            }
            return false;
        }

        void trip() {
            openUntil.set(System.currentTimeMillis() + openMillis);
            consecutive.set(0);
        }
    }
}
