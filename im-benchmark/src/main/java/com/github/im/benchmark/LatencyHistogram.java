package com.github.im.benchmark;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * 定宽分桶延迟直方图：多写多读、无锁。
 *
 * <p>压测端的统计不能拖慢被测链路，所以热路径上只做「一次除法 + 一次原子自增」，
 * 不用 {@code synchronized}、不保存原始样本（保存样本会在百万级 QPS 下把压测机自己先撑爆）。
 *
 * <p>桶宽 10µs、共 20 万个桶，覆盖 0~2s。选 10µs 是因为本机/同机架 RTT 常在亚毫秒级，
 * 用毫秒桶会把几乎所有样本挤进 0 号桶，P50/P95 直接失去分辨力。
 * 超过 2s 的样本落到最后一桶（这些通常是超时，P99 会被压到 2s 以下，但 max 与 avg 仍精确）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class LatencyHistogram {

    private static final long BUCKET_NANOS = 10_000L;
    private static final int BUCKETS = 200_000;

    private final AtomicLongArray buckets = new AtomicLongArray(BUCKETS);
    private final AtomicLong count = new AtomicLong();
    private final AtomicLong totalNanos = new AtomicLong();
    private final AtomicLong maxNanos = new AtomicLong();

    public void record(long nanos) {
        if (nanos < 0) {
            return;
        }
        count.incrementAndGet();
        totalNanos.addAndGet(nanos);
        maxNanos.accumulateAndGet(nanos, Math::max);
        int bucket = (int) Math.min(nanos / BUCKET_NANOS, BUCKETS - 1);
        buckets.incrementAndGet(bucket);
    }

    public long count() {
        return count.get();
    }

    public Snapshot snapshot() {
        long total = count.get();
        if (total == 0) {
            return Snapshot.EMPTY;
        }
        long[] targets = {
                upper(total, 500), upper(total, 950), upper(total, 990), upper(total, 999)
        };
        double[] percentiles = new double[targets.length];
        int targetIndex = 0;
        long cumulative = 0;
        for (int bucket = 0; bucket < BUCKETS && targetIndex < targets.length; bucket++) {
            long inBucket = buckets.get(bucket);
            if (inBucket == 0) {
                continue;
            }
            cumulative += inBucket;
            while (targetIndex < targets.length && cumulative >= targets[targetIndex]) {
                // 取桶上界：报「P50=0」比报得略保守更难误导人
                percentiles[targetIndex++] = (bucket + 1) * BUCKET_NANOS / 1_000.0;
            }
        }
        return new Snapshot(
                total,
                totalNanos.get() / (double) total / 1_000.0,
                percentiles[0],
                percentiles[1],
                percentiles[2],
                percentiles[3],
                maxNanos.get() / 1_000.0);
    }

    private static long upper(long total, int perMille) {
        return Math.max(1L, (total * perMille + 999L) / 1000L);
    }

    /** 延迟快照，单位均为微秒（对外格式化时才换成毫秒）。 */
    public record Snapshot(
            long count,
            double avgMicros,
            double p50Micros,
            double p95Micros,
            double p99Micros,
            double p999Micros,
            double maxMicros) {

        public static final Snapshot EMPTY = new Snapshot(0, 0, 0, 0, 0, 0, 0);

        public String percentileLine() {
            return String.format("p50=%.2fms p95=%.2fms p99=%.2fms p99.9=%.2fms max=%.2fms avg=%.3fms n=%,d",
                    p50Micros / 1000, p95Micros / 1000, p99Micros / 1000,
                    p999Micros / 1000, maxMicros / 1000, avgMicros / 1000, count);
        }

        public String shortLine() {
            return String.format("%.2f/%.2f/%.2f ms",
                    p50Micros / 1000, p95Micros / 1000, p99Micros / 1000);
        }
    }
}
