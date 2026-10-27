package com.github.im.server.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * {@link MessageMetrics} 的埋点约定：<b>名字/tag 稳定</b>、<b>registry 可空</b>、<b>meter 复用</b>。
 *
 * <p>这些断言看着琐碎，但每一条都对应一个线上后果：
 * <ul>
 *   <li>名字或 tag key 改名 → Grafana 面板与告警规则静默失效（不报错，只是没数据）；</li>
 *   <li>把条数/用户 ID 拼进 tag → 时间序列数随流量增长，最终把 Prometheus 打挂；</li>
 *   <li>registry 为空时没退化 → 没有 actuator 的部署形态（单测/嵌入式）直接启动失败。</li>
 * </ul>
 */
class MessageMetricsTest {

    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final MessageMetrics metrics = new MessageMetrics(registry);

    @Test
    @DisplayName("registry 为空时全部退化为空操作，且不抛异常")
    void nullRegistryIsNoOp() {
        MessageMetrics noop = new MessageMetrics(null);

        noop.submit(MessageMetrics.OUTCOME_ACCEPTED, 1_000_000);
        noop.phase(MessageMetrics.PHASE_SEQ, 1_000_000);
        noop.delivery(1_000_000, 3, 2);
        noop.offlineAppended(5, 2, 7);
        noop.offlineAppendFailed(4);
        noop.offlinePulled(6);
        noop.ingress("submitMessage", 1_000_000, MessageMetrics.RPC_OK);

        assertNull(noop.registry());
        assertEquals(0L, noop.offlineDepth(), "没有 registry 时队列深度恒为 0");
    }

    @Test
    @DisplayName("受理结果按 outcome 计数，耗时进同一个 submit timer")
    void submitCountsOutcomeAndDuration() {
        metrics.submit(MessageMetrics.OUTCOME_ACCEPTED, 2_000_000);
        metrics.submit(MessageMetrics.OUTCOME_ACCEPTED, 4_000_000);
        metrics.submit(MessageMetrics.OUTCOME_DUPLICATE, 1_000_000);

        assertEquals(2.0, counter("im.message.submit", "outcome", MessageMetrics.OUTCOME_ACCEPTED), 1e-9);
        assertEquals(1.0, counter("im.message.submit", "outcome", MessageMetrics.OUTCOME_DUPLICATE), 1e-9);
        // 三种不同的 outcome 共用同一个 timer：总量与结果分布分开看
        assertEquals(3L, registry.get("im.message.submit.duration").timer().count());
    }

    @Test
    @DisplayName("分段耗时按 phase 分桶，用来定位瓶颈在哪一段")
    void phaseTimersAreTaggedByPhase() {
        metrics.phase(MessageMetrics.PHASE_SEQ, 1_000_000);
        metrics.phase(MessageMetrics.PHASE_PERSIST, 5_000_000);
        metrics.phase(MessageMetrics.PHASE_PERSIST, 7_000_000);

        assertEquals(1L, timer("im.message.phase.duration", "phase", MessageMetrics.PHASE_SEQ));
        assertEquals(2L, timer("im.message.phase.duration", "phase", MessageMetrics.PHASE_PERSIST));
        assertEquals(0L, timer("im.message.phase.duration", "phase", MessageMetrics.PHASE_DISPATCH));
    }

    @Test
    @DisplayName("投递：耗时只记一次，送达/离线人数按人累加")
    void deliveryRecordsDurationAndRecipientCounts() {
        metrics.delivery(3_000_000, 5, 2);

        assertEquals(1L, registry.get("im.message.delivery.duration").timer().count());
        assertEquals(5.0, counter("im.message.recipients", "result", "delivered"), 1e-9);
        assertEquals(2.0, counter("im.message.recipients", "result", "offline"), 1e-9);
    }

    @Test
    @DisplayName("投递全是 0 人时不产生 0 值样本，只记耗时")
    void deliveryWithoutRecipientsOnlyRecordsDuration() {
        metrics.delivery(1_000_000, 0, 0);

        assertEquals(1L, registry.get("im.message.delivery.duration").timer().count());
        assertEquals(0.0, counter("im.message.recipients", "result", "delivered"), 1e-9);
        assertEquals(0.0, counter("im.message.recipients", "result", "offline"), 1e-9);
    }

    @Test
    @DisplayName("离线队列：入队/裁剪计数 + 最近一次观测到的深度 Gauge")
    void offlineAppendedUpdatesCountersAndDepthGauge() {
        metrics.offlineAppended(5, 2, 7);

        assertEquals(5.0, registry.get("im.offline.frames.appended").counter().count(), 1e-9);
        assertEquals(2.0, registry.get("im.offline.frames.trimmed").counter().count(), 1e-9);
        assertEquals(7.0, registry.get("im.offline.queue.depth").gauge().value(), 1e-9);
        assertEquals(7L, metrics.offlineDepth());
    }

    @Test
    @DisplayName("深度是写入时的近似值：没有新写入就不更新，0 也不覆盖旧值")
    void offlineDepthIsOnlySampledOnWrite() {
        metrics.offlineAppended(1, 0, 9);
        metrics.offlineAppended(1, 0, 0);

        assertEquals(9.0, registry.get("im.offline.queue.depth").gauge().value(), 1e-9,
                "depth=0 表示「这次采样没拿到」，不是「队列空了」");
        assertEquals(0.0, registry.get("im.offline.frames.trimmed").counter().count(), 1e-9);
    }

    @Test
    @DisplayName("落离线失败与补发帧数：0 不算一次样本")
    void offlineFailuresAndPullsIgnoreZero() {
        metrics.offlineAppendFailed(0);
        metrics.offlinePulled(0);
        assertEquals(0.0, registry.get("im.offline.append.failed").counter().count(), 1e-9);
        assertEquals(0.0, registry.get("im.offline.frames.pulled").counter().count(), 1e-9);

        metrics.offlineAppendFailed(4);
        metrics.offlinePulled(6);
        assertEquals(4.0, registry.get("im.offline.append.failed").counter().count(), 1e-9);
        assertEquals(6.0, registry.get("im.offline.frames.pulled").counter().count(), 1e-9);
    }

    @Test
    @DisplayName("上行 RPC：op 是固定方法名，同一 op 复用同一个 meter")
    void ingressReusesMeterPerOpAndResult() {
        metrics.ingress("batchSubmit", 1_000_000, MessageMetrics.RPC_OK);
        metrics.ingress("batchSubmit", 2_000_000, MessageMetrics.RPC_OK);
        metrics.ingress("batchSubmit", 3_000_000, MessageMetrics.RPC_INVALID);

        assertEquals(2L, timer("im.ingress.rpc.duration",
                "op", "batchSubmit", "result", MessageMetrics.RPC_OK));
        assertEquals(1L, timer("im.ingress.rpc.duration",
                "op", "batchSubmit", "result", MessageMetrics.RPC_INVALID));
        assertEquals(0L, timer("im.ingress.rpc.duration",
                "op", "batchSubmit", "result", MessageMetrics.RPC_ERROR));
        // 三次调用只应有 2 个 meter：tag 组合数由「方法 × 结果」决定，与请求内容无关
        assertEquals(2, registry.find("im.ingress.rpc.duration").timers().size());
    }

    private double counter(String name, String tagKey, String tagValue) {
        Counter counter = registry.find(name).tag(tagKey, tagValue).counter();
        return counter == null ? 0.0 : counter.count();
    }

    private long timer(String name, String... tags) {
        Timer timer = registry.find(name).tags(tags).timer();
        return timer == null ? 0L : timer.count();
    }
}
