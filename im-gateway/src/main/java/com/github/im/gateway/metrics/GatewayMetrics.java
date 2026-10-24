package com.github.im.gateway.metrics;

import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 长连接服务的扩容指标。
 *
 * <p>为什么必须有自定义指标：<b>空闲连接的 CPU 占用几乎为 0</b>，只用 CPU 做 HPA
 * 会在夜间把网关缩到 1 个实例，然后白天被重连风暴打穿。因此 HPA 的主指标是
 * 「每实例连接数」，CPU 只作为辅指标。
 *
 * <p>指标清单（Prometheus 命名）：
 * <ul>
 *   <li>{@code gateway_connections_active}（Gauge）—— 当前连接总数，<b>HPA 主指标</b>
 *       （不区分传输，保证原有 HPA 查询不受双通道改造影响）</li>
 *   <li>{@code gateway_connections_by_transport_active}（Gauge，tag {@code transport}）——
 *       WebSocket / TCP 各自的连接数，用于观察客户端迁移进度</li>
 *   <li>{@code gateway_sessions_opened_total} / {@code closed_total} —— 连接抖动检测</li>
 *   <li>{@code gateway_sessions_migrated_total} —— 摘流期间主动迁走的连接数</li>
 *   <li>{@code gateway_sessions_idle_closed_total} —— 心跳超时被清理的连接数</li>
 *   <li>{@code gateway_ingress_failures_total}（tag {@code op}、{@code status}）——
 *       上行 gRPC 调用失败次数，按操作（submit / pull）和 gRPC 状态码拆分。
 *       排障时它能直接区分「连不上」（{@code UNAVAILABLE}）和「建连慢/服务端慢」
 *       （{@code DEADLINE_EXCEEDED}），比翻业务日志里那一行 warn 快得多。</li>
 *   <li>{@code gateway_biz_backpressure_paused_active}（Gauge）+
 *       {@code _paused_total} / {@code _resumed_total}（Counter）——
 *       <b>上行背压</b>状态：当前有多少条连接因为处理不过来而暂停了读取，
 *       以及累计暂停/恢复次数。持续有值说明下游（Redis/DB/IM 服务）跟不上，
 *       是「该扩容还是该查下游」的第一手判断依据。</li>
 * </ul>
 *
 * <p>没有 {@link MeterRegistry}（未启用 actuator/metrics）时自动降级为空实现，不影响网关功能。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class GatewayMetrics {

    private final MeterRegistry registry;
    private final Counter opened;
    private final Counter closed;
    private final Counter migrated;
    private final Counter idleClosed;
    private final Counter bizPaused;
    private final Counter bizResumed;

    /**
     * 当前处于「暂停读取」状态的连接数。
     *
     * <p>不依赖 MeterRegistry 也存在：{@code /admin/status} 直接读它，
     * 这样即使没开 Prometheus 也能在本地看到背压有没有发生。
     */
    private final AtomicInteger bizPausedActive = new AtomicInteger();

    public GatewayMetrics(ObjectProvider<MeterRegistry> registryProvider, ConnectionManager connectionManager) {
        MeterRegistry registry = registryProvider.getIfAvailable();
        this.registry = registry;
        if (registry == null) {
            this.opened = null;
            this.closed = null;
            this.migrated = null;
            this.idleClosed = null;
            this.bizPaused = null;
            this.bizResumed = null;
            return;
        }
        Gauge.builder("gateway.connections.active", connectionManager, ConnectionManager::connectionCount)
                .description("当前网关实例承载的长连接总数（HPA 主指标，含 WebSocket 与 TCP）")
                .register(registry);
        // 按传输拆分：双通道上线后需要能看出「还有多少客户端在用 WS / 已切到 TCP」
        for (String transport : new String[]{ClientSession.TRANSPORT_WS, ClientSession.TRANSPORT_TCP}) {
            Gauge.builder("gateway.connections.by.transport.active",
                            connectionManager, cm -> cm.connectionCount(transport))
                    .description("按传输类型统计的长连接数")
                    .tag("transport", transport)
                    .register(registry);
        }
        this.opened = Counter.builder("gateway.sessions.opened").description("累计建立连接数").register(registry);
        this.closed = Counter.builder("gateway.sessions.closed").description("累计关闭连接数").register(registry);
        this.migrated = Counter.builder("gateway.sessions.migrated").description("摘流期间被主动迁移的连接数").register(registry);
        this.idleClosed = Counter.builder("gateway.sessions.idle.closed").description("心跳超时被清理的连接数").register(registry);
        Gauge.builder("gateway.biz.backpressure.paused.active", bizPausedActive, AtomicInteger::get)
                .description("当前因上行积压而暂停读取的连接数")
                .register(registry);
        this.bizPaused = Counter.builder("gateway.biz.backpressure.paused")
                .description("累计触发上行背压（暂停读取）的次数").register(registry);
        this.bizResumed = Counter.builder("gateway.biz.backpressure.resumed")
                .description("累计从上行背压恢复（恢复读取）的次数").register(registry);
    }

    /**
     * 记录一次鉴权拒绝（{@code gateway.auth.rejected}，tag {@code reason}）。
     *
     * <p>{@code warn} 档下这个计数也在涨——它正是「先观测再阻断」的观测手段：
     * 上线前先确认曲线只覆盖该被拒的那批客户端，再切 {@code enforce}。
     */
    public void authRejected(String reason) {
        if (registry == null) {
            return;
        }
        Counter.builder("gateway.auth.rejected")
                .description("接入鉴权未通过的连接数（按原因）")
                .tag("reason", reason)
                .register(registry)
                .increment();
    }

    public void sessionOpened() {
        if (opened != null) {
            opened.increment();
        }
    }

    public void sessionClosed() {
        if (closed != null) {
            closed.increment();
        }
    }

    public void sessionMigrated(int count) {
        if (migrated != null) {
            migrated.increment(count);
        }
    }

    public void sessionIdleClosed(int count) {
        if (idleClosed != null) {
            idleClosed.increment(count);
        }
    }

    /** 某条连接进入「暂停读取」（上行积压超过高水位） */
    public void bizBackpressurePaused() {
        bizPausedActive.incrementAndGet();
        if (bizPaused != null) {
            bizPaused.increment();
        }
    }

    /** 某条连接从背压恢复；用 max(0) 兜住，避免计数方法的调用顺序异常导致负数 */
    public void bizBackpressureResumed() {
        bizPausedActive.updateAndGet(active -> active > 0 ? active - 1 : 0);
        if (bizResumed != null) {
            bizResumed.increment();
        }
    }

    /** 当前暂停读取的连接数（{@code /admin/status} 直接展示） */
    public int bizBackpressurePausedActive() {
        return bizPausedActive.get();
    }

    /**
     * 记录一次上行 gRPC 调用失败。
     *
     * <p>{@code status} 取 gRPC 状态码（{@code UNAVAILABLE} / {@code DEADLINE_EXCEEDED} / …），
     * 非 gRPC 异常则取异常类名。这几个值恰好覆盖了上行失败的全部成因：
     * 连不上（subchannel 没 READY）、全部实例在重启、服务端处理超时、以及业务异常。
     */
    public void ingressFailed(String op, Throwable error) {
        if (registry == null) {
            return;
        }
        String status = error instanceof io.grpc.StatusRuntimeException sre
                ? sre.getStatus().getCode().name()
                : error.getClass().getSimpleName();
        Counter.builder("gateway.ingress.failures")
                .description("上行 gRPC 调用失败次数（按操作与 gRPC 状态码）")
                .tag("op", op)
                .tag("status", status)
                .register(registry)
                .increment();
    }
}
