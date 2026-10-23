package com.github.im.gateway.metrics;

import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

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

    public GatewayMetrics(ObjectProvider<MeterRegistry> registryProvider, ConnectionManager connectionManager) {
        MeterRegistry registry = registryProvider.getIfAvailable();
        this.registry = registry;
        if (registry == null) {
            this.opened = null;
            this.closed = null;
            this.migrated = null;
            this.idleClosed = null;
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
