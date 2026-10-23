package com.github.im.gateway.drain;

import com.github.im.gateway.metrics.GatewayMetrics;
import com.github.im.gateway.node.GatewayNacosDiscovery;
import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import com.github.im.gateway.session.GatewayRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 网关优雅摘流（缩容 / 发版 / 迁移时的关键动作）。
 *
 * <p>为什么不能直接杀进程：连接是<b>绑在进程上</b>的本机状态，进程一挂这些连接全部瞬断，
 * 客户端会同时发起重连，形成「重连风暴」，比慢一点摘流糟糕得多。
 *
 * <p>摘流顺序（四步，缺一不可）：
 * <ol>
 *   <li><b>摘除注册</b>：从 Nacos 注销本实例，LB 不再把新连接导进来；</li>
 *   <li><b>拒绝新连接</b>：握手阶段直接返回 503，避免「摘一半又进来一批」；</li>
 *   <li><b>通知客户端迁移</b>：给所有存量连接下发 {@code session.migrate}（带重连建议延迟，
 *       客户端再加抖动），让客户端主动重连到其它健康实例；</li>
 *   <li><b>等待 drain 到超时</b>：给客户端留出重连窗口，窗口结束仍未走的连接强制关闭。</li>
 * </ol>
 *
 * <p>触发方式：
 * <ul>
 *   <li>进程收到 SIGTERM/ContextClosed（K8s 默认终止流程）；</li>
 *   <li>显式 {@code POST /admin/drain}（K8s preStop hook 用，也方便本地验证）。</li>
 * </ul>
 *
 * <p><b>注意</b>：显式 drain 不会让进程退出，只是把连接迁走；退出仍由平台负责
 * （这样 preStop 之后 pod 还能继续把迁移计数上报给监控）。
 */
@Component
public class GatewayDrainService {

    private static final Logger log = LoggerFactory.getLogger(GatewayDrainService.class);

    private final ConnectionManager connectionManager;
    private final GatewayRegistryAccessor registryAccessor;
    private final ClientFrames frames;
    private final GatewayMetrics metrics;
    private final ApplicationEventPublisher events;

    private final boolean enabled;
    private final long timeoutMs;
    private final long retryAfterMs;
    private final AtomicBoolean draining = new AtomicBoolean(false);

    public GatewayDrainService(ConnectionManager connectionManager,
                               GatewayNacosDiscovery nacosDiscovery,
                               com.github.im.gateway.session.GatewayRegistry registry,
                               ClientFrames frames,
                               GatewayMetrics metrics,
                               ApplicationEventPublisher events,
                               @Value("${im.gateway.drain.enabled:true}") boolean enabled,
                               @Value("${im.gateway.drain.timeout-ms:60000}") long timeoutMs,
                               @Value("${im.gateway.drain.retry-after-ms:1500}") long retryAfterMs) {
        this.connectionManager = connectionManager;
        this.registryAccessor = new GatewayRegistryAccessor(nacosDiscovery, registry);
        this.frames = frames;
        this.metrics = metrics;
        this.events = events;
        this.enabled = enabled;
        this.timeoutMs = timeoutMs;
        this.retryAfterMs = retryAfterMs;
    }

    /** 是否处于摘流中：握手阶段据此拒绝新连接 */
    public boolean isDraining() {
        return draining.get();
    }

    @EventListener(ContextClosedEvent.class)
    public void onShutdown() {
        if (!enabled) {
            log.info("[drain] 已禁用（im.gateway.drain.enabled=false），跳过优雅摘流");
            return;
        }
        drain("context-closed");
    }

    /**
     * 执行摘流。<b>幂等</b>：重复调用只会返回首次的结果。
     *
     * @return 本次或此前摘流的迁移连接数
     */
    public DrainResult drain(String reason) {
        if (!draining.compareAndSet(false, true)) {
            log.info("[drain] 已在摘流中，忽略重复触发（reason={}）", reason);
            return new DrainResult(true, 0, 0, registryAccessor.localInstanceId());
        }

        String instanceId = registryAccessor.localInstanceId();
        int total = connectionManager.connectionCount();
        log.warn("[drain] 开始摘流 instance={} reason={} connections={} timeoutMs={}",
                instanceId, reason, total, timeoutMs);

        // 1) 从注册中心摘除 + 就绪探针翻红：先断新流量（K8s / LB 都不再导流进来）
        registryAccessor.deregister();
        try {
            AvailabilityChangeEvent.publish(events, this, ReadinessState.REFUSING_TRAFFIC);
        } catch (RuntimeException e) {
            log.debug("[drain] 发布 readiness=REFUSING_TRAFFIC 失败（忽略）: {}", e.getMessage());
        }

        // 2) 广播 session.migrate：让客户端主动重连到其它实例
        int notified = 0;
        String migrateFrame = frames.migrate(reason, retryAfterMs);
        for (Map.Entry<String, ClientSession> entry : connectionManager.snapshot().entrySet()) {
            if (connectionManager.sendTo(entry.getKey(), entry.getValue(), migrateFrame)) {
                notified++;
            }
        }
        metrics.sessionMigrated(notified);
        log.info("[drain] 已向 {} 个连接下发 session.migrate，等待客户端迁移（最长 {}ms）", notified, timeoutMs);

        // 3) 等待连接自然退出（客户端重连 -> 旧连接关闭）
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline && connectionManager.connectionCount() > 0) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // 4) 兜底：窗口结束仍在的连接强制关闭
        int forced = connectionManager.connectionCount();
        for (Map.Entry<String, ClientSession> entry : connectionManager.snapshot().entrySet()) {
            connectionManager.close(entry.getKey(), entry.getValue(), "gateway-draining");
        }
        log.warn("[drain] 摘流结束 instance={} notified={} forcedClose={} remaining={}",
                instanceId, notified, forced, connectionManager.connectionCount());
        return new DrainResult(false, notified, forced, instanceId);
    }

    /**
     * @param alreadyDraining 触发时是否已在摘流
     * @param notified        收到 session.migrate 的连接数
     * @param forcedClosed    等待窗口结束后被强制关闭的连接数
     * @param instanceId      本实例 ID
     */
    public record DrainResult(boolean alreadyDraining, int notified, int forcedClosed, String instanceId) {
    }

    /** 把「摘除注册」与「清理本地路由」收敛到一个内部访问器，避免 drain 逻辑散落 */
    private record GatewayRegistryAccessor(GatewayNacosDiscovery nacosDiscovery,
                                           GatewayRegistry registry) {

        void deregister() {
            try {
                nacosDiscovery.deregisterSelf();
            } catch (Exception e) {
                log.warn("[drain] 注销 Nacos 实例失败（继续摘流）: {}", e.getMessage());
            }
        }

        String localInstanceId() {
            return registry.localInstanceId();
        }
    }
}
