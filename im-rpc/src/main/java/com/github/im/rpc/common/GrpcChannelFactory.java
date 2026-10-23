package com.github.im.rpc.common;

import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * gRPC channel 缓存工厂：按「gRPC target」复用 ManagedChannel。
 *
 * <p>两个方向的长连接都由本类统一管理，避免每调用一次建一次 TCP：
 * <ul>
 *   <li>网关 → IM 服务（{@code rpc.ingress.RemoteMessageIngress}）：target 是
 *       {@code ingress:///im-server} 这类多地址 URI，<b>整个进程只有 1 个 channel</b>，
 *       多实例轮询由 channel 内的 {@code round_robin} 完成；</li>
 *   <li>IM 服务 → 各网关实例（{@code rpc.push.RemoteGatewayPushPort}）：target 是网关实例
 *       地址 {@code host:port}，按实例缓存。</li>
 * </ul>
 *
 * <p>线程安全：{@link ConcurrentHashMap} + {@code computeIfAbsent} 保证同一 target 只建一次。
 */
public class GrpcChannelFactory implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(GrpcChannelFactory.class);

    /**
     * 预热等待 READY 的上限。实测被调方首条连接 260~340ms，2s 足够覆盖；
     * 上限的意义是「对端根本没起来时别把调用线程挂死」。
     */
    private static final long WARM_UP_TIMEOUT_MS = 2_000L;

    /**
     * 健康检查针对的「服务名」：空串 = 服务端整体健康状态。
     *
     * <p>与 Spring Boot 的 {@code spring.grpc.server.health.include-overall-health}（默认开）对应，
     * 服务端把 Actuator 的聚合健康映射成 {@code ""} 这个 service 的 SERVING / NOT_SERVING。
     */
    private static final String HEALTH_SERVICE_NAME = "";

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final boolean tlsEnabled;
    private final String trustCert;
    private final boolean healthCheckEnabled;

    /**
     * @param healthCheckEnabled 是否启用客户端健康检查（{@code im.node.grpc.health-check-enabled}）。
     *                           打开后「连得上但 NOT_SERVING」的实例会被排除出轮询。
     *                           对端若没实现 {@code grpc.health.v1.Health}，Watch 会返回 UNIMPLEMENTED，
     *                           此时 grpc-java <b>不会</b>把该实例判为不健康（实测，见
     *                           {@code GrpcChannelFactoryHealthTest#missingHealthServiceIsTreatedAsUsable}），
     *                           所以混合版本部署是安全的：老服务端只是永远不会被摘掉。
     */
    public GrpcChannelFactory(boolean tlsEnabled, String trustCert, boolean healthCheckEnabled) {
        this.tlsEnabled = tlsEnabled;
        this.trustCert = trustCert;
        this.healthCheckEnabled = healthCheckEnabled;
    }

    public ManagedChannel channelFor(String address) {
        return channels.computeIfAbsent(address, this::build);
    }

    /**
     * 预热：建好 channel 并<b>等到 READY</b>，把建连开销挪出首条消息的 deadline。
     *
     * <p><b>为什么必须等 READY，而不是只 {@code getState(true)}</b>：后者只把 channel 从 IDLE
     * 推进到 CONNECTING 就返回。实测被调方 gRPC 服务进程的首条连接（Netty/H2 handler 初始化 +
     * 首个请求的类加载）要 <b>260~340ms</b>，而纯 TCP 建连只要 5ms——这段时间若留给第一条 RPC，
     * 300ms 的 deadline 会被吃掉 260ms 并稳定超时。表现为「预热明明调了，第一条消息还是 503」。
     *
     * <p>等待有上限（{@code warmUpTimeoutMs}）：被调方没起来时不能让启动或 Nacos 回调线程挂死。
     *
     * <p>目标地址不固定的方向（如 IM 服务 → 各网关实例）无法预热，只能由调用方承担首次建连耗时，
     * 那里用的是更宽松的推送超时（默认 800ms）与「失败即落离线」的兜底。
     */
    public void warmUp(String address) {
        ManagedChannel channel = channelFor(address);
        long start = System.nanoTime();
        if (awaitReady(channel, WARM_UP_TIMEOUT_MS)) {
            log.debug("channel {} 预热完成，{}ms", address, elapsedMs(start));
            return;
        }
        // 对端可能「刚注册进注册中心、gRPC 端口还没开始监听」：这时连接被拒，gRPC 按
        // 1s/2s/4s… 指数退避重连。光等着会把恢复时间从毫秒级拖到十几秒，而这段时间里
        // 每条消息都在 300ms 的 deadline 上排队并失败（线上表现就是连续 DEADLINE_EXCEEDED）。
        // 显式重置退避再等一轮，让预热具备「立刻重试一次」的语义。
        channel.resetConnectBackoff();
        if (awaitReady(channel, WARM_UP_TIMEOUT_MS)) {
            log.debug("channel {} 预热完成（重置连接退避后），{}ms", address, elapsedMs(start));
            return;
        }
        log.warn("channel {} 预热 {}ms 后仍未 READY（state={}），首条消息可能超时",
                address, elapsedMs(start), channel.getState(false));
    }

    /** 轮询等待 channel 变成 READY；{@code getState(true)} 负责在 IDLE 时触发连接 */
    private static boolean awaitReady(ManagedChannel channel, long timeoutMs) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        ConnectivityState state = channel.getState(true);
        while (state != ConnectivityState.READY && System.nanoTime() < deadline) {
            if (state == ConnectivityState.SHUTDOWN) {
                return false;
            }
            try {
                Thread.sleep(10L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
            state = channel.getState(false);
        }
        return channel.getState(false) == ConnectivityState.READY;
    }

    private static long elapsedMs(long startNanos) {
        return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
    }

    private ManagedChannel build(String address) {
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget(address);
        // 一律用 round_robin，理由有两条：
        //   1) 带 scheme 的 target 意味着「一个 target 可能解析出多个地址」（服务发现）。默认的
        //      pick_first 只会用解析结果里的第一个地址，多实例照样只吃一份流量；round_robin 在
        //      subchannel 层轮询，并内建「连不上就摘掉、恢复后自动放回」，不用应用层维护地址表。
        //   2) 健康检查（gRFC A18）只在 round_robin 下生效；单地址 target 用 round_robin 也无害
        //      （只有一个 subchannel），换来上下行两个方向语义一致。
        // 注意：round_robin **本来就只挑 READY 的 subchannel**（CONNECTING / TRANSIENT_FAILURE
        // 不参与 pick，RPC 会在 picker 里排队等一个 READY 的）。健康检查是在此之上再加一层：
        // 「TCP/HTTP2 连得上、但应用还没就绪或正在摘流」的实例也会被排除。
        builder.defaultLoadBalancingPolicy("round_robin");
        if (healthCheckEnabled) {
            // 客户端按 grpc.health.v1.Health/Watch 订阅服务端状态：NOT_SERVING 的 subchannel 会被
            // 当作 TRANSIENT_FAILURE 从 picker 里摘掉，恢复 SERVING 后自动放回。
            // 与 warmUp() 配合后的语义也更准了：channel READY 现在意味着「至少有一个实例
            // 既连得上、又认为自己能服务」，而不再只是「连得上」。
            builder.defaultServiceConfig(Map.of(
                    "healthCheckConfig", Map.of("serviceName", HEALTH_SERVICE_NAME)));
        }
        if (tlsEnabled) {
            builder.useTransportSecurity();
            if (trustCert != null && !trustCert.isBlank()) {
                try {
                    Resource resource = trustCert.startsWith("classpath:")
                            ? new ClassPathResource(trustCert.substring("classpath:".length()))
                            : new FileSystemResource(new File(trustCert));
                    try (InputStream in = resource.getInputStream()) {
                        builder.sslContext(GrpcSslContexts.forClient().trustManager(in).build());
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("load gRPC client trust cert failed: " + trustCert, e);
                }
            }
        } else {
            builder.usePlaintext();
        }
        ManagedChannel channel = builder.enableRetry().build();
        // 主动触发建连（DNS 解析 + TCP 握手 + HTTP/2 preface），让它在后台完成。
        // 否则这笔开销会算进「首次」RPC 的 deadline：上行超时只有 300ms 量级，
        // 冷启动时第一条消息必然 DEADLINE_EXCEEDED（实测 265ms 全花在建连上）。
        channel.getState(true);
        return channel;
    }

    @Override
    public void close() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
