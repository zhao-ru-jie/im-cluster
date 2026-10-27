package com.github.im.gatewayboot.config;

import com.github.im.contract.MessageIngress;
import com.github.im.gatewayboot.discovery.IngressNameResolverProvider;
import com.github.im.gatewayboot.discovery.IngressTargets;
import com.github.im.rpc.common.GrpcChannelFactory;
import com.github.im.rpc.ingress.RemoteMessageIngress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 网关独立部署时的上行装配：{@link MessageIngress} 只有一个实现——远程 gRPC。
 *
 * <p>与 {@code im-app}（单体装配器）的区别：这里没有本地 {@code MessageService}，
 * 所以不需要 {@code @Primary} 去区分多个同类型 bean，装配意图更直白。
 *
 * <h2>上行多实例：轮询放在 gRPC 里，不放在应用里</h2>
 *
 * <p>链路是：{@link IngressTargets}（订阅 Nacos 健康实例，拿不到就退回
 * {@code im.server.address}）→ {@link IngressNameResolverProvider}（{@code ingress://} scheme）
 * → gRPC channel（{@code round_robin}）→ {@link RemoteMessageIngress}。
 *
 * <p>三个收益，都是「不在应用层选地址」换来的：
 * <ul>
 *   <li><b>轮询精确</b>：gRPC 在 subchannel 层轮询，不受「每请求 RPC 次数」与实例数的公因子影响；</li>
 *   <li><b>故障剔除内建</b>：连不上的 subchannel 进 {@code TRANSIENT_FAILURE} 自动退出轮询，恢复后自动放回；</li>
 *   <li><b>新实例先连上再参与轮询</b>：新地址的 subchannel 要 READY 才进轮询，所以不会出现
 *       「网关选中冷实例、首条消息把建连耗时（实测 260~340ms）塞进 deadline」的偶发 503。</li>
 * </ul>
 *
 * <p><b>超时</b>（默认 1s，见 {@code im.server.timeout-ms}）决定网关对「单条消息实时性」的耐心：
 * 超时即回 {@code error 503} 让客户端退避重发，<b>不会</b>在网关侧重试，也不会因为 IM 服务慢而断开长连接。
 * 早先默认 300ms，与「建连 260~340ms」本就贴得太近——分离部署下 IM 服务还要落库，正常抖动就会撞线。
 *
 * <p><b>仍然要预热</b>：deadline 放宽只是留余量，不该把「首次建连」这种可预期的耗时算进每条消息的预算里。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
public class GatewayWiringConfig {

    private static final Logger log = LoggerFactory.getLogger(GatewayWiringConfig.class);

    @Bean(destroyMethod = "close")
    public GrpcChannelFactory grpcChannelFactory(
            @Value("${im.node.grpc.tls.enabled:false}") boolean tlsEnabled,
            @Value("${im.node.grpc.tls.trust-cert:}") String trustCert,
            @Value("${im.node.grpc.health-check-enabled:false}") boolean healthCheckEnabled) {
        return new GrpcChannelFactory(tlsEnabled, trustCert, healthCheckEnabled);
    }

    /**
     * 上行地址来源：订阅 Nacos 里 im-server 的健康实例。
     *
     * <p>显式创建而非组件扫描：单体 {@code im-app} 不需要远程上行发现，不该为此建 Nacos 客户端。
     * {@code destroy} 由类上的 {@code @PreDestroy} 负责，不需要在这里声明 destroyMethod。
     */
    @Bean
    public IngressTargets ingressTargets(
            @Value("${im.nacos.enabled:false}") boolean nacosEnabled,
            @Value("${im.nacos.server-addr:127.0.0.1:8848}") String nacosServerAddr,
            @Value("${im.nacos.namespace:}") String nacosNamespace,
            @Value("${im.nacos.server.service-name:im-server}") String serverServiceName,
            @Value("${im.server.address:127.0.0.1:9091}") String fallbackAddresses) {
        return new IngressTargets(nacosEnabled, nacosServerAddr, nacosNamespace,
                serverServiceName, fallbackAddresses);
    }

    /**
     * 把 {@code ingress://} 注册进 gRPC 的 NameResolver 表。
     *
     * <p>放在这里显式创建（没有用 SPI 的 {@code META-INF/services}）：注册表是进程级的，
     * 只有「网关独立部署」这一种形态需要它，不该让单体 {@code im-app} 也扫到一个用不上的 resolver。
     */
    @Bean(destroyMethod = "close")
    public IngressNameResolverProvider ingressNameResolverProvider(IngressTargets targets) {
        return new IngressNameResolverProvider(targets);
    }

    @Bean
    public MessageIngress messageIngress(GrpcChannelFactory channels,
                                         IngressTargets targets,
                                         IngressNameResolverProvider resolverProvider,
                                         @Value("${im.server.timeout-ms:1000}") long timeoutMs,
                                         @Value("${im.server.wait-for-ready:true}") boolean waitForReady) {
        // resolverProvider 作为参数只是为了确保「scheme 已注册」先于 channel 构建
        String target = IngressNameResolverProvider.SCHEME + ":///" + targets.serviceName();
        log.info("[ingress] 长连接服务独立部署：上行 gRPC target={}（round_robin），当前可用 IM 服务实例 {}",
                target, targets.current());
        // 启动即等到 channel READY：否则首条消息会把建连（含被调方冷启动）算进 300ms 的 deadline。
        // 注意 READY 的语义：round_robin 聚合状态为 READY 只代表「至少一个实例就绪」，
        // 打开的实例个数由 NameResolver 的地址数决定，其余实例在后台陆续建连（不影响可用性）。
        channels.warmUp(target);
        return new RemoteMessageIngress(channels, target, timeoutMs, waitForReady);
    }
}
