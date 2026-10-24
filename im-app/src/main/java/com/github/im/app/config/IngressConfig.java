package com.github.im.app.config;

import com.github.im.app.shadow.ShadowMessageIngress;
import com.github.im.contract.MessageIngress;
import com.github.im.rpc.common.GrpcChannelFactory;
import com.github.im.rpc.ingress.RemoteMessageIngress;
import com.github.im.server.message.MessageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * 上行通道装配：决定「网关把消息交给谁」。
 *
 * <p>三档由 {@code im.ingress.transport} 控制，对应灰度上线的三个步骤：
 * <table border="1">
 *   <caption>transport 三档</caption>
 *   <tr><th>取值</th><th>上行路径</th><th>适用阶段</th></tr>
 *   <tr><td>{@code local}（默认）</td><td>本地直调 {@link MessageService}</td><td>阶段 1：行为与拆分前一致</td></tr>
 *   <tr><td>{@code grpc}</td><td>{@link RemoteMessageIngress} → 独立部署的 IM 服务</td><td>阶段 2：切流（第一步先小流量）</td></tr>
 *   <tr><td>{@code shadow}</td><td>本地为主 + 远程异步双跑比对</td><td>阶段 2：切流前的等价性验证</td></tr>
 * </table>
 *
 * <p>{@code @Primary} 让 {@code ObjectProvider<MessageIngress>} 在「本地实现 / 远程实现 / Shadow 包装」
 * 三个同类型 bean 并存时仍能唯一解析——这是单体装配下必须显式表达的语义。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
public class IngressConfig {

    private static final Logger log = LoggerFactory.getLogger(IngressConfig.class);

    @Bean(destroyMethod = "close")
    public GrpcChannelFactory grpcChannelFactory(
            @Value("${im.node.grpc.tls.enabled:false}") boolean tlsEnabled,
            @Value("${im.node.grpc.tls.trust-cert:}") String trustCert,
            @Value("${im.node.grpc.health-check-enabled:false}") boolean healthCheckEnabled) {
        return new GrpcChannelFactory(tlsEnabled, trustCert, healthCheckEnabled);
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(name = "im.ingress.transport", havingValue = "shadow")
    public ShadowMessageIngress shadowMessageIngress(
            MessageService localMessageService,
            GrpcChannelFactory channels,
            @Value("${im.server.address:127.0.0.1:9091}") String serverAddress,
            @Value("${im.server.timeout-ms:300}") long timeoutMs,
            @Value("${im.server.wait-for-ready:true}") boolean waitForReady,
            @Value("${im.shadow.sample-rate:1.0}") double sampleRate) {
        RemoteMessageIngress remote = new RemoteMessageIngress(channels, serverAddress, timeoutMs, waitForReady);
        log.info("[ingress] shadow 双跑已启用：主=本地, shadow=grpc({}), sampleRate={}", serverAddress, sampleRate);
        return new ShadowMessageIngress(localMessageService, remote, sampleRate);
    }

    @Bean
    @Primary
    public MessageIngress messageIngress(
            MessageService localMessageService,
            GrpcChannelFactory channels,
            ObjectProvider<ShadowMessageIngress> shadowProvider,
            @Value("${im.ingress.transport:local}") String transport,
            @Value("${im.server.address:127.0.0.1:9091}") String serverAddress,
            @Value("${im.server.timeout-ms:300}") long timeoutMs,
            @Value("${im.server.wait-for-ready:true}") boolean waitForReady) {
        if (!"local".equalsIgnoreCase(transport)) {
            // grpc / shadow 都要连远程 IM 服务：启动即预热（等到 READY），
            // 避免第一条消息把建连耗时算进 300ms 的 deadline
            channels.warmUp(serverAddress);
        }
        return switch (transport.toLowerCase()) {
            case "grpc" -> {
                log.info("[ingress] 上行走远程 gRPC：{}", serverAddress);
                yield new RemoteMessageIngress(channels, serverAddress, timeoutMs, waitForReady);
            }
            case "shadow" -> {
                ShadowMessageIngress shadow = shadowProvider.getIfAvailable();
                if (shadow == null) {
                    throw new IllegalStateException("im.ingress.transport=shadow 但 ShadowMessageIngress 未装配");
                }
                yield shadow;
            }
            default -> {
                log.info("[ingress] 上行走本地直调（阶段 1 行为）");
                yield localMessageService;
            }
        };
    }
}
