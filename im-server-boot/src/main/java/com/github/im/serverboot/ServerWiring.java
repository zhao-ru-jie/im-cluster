package com.github.im.serverboot;

import com.github.im.contract.DeviceControlPort;
import com.github.im.contract.PushPort;
import com.github.im.rpc.common.AddressResolver;
import com.github.im.rpc.common.GrpcChannelFactory;
import com.github.im.rpc.push.GatewayRouteReader;
import com.github.im.rpc.push.RemoteDeviceControl;
import com.github.im.rpc.push.RemoteGatewayPushPort;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;

/**
 * IM 服务独立部署时的端口装配：把 {@link PushPort} 接到「远程网关实例」。
 *
 * <p>业务模块（im-server）只依赖 {@link PushPort} 接口，不知道 gRPC、channel、路由表的存在，
 * 所以这一层装配是整个拆分里唯一需要「决定用哪种传输」的地方。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
public class ServerWiring {

    @Bean(destroyMethod = "close")
    public GrpcChannelFactory grpcChannelFactory(
            @Value("${im.node.grpc.tls.enabled:false}") boolean tlsEnabled,
            @Value("${im.node.grpc.tls.trust-cert:}") String trustCert,
            @Value("${im.node.grpc.health-check-enabled:false}") boolean healthCheckEnabled) {
        return new GrpcChannelFactory(tlsEnabled, trustCert, healthCheckEnabled);
    }

    @Bean
    public PushPort pushPort(StringRedisTemplate redis,
                             GrpcChannelFactory channels,
                             @Value("${im.push.timeout-ms:800}") long timeoutMs,
                             @Value("${im.push.wait-for-ready:false}") boolean waitForReady) {
        return new RemoteGatewayPushPort(new GatewayRouteReader(redis), channels,
                AddressResolver.DIRECT, timeoutMs, waitForReady);
    }

    /**
     * 连接控制（踢下线）的远程实现。
     *
     * <p>与 {@link #pushPort} 分开两个 bean 而不是合成一个端口：两者可靠性模型相反
     * （推送可丢、失败转离线；踢下线不可丢、失败必须如实上报），
     * 合并后调用方迟早会把「没踢到」当成「没关系」。
     *
     * <p>超时用的是 {@code im.device.kick-timeout-ms}（默认 800ms）：踢下线是同步动作，
     * 调用方在等结果，不能像推送那样无限容忍实例慢。
     */
    @Bean
    public DeviceControlPort deviceControlPort(StringRedisTemplate redis,
                                              GrpcChannelFactory channels,
                                              @Value("${im.device.kick-timeout-ms:800}") long timeoutMs) {
        return new RemoteDeviceControl(new GatewayRouteReader(redis), channels,
                AddressResolver.DIRECT, timeoutMs);
    }
}
