package com.github.im.gateway.push;

import com.github.im.proto.push.v1.DownstreamPushGrpc;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 跨网关实例的下行推送客户端：按「实例 gRPC 地址」缓存 channel。
 *
 * <p>场景：用户连在网关 A，但 IM 服务把帧交给了网关 B（例如 B 是该会话的上报方，
 * 或 IM 服务做了批量聚合）。B 需要把帧转给 A，走本客户端。
 */
@Component
public class GatewayPushClient {

    private static final long DEFAULT_TIMEOUT_MS = 800;

    private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final boolean tlsEnabled;
    private final String trustCert;

    public GatewayPushClient(@Value("${im.node.grpc.tls.enabled:false}") boolean tlsEnabled,
                             @Value("${im.node.grpc.tls.trust-cert:}") String trustCert) {
        this.tlsEnabled = tlsEnabled;
        this.trustCert = trustCert;
    }

    public PushResult push(String address, PushTask task) {
        ManagedChannel channel = channels.computeIfAbsent(address, this::buildChannel);
        return DownstreamPushGrpc.newBlockingStub(channel)
                .withDeadlineAfter(DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                .push(task);
    }

    private ManagedChannel buildChannel(String address) {
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget(address);
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
        return builder.enableRetry().build();
    }

    @PreDestroy
    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdownNow);
        channels.clear();
    }
}
