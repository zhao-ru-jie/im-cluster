package com.example.im.grpc;

import io.grpc.ManagedChannel;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NettyChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;

/**
 * gRPC 客户端：把消息转发给其他节点。
 * 目标节点地址在运行时从 Redis 动态查得，因此按「节点 gRPC 地址」缓存 ManagedChannel。
 * TLS 开关来自配置 im.node.grpc.tls.*。
 */
@Component
public class ForwardClient {

    private final ConcurrentHashMap<String, ManagedChannel> channels = new ConcurrentHashMap<>();
    private final boolean tlsEnabled;
    private final String trustCert;

    public ForwardClient(@Value("${im.node.grpc.tls.enabled:false}") boolean tlsEnabled,
                         @Value("${im.node.grpc.tls.trust-cert:}") String trustCert) {
        this.tlsEnabled = tlsEnabled;
        this.trustCert = trustCert;
    }

    public ForwardResponse forward(String nodeAddress, ForwardRequest request) {
        ManagedChannel channel = channels.computeIfAbsent(nodeAddress, this::buildChannel);
        return ForwardServiceGrpc.newBlockingStub(channel).forward(request);
    }

    private ManagedChannel buildChannel(String addr) {
        NettyChannelBuilder builder = NettyChannelBuilder.forTarget(addr);
        if (tlsEnabled) {
            builder.useTransportSecurity();
            if (trustCert != null && !trustCert.isBlank()) {
                try {
                    Resource r = trustCert.startsWith("classpath:")
                            ? new ClassPathResource(trustCert.substring("classpath:".length()))
                            : new FileSystemResource(new File(trustCert));
                    builder.sslContext(GrpcSslContexts.forClient().trustManager(r.getInputStream()).build());
                } catch (Exception e) {
                    throw new IllegalStateException("load gRPC client trust cert failed: " + trustCert, e);
                }
            }
        } else {
            builder.usePlaintext();
        }
        return builder.enableRetry().build();
    }

    public void shutdown() {
        channels.values().forEach(ManagedChannel::shutdownNow);
    }
}
