package com.github.im.gatewayboot.discovery;

import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

/**
 * 注册 {@code ingress://} 这个 target scheme，让 gRPC 的 channel 能从服务发现拿地址。
 *
 * <p>用法：{@code NettyChannelBuilder.forTarget("ingress:///im-server")}。scheme 一出现，
 * gRPC 就去 {@link NameResolverRegistry} 找本类，拿到 {@link IngressNameResolver}，
 * 进而拿到地址列表，再交给 {@code round_robin} 轮询。
 *
 * <p><b>为什么 scheme 叫 ingress 而不是 nacos</b>：地址来源可能不是 Nacos——Nacos 关闭或不可用时
 * {@link IngressTargets} 会退回 {@code im.server.address} 静态地址。叫 {@code ingress} 更诚实：
 * 这个 scheme 的含义是「上行目标，来源由 {@link IngressTargets} 决定」。
 *
 * <p>注册是<b>进程级</b>的（{@link NameResolverRegistry} 是全局表），所以由装配层创建单例并在
 * 关闭时 {@link #close()} 反注册，避免多次启动/关闭（测试、热重启）时残留。
 */
public class IngressNameResolverProvider extends NameResolverProvider {

    public static final String SCHEME = "ingress";

    private static final Logger log = LoggerFactory.getLogger(IngressNameResolverProvider.class);

    private final IngressTargets targets;
    private boolean registered;

    public IngressNameResolverProvider(IngressTargets targets) {
        this.targets = targets;
        NameResolverRegistry.getDefaultRegistry().register(this);
        this.registered = true;
        log.info("[ingress] 已注册 NameResolver scheme {}://，服务名 {}", SCHEME, targets.serviceName());
    }

    @Override
    public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
        return new IngressNameResolver(targets);
    }

    @Override
    public String getDefaultScheme() {
        return SCHEME;
    }

    @Override
    protected boolean isAvailable() {
        return true;
    }

    @Override
    protected int priority() {
        return 5;
    }

    public void close() {
        if (registered) {
            NameResolverRegistry.getDefaultRegistry().deregister(this);
            registered = false;
        }
    }

    @Override
    public String toString() {
        return "IngressNameResolverProvider{" + targets.serviceName() + "}";
    }
}
