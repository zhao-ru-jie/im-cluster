package com.github.im.gatewayboot.discovery;

import io.grpc.EquivalentAddressGroup;
import io.grpc.NameResolver;
import io.grpc.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.function.Consumer;

/**
 * 把 {@link IngressTargets} 的地址列表喂给 gRPC 的 {@link NameResolver}。
 *
 * <p>关键接缝：gRPC 一旦拿到多个地址，就会为每个地址建一个 subchannel，
 * 再由 channel 上的 {@code round_robin} 策略在 <b>READY 的 subchannel</b> 之间轮询。于是：
 * <ul>
 *   <li>轮询是精确的，不再依赖应用层的计数器（也就不会有「每请求 2 次 RPC + 2 实例」的别名问题）；</li>
 *   <li>实例挂掉时 subchannel 进 {@code TRANSIENT_FAILURE}，自动退出轮询；恢复后自动放回；</li>
 *   <li>新实例出现时它会先在后台连上、READY 之后才进入轮询——不会出现「网关选到冷实例、
 *       首条消息把建连耗时塞进 300ms deadline」的问题（这正是之前偶发 503 的成因）。</li>
 * </ul>
 *
 * <p>实例变更是<b>推送式</b>的：Nacos 通知 → {@link IngressTargets#addListener} → 这里重新
 * {@code onResult}，gRPC 据此增减 subchannel，不需要轮询或重建 channel。
 */
public class IngressNameResolver extends NameResolver {

    private static final Logger log = LoggerFactory.getLogger(IngressNameResolver.class);

    private final IngressTargets targets;

    private Listener2 listener;
    private Consumer<List<String>> subscription;
    /** 上一次推给 gRPC 的地址，用于去重（Nacos 有时会重复通知同样的内容） */
    private List<String> published = List.of();

    public IngressNameResolver(IngressTargets targets) {
        this.targets = targets;
    }

    @Override
    public String getServiceAuthority() {
        return targets.serviceName();
    }

    @Override
    public void start(Listener2 listener) {
        this.listener = listener;
        this.subscription = this::publish;
        // 注册即推一次当前值，之后每次变更再推
        targets.addListener(subscription);
    }

    private void publish(List<String> addresses) {
        if (listener == null || addresses.equals(published)) {
            return;
        }
        List<EquivalentAddressGroup> groups = addresses.stream()
                .map(IngressNameResolver::toGroup)
                .toList();
        if (groups.isEmpty()) {
            // 正常不会走到这里（IngressTargets 保证至少有降级地址），留作兜底
            listener.onError(Status.UNAVAILABLE.withDescription("no ingress target available"));
            return;
        }
        published = addresses;
        log.info("[ingress] 推送上行地址给 gRPC（round_robin）: {}", addresses);
        listener.onResult(ResolutionResult.newBuilder().setAddresses(groups).build());
    }

    @Override
    public void refresh() {
        publish(targets.current());
    }

    @Override
    public void shutdown() {
        if (subscription != null) {
            targets.removeListener(subscription);
            subscription = null;
        }
        listener = null;
    }

    /** {@code host:port} → gRPC 的地址组；这里一个地址一个组，round_robin 按组轮询 */
    private static EquivalentAddressGroup toGroup(String address) {
        int idx = address.lastIndexOf(':');
        if (idx <= 0 || idx == address.length() - 1) {
            throw new IllegalArgumentException("invalid ingress address: " + address);
        }
        String host = address.substring(0, idx);
        int port = Integer.parseInt(address.substring(idx + 1));
        return new EquivalentAddressGroup(new InetSocketAddress(host, port));
    }
}
