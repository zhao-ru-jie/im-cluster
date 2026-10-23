package com.github.im.serverboot.discovery;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.pojo.Instance;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;
import java.util.Properties;

/**
 * 把本 IM 服务实例注册到 Nacos（service 名默认 {@code im-server}），供网关做上行负载分摊。
 *
 * <h2>为什么 IM 服务现在需要注册</h2>
 *
 * <p>早期的分工是「网关注册自己，IM 服务不注册，网关用 K8s Service / 静态地址调用」。
 * 这个假设只在「IM 服务单实例」或「K8s Service 帮忙做四层负载」时成立——而 gRPC 是
 * <b>长连接 + HTTP/2 多路复用</b>，一旦连上就粘在同一台 Pod 上，Service 的 L4 转发帮不上忙。
 * 结果就是：起 N 个 im-server，实际只有一个在干活。
 *
 * <p>把实例写进 Nacos 后，网关订阅健康列表、每次 RPC 轮询，多实例才真正被用上；
 * 实例下线时 Nacos 的健康状态会把它从列表里摘掉，滚动发布不会把请求打到正在停的实例上。
 *
 * <h2>注册什么</h2>
 *
 * <p>注册的是<b>本实例的 gRPC 地址</b>（{@code ip:grpc-port}），不是 HTTP 端口——
 * 网关调用的是 {@code MessageIngress} gRPC 服务。这和网关那边注册自己是同理：
 * 注册的永远是「别人来找我时该用的那个端口」。
 *
 * <p>健康检查用 Nacos 客户端心跳（{@code ephemeral=true}）：进程一挂，心跳断，实例被摘。
 * 因此不需要额外的 HTTP 健康端点——IM 服务的 HTTP 端口和 gRPC 端口是两条独立链路，
 * 用 HTTP 探活去代表 gRPC 可用性反而不准。
 */
@Component
public class ServerNacosRegistrar {

    private static final Logger log = LoggerFactory.getLogger(ServerNacosRegistrar.class);

    private final boolean enabled;
    private final String serverAddr;
    private final String namespace;
    private final String serviceName;
    private final String configuredRegisterIp;
    private final String grpcHost;
    private final int grpcPort;
    /** 注册前的自测预热：把冷启动开销付在「别人能找到我」之前 */
    private final IngressSelfWarmup selfWarmup;

    private NamingService namingService;
    private Instance self;

    public ServerNacosRegistrar(@Value("${im.nacos.enabled:false}") boolean enabled,
                                @Value("${im.nacos.server-addr:127.0.0.1:8848}") String serverAddr,
                                @Value("${im.nacos.namespace:}") String namespace,
                                @Value("${im.nacos.server.service-name:im-server}") String serviceName,
                                @Value("${im.nacos.register-ip:}") String configuredRegisterIp,
                                @Value("${im.node.grpc-host:}") String grpcHost,
                                @Value("${im.node.grpc-port:9091}") int grpcPort,
                                IngressSelfWarmup selfWarmup) {
        this.enabled = enabled;
        this.serverAddr = serverAddr;
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.configuredRegisterIp = configuredRegisterIp;
        this.grpcHost = grpcHost;
        this.grpcPort = grpcPort;
        this.selfWarmup = selfWarmup;
    }

    /**
     * 在 {@code ApplicationReadyEvent} 时才注册，<b>不是</b> {@code @PostConstruct}；
     * 并且<b>先自测预热、再注册</b>。
     *
     * <p>两个原因，对应两种「重启 im-server 就报 503」的场景：
     * <ul>
     *   <li><b>注册时机</b>：gRPC server 是在 Spring 容器刷新的最后（{@code SmartLifecycle}）才开始监听
     *       端口的，{@code @PostConstruct} 早于它。早注册会让网关去连一个「地址可见、端口没开」的目标，
     *       连接被拒后进入 gRPC 的指数退避（1s/2s/4s…），表现为十几秒的 {@code DEADLINE_EXCEEDED}。</li>
     *   <li><b>预热</b>：即使端口已开，刚启动的实例处理第一条请求也要 0.8~1.2 秒
     *       （首连握手 350ms + 首次处理 400ms+）。先自测预热，就能让网关只看到「已经打得动」的实例。</li>
     * </ul>
     */
    @EventListener(ApplicationReadyEvent.class)
    public void init() {
        // 预热先做：与是否注册 Nacos 无关（静态地址部署同样受益）
        selfWarmup.warmUp();
        if (!enabled) {
            log.info("[nacos] IM 服务未注册（im.nacos.enabled=false），网关将使用静态 im.server.address");
            return;
        }
        try {
            Properties props = new Properties();
            props.put("serverAddr", serverAddr);
            if (namespace != null && !namespace.isBlank()) {
                props.put("namespace", namespace);
            }
            namingService = NacosFactory.createNamingService(props);
            self = buildInstance();
            namingService.registerInstance(serviceName, self);
            log.info("[nacos] 已注册 IM 服务实例 {}/{}（ip={}, grpcPort={}），网关可据此做上行分摊",
                    serviceName, self.getMetadata().get("grpcHost"), self.getIp(), self.getPort());
        } catch (Exception e) {
            // 容错：注册失败只影响多实例分摊，不影响本实例处理请求（网关仍可静态地址调用）
            log.error("[nacos] 注册 IM 服务实例失败，多实例分摊不可用: {}", e.getMessage());
            namingService = null;
        }
    }

    private Instance buildInstance() {
        Instance instance = new Instance();
        instance.setIp(resolveRegisterIp());
        instance.setPort(grpcPort);
        instance.setEphemeral(true);
        instance.addMetadata("grpcHost", grpcHost == null || grpcHost.isBlank() ? instance.getIp() : grpcHost);
        return instance;
    }

    /**
     * 注册用 IP 必须是<b>对端可直连</b>的地址，不能用 {@code 0.0.0.0} 或 DNS 名。
     *
     * <p>gRPC 监听通常绑 {@code 0.0.0.0}（{@code spring.grpc.server.host}），那是监听地址、
     * 不是可达地址，直接拿去注册会让网关连不上。
     */
    private String resolveRegisterIp() {
        if (configuredRegisterIp != null && !configuredRegisterIp.isBlank()) {
            return configuredRegisterIp;
        }
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isBlank()) {
            return podIp;
        }
        if (isInet4Address(grpcHost)) {
            return grpcHost;
        }
        String nic = firstNonLoopbackIp();
        return nic != null ? nic : "127.0.0.1";
    }

    private static String firstNonLoopbackIp() {
        try {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            while (nets != null && nets.hasMoreElements()) {
                NetworkInterface ni = nets.nextElement();
                if (ni.isLoopback() || !ni.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addrs = ni.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if (a instanceof Inet4Address && !a.isLoopbackAddress()) {
                        return a.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            // 探测失败则交给调用方兜底
        }
        return null;
    }

    private static boolean isInet4Address(String s) {
        if (s == null || s.isBlank()) {
            return false;
        }
        String[] parts = s.split("\\.");
        if (parts.length != 4) {
            return false;
        }
        for (String p : parts) {
            try {
                int v = Integer.parseInt(p);
                if (v < 0 || v > 255) {
                    return false;
                }
            } catch (NumberFormatException e) {
                return false;
            }
        }
        return true;
    }

    /**
     * 提前注销（优雅停机第一步）：让网关立刻停止把新请求导进来。
     *
     * <p>与「等待在途请求结束」配合使用才有意义——先摘掉流量，再等在途 RPC 收尾，
     * 最后才关 gRPC server。顺序反了就会出现「正在处理请求的实例被关掉」的瞬时错误。
     */
    public void deregisterSelf() {
        if (namingService == null || self == null) {
            return;
        }
        try {
            namingService.deregisterInstance(serviceName, self);
            log.warn("[nacos] 已注销 IM 服务实例 {}:{}, 不再接收新的上行请求", self.getIp(), self.getPort());
        } catch (Exception e) {
            log.error("[nacos] 注销 IM 服务实例失败（依赖心跳超时兜底）: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        if (namingService == null) {
            return;
        }
        try {
            if (self != null) {
                namingService.deregisterInstance(serviceName, self);
            }
            namingService.shutDown();
            log.info("[nacos] 已注销 IM 服务实例并关闭 Nacos 客户端");
        } catch (Exception e) {
            log.error("[nacos] 注销失败: {}", e.getMessage());
        }
    }
}
