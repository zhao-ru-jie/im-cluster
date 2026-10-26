package com.github.im.gateway.node;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.github.im.gateway.session.GatewayRegistry;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 网关实例地址的注册与动态发现（Nacos-Client SDK，不用 Spring Cloud Alibaba 全家桶）。
 *
 * <p><b>职责边界（拆分后更清晰）</b>：
 * <ul>
 *   <li>Nacos 负责：「gatewayInstanceId → 实时可达的 gRPC 地址(ip:port)」，带健康探针；</li>
 *   <li>Redis 负责：「userId → gatewayInstanceId」这种 per-user 会话路由，Nacos 管不了；</li>
 *   <li>「网关 → IM 服务」的上行寻址<b>不由本类负责</b>：IM 服务由它自己注册
 *       （{@code im-server-boot} 的 {@code ServerNacosRegistrar}，service=im-server，
 *       注册的是 gRPC 端口），网关侧由 {@code im-gateway-boot} 的 {@code IngressTargets}
 *       订阅该服务的健康实例，再交给 gRPC 的 {@code round_robin} 轮询；
 *       订阅不到时才回退 {@code im.server.address}（静态地址兜底）。
 *       之所以不用 K8s Service 做四层负载：gRPC 是长连接，连上就粘在同一台 Pod 上，
 *       起 N 个 im-server 会只有一个在干活。</li>
 * </ul>
 *
 * <p><b>不健康监听</b>：订阅实例变更，diff 出掉队的实例并调用
 * {@link GatewayRegistry#removeInstanceBindings(String)} 清理 Redis 路由，
 * 避免窗口期内消息持续推往死实例。清理动作发生在其它健康实例上（死实例自己已无法执行），
 * 并且按确定性选主只让<b>其中一台</b>执行——否则 N-1 台会各扫一遍全库（见 {@link #cleanDeadInstance}）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class GatewayNacosDiscovery {

    private static final Logger log = LoggerFactory.getLogger(GatewayNacosDiscovery.class);
    private static final String INSTANCE_ID_KEY = "instanceId";

    private final boolean enabled;
    private final String serverAddr;
    private final String serviceName;
    private final String namespace;
    private final NodeAddressResolver addressResolver;
    private final GatewayRegistry gatewayRegistry;

    private NamingService namingService;
    private EventListener instanceListener;
    /** 当前已知健康的实例 ID 集合，用于 diff 出「变不健康 / 已下线」的实例 */
    private final Set<String> healthyInstanceIds = new HashSet<>();

    public GatewayNacosDiscovery(@Value("${im.nacos.enabled:false}") boolean enabled,
                                 @Value("${im.nacos.server-addr:127.0.0.1:8848}") String serverAddr,
                                 @Value("${im.nacos.service-name:im-gateway}") String serviceName,
                                 @Value("${im.nacos.namespace:}") String namespace,
                                 NodeAddressResolver addressResolver,
                                 GatewayRegistry gatewayRegistry) {
        this.enabled = enabled;
        this.serverAddr = serverAddr;
        this.serviceName = serviceName;
        this.namespace = namespace;
        this.addressResolver = addressResolver;
        this.gatewayRegistry = gatewayRegistry;
    }

    // 全限定名：本类同时 import 了 Nacos 的 EventListener（实例变更回调），简名会冲突
    @org.springframework.context.event.EventListener(ApplicationReadyEvent.class)
    public void init() {
        if (!enabled) {
            log.info("[nacos] disabled (im.nacos.enabled=false)，跨实例推送的目标地址回退为直接使用实例 ID");
            return;
        }
        try {
            Properties props = new Properties();
            props.put("serverAddr", serverAddr);
            if (namespace != null && !namespace.isBlank()) {
                props.put("namespace", namespace);
            }
            namingService = NacosFactory.createNamingService(props);
            // 注册放在 ApplicationReadyEvent：此时 gRPC server（9090）已经开始监听。
            // 若在 @PostConstruct 注册，IM 服务会在「地址已可见、端口还没开」的窗口里回推，
            // 那些推送必然失败并落到离线，表现为重启网关后一批消息延迟。
            namingService.registerInstance(serviceName, buildInstance());
            subscribe();
            log.info("[nacos] 已连接 {}，注册网关服务 {}（instanceId={}, ip={}, port={}），并已订阅实例变更",
                    serverAddr, serviceName, addressResolver.localInstanceId(),
                    addressResolver.getRegisterIp(), addressResolver.getPort());
        } catch (Exception e) {
            // 容错：Nacos 起不来不影响单机/同机 demo，跨实例推送回退到直接用实例 ID dial
            log.error("[nacos] 初始化失败，跨实例推送将回退为直接使用实例 ID: {}", e.getMessage());
            namingService = null;
        }
    }

    private Instance buildInstance() {
        Instance instance = new Instance();
        instance.setIp(addressResolver.getRegisterIp());
        instance.setPort(addressResolver.getPort());
        instance.setEphemeral(true);
        instance.addMetadata(INSTANCE_ID_KEY, addressResolver.localInstanceId());
        instance.addMetadata("grpcHost", addressResolver.getHost());
        return instance;
    }

    private void subscribe() throws Exception {
        for (Instance ins : namingService.getAllInstances(serviceName)) {
            String instanceId = instanceIdOf(ins);
            if (ins.isHealthy() && instanceId != null) {
                healthyInstanceIds.add(instanceId);
            }
        }
        instanceListener = event -> {
            if (!(event instanceof NamingEvent namingEvent)) {
                return;
            }
            Set<String> nowHealthy = namingEvent.getInstances().stream()
                    .filter(Instance::isHealthy)
                    .map(this::instanceIdOf)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            // 排序后再选主：Nacos 通知里的实例顺序不保证一致，不排序各实例会算出不同的 owner
            List<String> aliveSorted = nowHealthy.stream().sorted().toList();

            for (String gone : new HashSet<>(healthyInstanceIds)) {
                if (nowHealthy.contains(gone)) {
                    continue;
                }
                // 自己「变不健康」时只更新本地视图，绝不清理 Redis 路由，原因有三：
                //   1. 进程真死了，这段监听根本不会执行——死实例的路由只能由活着的实例代劳清理；
                //   2. 进程还活着时只有两种可能：主动摘流（deregisterSelf 之后仍要靠这些路由
                //      收推送、广播 session.migrate）与 Nacos 心跳抖动。两种情况下本机连接都
                //      好好的，删掉自己的路由会让 IM 服务把这些在线用户判成离线、消息误落离线队列；
                //   3. 真死了也有兜底：路由 TTL 30s，只靠本实例每 10s 一次的续期维持；
                //      而删掉之后下一轮续期又会写回来，等于白造一个 0~10s 的假离线窗口。
                if (gone.equals(addressResolver.localInstanceId())) {
                    healthyInstanceIds.remove(gone);
                    continue;
                }
                healthyInstanceIds.remove(gone);
                cleanDeadInstance(gone, aliveSorted);
            }
            healthyInstanceIds.addAll(nowHealthy);
        };
        namingService.subscribe(serviceName, instanceListener);
    }

    /**
     * 清理一个已下线实例残留的用户路由，<b>保证同一时刻只有一个实例真正执行</b>。
     *
     * <p>为什么必须去重：Nacos 把「XX 不健康」推给<b>每个</b>订阅者，N-1 个活实例都会走到这里，
     * 而 {@link GatewayRegistry#removeInstanceBindings(String)} 是一次 O(全部在线用户) 的全库 SCAN。
     * N-1 份并发正好砸在「刚掉一个节点、客户端批量重连、路由表读写最忙」的时刻，
     * 等于拿 Redis 单线程的延迟去换一次本来就可有可无的清理。
     *
     * <p>选主规则：健康实例列表<b>排序</b>后取 {@code hash(死实例ID) % 实例数}。不需要锁、
     * 不需要协调，天然只清一次；排序是前提，否则各实例算出的 owner 会不一致。
     *
     * <p>漏清是安全的：命中的 owner 自己也恰好不可用时没人清理，死实例的路由因为不再被续期，
     * 30s TTL 后自然过期；这期间多出来的无效推送由 IM 服务的 {@code failed_user_ids} 落离线兜底。
     */
    private void cleanDeadInstance(String deadInstanceId, List<String> aliveSorted) {
        if (aliveSorted.isEmpty()) {
            log.warn("[nacos] 网关实例 {} 不再健康，但当前无健康实例可执行清理，交由路由 TTL 兜底", deadInstanceId);
            return;
        }
        String owner = cleanupOwnerOf(deadInstanceId, aliveSorted);
        if (!owner.equals(addressResolver.localInstanceId())) {
            log.debug("[nacos] 网关实例 {} 不再健康，由 {} 负责清理路由（本实例跳过）", deadInstanceId, owner);
            return;
        }
        log.warn("[nacos] 网关实例 {} 不再健康，本实例负责清理其在 Redis 中的用户路由", deadInstanceId);
        try {
            gatewayRegistry.removeInstanceBindings(deadInstanceId);
        } catch (Exception ex) {
            log.error("[nacos] 清理实例 {} 的 Redis 路由失败: {}", deadInstanceId, ex.getMessage());
        }
    }

    /**
     * 「谁负责清理这个死实例的路由」的确定性选主：所有实例对同一组输入必须算出同一结果。
     *
     * <p>抽成静态纯函数是为了能单测：选主错了不会抛异常，只会退化成「多台一起扫全库」或
     * 「谁都不扫」，前者表现为故障瞬间 Redis 抖一下，后者表现为消息多绕 30s 才落离线，
     * 两个方向在线上都很难看出来。
     *
     * @param aliveSorted 已排序的健康实例 ID 列表；<b>排序是选主一致性的前提</b>
     */
    static String cleanupOwnerOf(String deadInstanceId, List<String> aliveSorted) {
        return aliveSorted.get(Math.floorMod(deadInstanceId.hashCode(), aliveSorted.size()));
    }

    private String instanceIdOf(Instance ins) {
        Map<String, String> metadata = ins.getMetadata();
        return metadata == null ? null : metadata.get(INSTANCE_ID_KEY);
    }

    /**
     * 把实例 ID 解析成实时可达的 gRPC 目标地址(ip:port)。
     * Nacos 未启用 / 不可用 / 匹配不到时直接返回实例 ID 兜底。
     */
    public String resolveTarget(String instanceId) {
        if (!enabled || namingService == null) {
            return instanceId;
        }
        try {
            for (Instance ins : namingService.getAllInstances(serviceName)) {
                if (ins.isHealthy() && instanceId.equals(ins.getMetadata().get(INSTANCE_ID_KEY))) {
                    return ins.getIp() + ":" + ins.getPort();
                }
            }
            log.warn("[nacos] 未找到 instanceId={} 的健康实例，回退为直接使用实例 ID", instanceId);
        } catch (Exception e) {
            log.error("[nacos] resolveTarget 失败，回退为直接使用实例 ID: {}", e.getMessage());
        }
        return instanceId;
    }

    /**
     * 立即注销本实例（优雅摘流第一步）：让 LB / 服务发现停止把新连接导进来。
     *
     * <p>只注销、不 shutdown：摘流后续还要用 namingService 周期上报吗？不需要，
     * 但保留连接可以避免摘流窗口内的任何查询异常。真正的 shutDown 在 {@link #destroy()}。
     */
    public void deregisterSelf() {
        if (namingService == null) {
            return;
        }
        try {
            namingService.deregisterInstance(serviceName, buildInstance());
            log.warn("[nacos] 已注销网关实例 {}（摘流开始，不再接收新连接）", addressResolver.localInstanceId());
        } catch (Exception e) {
            log.error("[nacos] 注销实例失败（摘流继续，靠就绪探针与 TTL 兜底）: {}", e.getMessage());
        }
    }

    /** 列出当前所有健康实例地址(ip:port)，供全集群广播使用 */
    public List<String> listHealthyTargets() {
        if (!enabled || namingService == null) {
            return List.of();
        }
        try {
            return namingService.getAllInstances(serviceName).stream()
                    .filter(Instance::isHealthy)
                    .map(i -> i.getIp() + ":" + i.getPort())
                    .toList();
        } catch (Exception e) {
            log.error("[nacos] listHealthyTargets 失败: {}", e.getMessage());
            return List.of();
        }
    }

    @PreDestroy
    public void destroy() {
        if (namingService == null) {
            return;
        }
        try {
            if (instanceListener != null) {
                namingService.unsubscribe(serviceName, instanceListener);
            }
            namingService.deregisterInstance(serviceName, buildInstance());
            namingService.shutDown();
            log.info("[nacos] 已注销本网关实例并取消订阅");
        } catch (Exception e) {
            log.error("[nacos] 注销失败: {}", e.getMessage());
        }
    }
}
