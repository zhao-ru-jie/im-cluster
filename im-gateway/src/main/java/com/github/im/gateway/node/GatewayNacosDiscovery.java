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
 *   <li>IM 服务本身不注册到 Nacos，网关通过 K8s Service / 静态地址调用 IM 服务的
 *       {@code MessageIngress}（IM 服务是无状态应用，普通 Service 负载均衡即可）。</li>
 * </ul>
 *
 * <p><b>不健康监听</b>：订阅实例变更，diff 出掉队的实例并调用
 * {@link GatewayRegistry#removeInstanceBindings(String)} 清理 Redis 路由，
 * 避免窗口期内消息持续推往死实例。清理动作发生在其它健康实例上（死实例自己已无法执行）。
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

            for (String gone : new HashSet<>(healthyInstanceIds)) {
                if (nowHealthy.contains(gone)) {
                    continue;
                }
                if (gone.equals(addressResolver.localInstanceId())) {
                    healthyInstanceIds.remove(gone);
                    continue;
                }
                log.warn("[nacos] 网关实例 {} 不再健康，清理 Redis 中该实例的用户路由", gone);
                try {
                    gatewayRegistry.removeInstanceBindings(gone);
                } catch (Exception ex) {
                    log.error("[nacos] 清理实例 {} 的 Redis 路由失败: {}", gone, ex.getMessage());
                }
                healthyInstanceIds.remove(gone);
            }
            healthyInstanceIds.addAll(nowHealthy);
        };
        namingService.subscribe(serviceName, instanceListener);
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
