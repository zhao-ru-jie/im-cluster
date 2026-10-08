package com.example.im.discovery;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.example.im.connection.NodeAddressResolver;
import com.example.im.connection.UserNodeRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 基于 Nacos-Client SDK 的「节点 gRPC 地址动态发现」。
 *
 * <p><b>职责边界</b>：本组件只解决「nodeId → 实时可达的 gRPC 地址(ip:port)」这一问题，
 * 且带有 Nacos 的健康探针（节点宕机自动从列表剔除）。
 * 而「某个 userId 连在哪个节点」这种 per-user 会话路由，仍由 Redis({@code im:user:node:{userId}})
 * 持有——Nacos 管不了 per-user 状态，二者职责不重叠。
 *
 * <p><b>不健康监听（核心新增）</b>：订阅 Nacos 的实例变更事件。当某节点被 Nacos 标记为
 * 不健康或下线，其它健康节点会 diff 出该「掉队」的 nodeId，并调用
 * {@link UserNodeRegistry#removeNodeBindings(String)} 把 Redis 中指向该死节点的
 * {@code im:user:node:{userId}} 路由缓存清掉——使这些用户被视为「离线」，后续消息不再转发到
 * 死节点，而是落入离线队列。节点恢复、用户重连后会重新注册路由。
 *
 * <p><b>与现有架构的关系</b>：
 * <ul>
 *   <li>节点 ID(nodeId) 仍由 {@link NodeAddressResolver} 自动生成（K8s 下为 POD_IP / POD_NAME，无需写死）。</li>
 *   <li>本组件在 Nacos 注册一个实例：ip = 本节点真实可达 IP，port = gRPC 端口，metadata.nodeId = nodeId。</li>
 *   <li>转发时 {@link #resolveTarget(String)} 查询 Nacos 健康实例，按 metadata.nodeId 匹配出最新 ip:port。</li>
 *   <li>未启用 Nacos({@code im.nacos.enabled=false})或 Nacos 不可用时，{@link #resolveTarget(String)} 直接返回
 *       nodeId 本身（即 host:port），行为与引入前完全一致——零侵入、可平滑回退。</li>
 * </ul>
 *
 * <p>只依赖 {@code nacos-client} 这一个轻量 SDK（不走 Spring Cloud Alibaba 全家桶），规避 Boot 4.1 兼容风险。
 */
@Component
public class NacosNodeDiscovery {

    private static final Logger log = LoggerFactory.getLogger(NacosNodeDiscovery.class);
    private static final String NODE_ID_KEY = "nodeId";

    private final boolean enabled;
    private final String serverAddr;
    private final String serviceName;
    private final String namespace;
    private final NodeAddressResolver addressResolver;
    private final UserNodeRegistry userNodeRegistry;

    private NamingService namingService;
    private EventListener instanceListener;
    /** 当前已知健康的 nodeId 集合，用于 diff 出「变不健康 / 已下线」的节点 */
    private final Set<String> healthyNodeIds = ConcurrentHashMap.newKeySet();

    public NacosNodeDiscovery(@Value("${im.nacos.enabled:false}") boolean enabled,
                              @Value("${im.nacos.server-addr:127.0.0.1:8848}") String serverAddr,
                              @Value("${im.nacos.service-name:im-grpc-nodes}") String serviceName,
                              @Value("${im.nacos.namespace:}") String namespace,
                              NodeAddressResolver addressResolver,
                              UserNodeRegistry userNodeRegistry) {
        this.enabled = enabled;
        this.serverAddr = serverAddr;
        this.serviceName = serviceName;
        this.namespace = namespace;
        this.addressResolver = addressResolver;
        this.userNodeRegistry = userNodeRegistry;
    }

    @PostConstruct
    public void init() {
        if (!enabled) {
            log.info("[nacos] disabled (im.nacos.enabled=false)，gRPC 目标地址解析回退为直接使用 nodeId");
            return;
        }
        try {
            Properties props = new Properties();
            props.put("serverAddr", serverAddr);
            if (namespace != null && !namespace.isBlank()) {
                props.put("namespace", namespace);
            }
            namingService = NacosFactory.createNamingService(props);
            registerSelf();
            subscribe();
            log.info("[nacos] 已连接 {}，注册服务 {}（nodeId={}, ip={}, port={}），并已订阅实例变更",
                    serverAddr, serviceName, addressResolver.localNodeId(),
                    addressResolver.getRegisterIp(), addressResolver.getPort());
        } catch (Exception e) {
            // 容错：Nacos 起不来不影响单机/同机 demo，转发回退到直接用 nodeId dial
            log.error("[nacos] 初始化失败，转发将回退为直接使用 nodeId: {}", e.getMessage());
            namingService = null;
        }
    }

    private Instance buildInstance() {
        Instance instance = new Instance();
        instance.setIp(addressResolver.getRegisterIp());
        instance.setPort(addressResolver.getPort());
        instance.setEphemeral(true); // 临时实例：断心跳后 Nacos 自动剔除
        instance.addMetadata(NODE_ID_KEY, addressResolver.localNodeId());
        instance.addMetadata("grpcHost", addressResolver.getHost());
        return instance;
    }

    private void registerSelf() throws Exception {
        namingService.registerInstance(serviceName, buildInstance());
    }

    /** 订阅 Nacos 实例变更，维护健康集合并清理掉队节点的 Redis 路由缓存 */
    private void subscribe() throws Exception {
        // 初始化当前健康集合
        for (Instance ins : namingService.getAllInstances(serviceName)) {
            String nodeId = nodeIdOf(ins);
            if (ins.isHealthy() && nodeId != null) {
                healthyNodeIds.add(nodeId);
            }
        }
        instanceListener = event -> {
            if (!(event instanceof NamingEvent ne)) {
                return;
            }
            Set<String> nowHealthy = ne.getInstances().stream()
                    .filter(Instance::isHealthy)
                    .map(this::nodeIdOf)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet());

            // 找出从健康集合掉出的节点（变不健康 或 已注销）：清理其在 Redis 中的用户路由缓存
            for (String gone : new HashSet<>(healthyNodeIds)) {
                if (nowHealthy.contains(gone)) {
                    continue;
                }
                // 不处理本节点自身，避免误删本地在线用户的路由（本地仍连着、只是 Nacos 视角异常时）
                if (gone.equals(addressResolver.localNodeId())) {
                    healthyNodeIds.remove(gone);
                    continue;
                }
                log.warn("[nacos] 节点 {} 不再健康，清理 Redis 中该节点的用户路由缓存", gone);
                try {
                    userNodeRegistry.removeNodeBindings(gone);
                } catch (Exception ex) {
                    log.error("[nacos] 清理节点 {} 的 Redis 路由缓存失败: {}", gone, ex.getMessage());
                }
                healthyNodeIds.remove(gone);
            }
            healthyNodeIds.addAll(nowHealthy);
        };
        namingService.subscribe(serviceName, instanceListener);
    }

    private String nodeIdOf(Instance ins) {
        Map<String, String> md = ins.getMetadata();
        return md == null ? null : md.get(NODE_ID_KEY);
    }

    /**
     * 把节点 ID 解析成实时可达的 gRPC 目标地址(ip:port)。
     * 优先从 Nacos 查询「健康」实例；Nacos 不可用 / 未启用 / 匹配不到时，直接返回 nodeId 兜底。
     */
    public String resolveTarget(String nodeId) {
        if (!enabled || namingService == null) {
            return nodeId;
        }
        try {
            List<Instance> instances = namingService.getAllInstances(serviceName);
            for (Instance ins : instances) {
                if (ins.isHealthy() && nodeId.equals(ins.getMetadata().get(NODE_ID_KEY))) {
                    return ins.getIp() + ":" + ins.getPort();
                }
            }
            log.warn("[nacos] 未找到 nodeId={} 的健康实例，回退为直接使用 nodeId", nodeId);
        } catch (Exception e) {
            log.error("[nacos] resolveTarget 失败，回退为直接使用 nodeId: {}", e.getMessage());
        }
        return nodeId;
    }

    /** 列出当前所有健康节点地址(ip:port)，供全集群广播使用 */
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
            log.info("[nacos] 已注销本节点实例并取消订阅");
        } catch (Exception e) {
            log.error("[nacos] 注销失败: {}", e.getMessage());
        }
    }
}
