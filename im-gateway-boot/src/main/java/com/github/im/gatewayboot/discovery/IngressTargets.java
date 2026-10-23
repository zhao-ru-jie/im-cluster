package com.github.im.gatewayboot.discovery;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.naming.NamingService;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.Instance;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

/**
 * 上行目标（IM 服务）的地址来源：订阅 Nacos 里 {@code im-server} 的健康实例，
 * 把「当前有哪些地址」推给需要的人（实际是 {@link IngressNameResolver}，再交给 gRPC 的
 * {@code round_robin} 去做轮询）。
 *
 * <h2>职责边界</h2>
 *
 * <p>本类<b>只回答「有哪些地址」</b>，不选地址、不缓存 channel、不做健康剔除——这些都在 gRPC 里：
 * <ul>
 *   <li>轮询：channel 上的 {@code round_robin} 策略，工作在 subchannel 层；</li>
 *   <li>故障剔除：subchannel 连接失败进入 {@code TRANSIENT_FAILURE} 后自动退出轮询，
 *       恢复后自动放回（比应用层计数器更准确，也不会和应用层重试叠加）。</li>
 * </ul>
 *
 * <p>为什么不让应用层选地址：取址若放在「每次 RPC」这一层，全局计数器的周期会和
 * 「每个业务请求的 RPC 次数」产生别名（2 实例 + 每条消息 2 次 RPC 时 submit 永远落同一个），
 * 随机虽然能避开，但分布只是概率性的；交给 gRPC 的 LoadBalancer 既准确又不丢健康语义。
 *
 * <h2>降级</h2>
 *
 * <p>Nacos 关闭 / 初始化失败 / 拿不到健康实例时，返回 {@code im.server.address}
 * （支持逗号分隔多地址，同样参与 {@code round_robin}）。保证「没有注册中心也能单机跑起来」
 * 这条底线不被破坏。
 *
 * <h2>与 {@code GatewayNacosDiscovery} 的分工</h2>
 *
 * <p>两者方向相反、互不替代：
 * <ul>
 *   <li>{@code GatewayNacosDiscovery}（im-gateway）：<b>注册自己</b>（service=im-gateway）
 *       并订阅同服务实例，服务于「IM 服务 → 网关」的下行推送寻址；</li>
 *   <li>本类：<b>不注册</b>，只订阅别人（service=im-server），服务于「网关 → IM 服务」的上行寻址。</li>
 * </ul>
 */
public class IngressTargets {

    private static final Logger log = LoggerFactory.getLogger(IngressTargets.class);

    private final boolean enabled;
    private final String serverAddr;
    private final String namespace;
    private final String serviceName;
    /** 降级地址（Nacos 不可用时使用）：逗号分隔，至少一个 */
    private final List<String> fallback;

    /** Nacos 报上来的健康实例；空表示「发现不可用」，此时用 fallback */
    private volatile List<String> discovered = List.of();
    /** 地址列表变更的订阅者（每个 gRPC channel 的 NameResolver 一个） */
    private final List<Consumer<List<String>>> listeners = new CopyOnWriteArrayList<>();

    private NamingService namingService;
    private EventListener instanceListener;

    public IngressTargets(boolean enabled,
                          String serverAddr,
                          String namespace,
                          String serviceName,
                          String fallbackAddresses) {
        this.enabled = enabled;
        this.serverAddr = serverAddr;
        this.namespace = namespace;
        this.serviceName = serviceName;
        this.fallback = parse(fallbackAddresses);
        if (this.fallback.isEmpty()) {
            throw new IllegalArgumentException("at least one ingress address is required (im.server.address)");
        }
    }

    private static List<String> parse(String addresses) {
        if (addresses == null) {
            return List.of();
        }
        return Arrays.stream(addresses.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .toList();
    }

    @PostConstruct
    public void init() {
        if (!enabled) {
            log.info("[nacos] 上行地址发现未启用（im.nacos.enabled=false），固定使用 im.server.address={}", fallback);
            return;
        }
        try {
            Properties props = new Properties();
            props.put("serverAddr", serverAddr);
            if (namespace != null && !namespace.isBlank()) {
                props.put("namespace", namespace);
            }
            namingService = NacosFactory.createNamingService(props);
            refresh(namingService.getAllInstances(serviceName));
            instanceListener = event -> {
                if (event instanceof NamingEvent namingEvent) {
                    refresh(namingEvent.getInstances());
                }
            };
            namingService.subscribe(serviceName, instanceListener);
            log.info("[nacos] 已订阅上行服务 {}，当前可用地址: {}", serviceName, current());
        } catch (Exception e) {
            // 容错：Nacos 起不来不影响网关工作，退化为静态地址
            log.error("[nacos] 上行地址发现初始化失败，回退为 im.server.address={}: {}", fallback, e.getMessage());
            namingService = null;
            discovered = List.of();
        }
    }

    /** 当前应该用于连接的地址：有发现结果就用发现结果，否则用静态降级地址 */
    public List<String> current() {
        List<String> targets = discovered;
        return targets.isEmpty() ? fallback : targets;
    }

    /** 订阅地址变更：注册时立刻推一次当前值，之后每次变更推一次 */
    public void addListener(Consumer<List<String>> listener) {
        listeners.add(listener);
        listener.accept(current());
    }

    public void removeListener(Consumer<List<String>> listener) {
        listeners.remove(listener);
    }

    private void refresh(List<Instance> instances) {
        if (instances == null) {
            return;
        }
        List<String> now = instances.stream()
                .filter(Objects::nonNull)
                .filter(Instance::isHealthy)
                .filter(Instance::isEnabled)
                .map(ins -> ins.getIp() + ":" + ins.getPort())
                .distinct()
                .sorted()
                .toList();
        if (now.equals(discovered)) {
            return;
        }
        discovered = now;
        List<String> effective = current();
        log.info("[nacos] 上行地址变更: {}（Nacos 健康实例 {}）", effective, now.isEmpty() ? "无，已回退静态地址" : now);
        // 推给各 channel 的 NameResolver：它把结果交给 gRPC，round_robin 会自行增减 subchannel
        listeners.forEach(listener -> {
            try {
                listener.accept(effective);
            } catch (Exception e) {
                log.warn("[nacos] 推送上行地址变更失败: {}", e.getMessage());
            }
        });
    }

    /** 当前是否真的在用 Nacos 的实例列表（排障用） */
    public boolean discovering() {
        return !discovered.isEmpty();
    }

    public String serviceName() {
        return serviceName;
    }

    /**
     * 关闭订阅。
     *
     * <p>由 {@code @PreDestroy} 驱动。先把 {@code namingService} 置空，保证
     * 「未启用 / 初始化失败 / 重复调用」都是安静的 no-op。
     */
    @PreDestroy
    public void destroy() {
        NamingService service = namingService;
        namingService = null;
        if (service == null) {
            return;
        }
        try {
            if (instanceListener != null) {
                service.unsubscribe(serviceName, instanceListener);
            }
            service.shutDown();
            log.info("[nacos] 已取消上行服务 {} 的订阅", serviceName);
        } catch (Exception e) {
            log.error("[nacos] 取消上行订阅失败: {}", e.getMessage());
        }
    }
}
