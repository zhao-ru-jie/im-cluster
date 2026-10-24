package com.github.im.gateway.push;

import com.github.im.contract.PushPort;
import com.github.im.gateway.node.GatewayNacosDiscovery;
import com.github.im.gateway.session.GatewayRegistry;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * {@link PushPort} 的网关侧实现：把「推给这些 userId」翻译成「本实例直接推 / 跨实例 gRPC 推」。
 *
 * <p>这是拆分后 IM 服务与连接状态的唯一交点：IM 服务只依赖 {@link PushPort} 接口，
 * 完全不知道在线路由表、gRPC channel、会话表的存在。
 *
 * <p>失败处理原则：<b>永不抛异常给 IM 服务</b>，一律转成 {@code failed_user_ids}，
 * 由 IM 服务走离线兜底——可丢实时性，不可丢消息。
 *
 * <p><b>本地同步、跨实例并行</b>：本地推送只是一次内存查表 + {@code writeAndFlush}，
 * 没有 RTT，串行做反而更省；跨实例的 gRPC 逐个 await 会把各实例 RTT 相加
 * （有实例慢/超时时最坏是 N × 超时），所以改成「先全部提交、再统一收集」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class GatewayPushPort implements PushPort {

    private static final Logger log = LoggerFactory.getLogger(GatewayPushPort.class);

    private final GatewayRegistry registry;
    private final GatewayNacosDiscovery nacosDiscovery;
    private final LocalSessionPusher localPusher;
    private final GatewayPushClient pushClient;

    /** 跨实例扇出执行器：全是阻塞等 gRPC，虚拟线程，数量受「一次任务涉及的实例数」约束 */
    private final ExecutorService fanout = Executors.newVirtualThreadPerTaskExecutor();

    public GatewayPushPort(GatewayRegistry registry,
                          GatewayNacosDiscovery nacosDiscovery,
                          LocalSessionPusher localPusher,
                          GatewayPushClient pushClient) {
        this.registry = registry;
        this.nacosDiscovery = nacosDiscovery;
        this.localPusher = localPusher;
        this.pushClient = pushClient;
    }

    @Override
    public PushResult push(PushTask task) {
        List<String> targets = task.getTargetUserIdsList();
        Set<String> delivered = new LinkedHashSet<>();

        // 按「目标网关实例」分组：同一次任务可能涉及多个实例（群聊扇出、多端跨实例）
        Map<String, List<String>> byInstance = new LinkedHashMap<>();
        for (String userId : targets) {
            // 路由表里没有 = 视为离线；真正的离线存储在 IM 服务侧。
            // 多端时一个用户可能同时在多个实例上，必须每个实例都推。
            for (String instance : registry.instancesOf(userId)) {
                byInstance.computeIfAbsent(instance, key -> new ArrayList<>()).add(userId);
            }
        }

        // 先提交跨实例任务，再处理本地：所有实例的等待时间重叠而不是相加
        List<Future<PushResult>> remoteFutures = new ArrayList<>(byInstance.size());
        for (Map.Entry<String, List<String>> entry : byInstance.entrySet()) {
            String instance = entry.getKey();
            List<String> users = List.copyOf(entry.getValue());
            PushTask subTask = task.toBuilder()
                    .clearTargetUserIds()
                    .addAllTargetUserIds(users)
                    .build();
            if (instance.equals(registry.localInstanceId())) {
                // 本地：内存查表 + writeAndFlush，同步做比切换线程便宜
                delivered.addAll(localPusher.push(subTask).getDeliveredUserIdsList());
            } else {
                remoteFutures.add(fanout.submit(() -> pushRemote(instance, subTask)));
            }
        }
        for (Future<PushResult> future : remoteFutures) {
            try {
                PushResult result = future.get();
                if (result != null) {
                    delivered.addAll(result.getDeliveredUserIdsList());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("cross-instance push interrupted, those users will be treated as offline");
            } catch (ExecutionException e) {
                // pushRemote 自己已 catch 所有异常并返回 null，这里只可能是 Error
                Throwable cause = e.getCause() == null ? e : e.getCause();
                log.warn("cross-instance push failed: {}", cause.getMessage());
            }
        }

        // failed = 目标 − 已投递：多端时「手机推到、网页所在实例推失败」不该让这个人再收一份离线
        List<String> failed = new ArrayList<>();
        for (String userId : targets) {
            if (!delivered.contains(userId)) {
                failed.add(userId);
            }
        }
        return PushResult.newBuilder()
                .addAllDeliveredUserIds(delivered)
                .addAllFailedUserIds(failed)
                .build();
    }

    /** 跨实例推一次；<b>永不抛异常</b>，失败返回 {@code null}（由调用方算作 failed） */
    private PushResult pushRemote(String instance, PushTask task) {
        String address = nacosDiscovery.resolveTarget(instance);
        try {
            return pushClient.push(address, task);
        } catch (Exception e) {
            log.warn("push to gateway {} (resolved {}) failed: {}", instance, address, e.getMessage());
            return null;
        }
    }

    @PreDestroy
    public void shutdown() {
        fanout.shutdown();
        log.info("[push] 跨实例扇出执行器已停止");
    }
}
