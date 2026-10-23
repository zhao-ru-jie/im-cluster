package com.github.im.gateway.push;

import com.github.im.contract.PushPort;
import com.github.im.gateway.node.GatewayNacosDiscovery;
import com.github.im.gateway.session.GatewayRegistry;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link PushPort} 的网关侧实现：把「推给这些 userId」翻译成「本实例直接推 / 跨实例 gRPC 推」。
 *
 * <p>这是拆分后 IM 服务与连接状态的唯一交点：IM 服务只依赖 {@link PushPort} 接口，
 * 完全不知道在线路由表、gRPC channel、会话表的存在。
 *
 * <p>失败处理原则：<b>永不抛异常给 IM 服务</b>，一律转成 {@code failed_user_ids}，
 * 由 IM 服务走离线兜底——可丢实时性，不可丢消息。
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
        List<String> delivered = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        // 按「目标网关实例」分组：同一次任务可能涉及多个实例（群聊扇出、多端跨实例）
        Map<String, List<String>> byInstance = new LinkedHashMap<>();
        for (String userId : task.getTargetUserIdsList()) {
            String instance = registry.instanceOf(userId);
            if (instance == null) {
                // 路由表里没有 = 视为离线；真正的离线存储在 IM 服务侧
                failed.add(userId);
                continue;
            }
            byInstance.computeIfAbsent(instance, key -> new ArrayList<>()).add(userId);
        }

        for (Map.Entry<String, List<String>> entry : byInstance.entrySet()) {
            String instance = entry.getKey();
            List<String> users = entry.getValue();
            PushTask subTask = task.toBuilder()
                    .clearTargetUserIds()
                    .addAllTargetUserIds(users)
                    .build();

            PushResult result;
            if (instance.equals(registry.localInstanceId())) {
                result = localPusher.push(subTask);
            } else {
                result = pushRemote(instance, subTask);
            }

            if (result == null) {
                failed.addAll(users);
            } else {
                delivered.addAll(result.getDeliveredUserIdsList());
                failed.addAll(result.getFailedUserIdsList());
            }
        }

        return PushResult.newBuilder()
                .addAllDeliveredUserIds(delivered)
                .addAllFailedUserIds(failed)
                .build();
    }

    private PushResult pushRemote(String instance, PushTask task) {
        String address = nacosDiscovery.resolveTarget(instance);
        try {
            return pushClient.push(address, task);
        } catch (Exception e) {
            log.warn("push to gateway {} (resolved {}) failed: {}", instance, address, e.getMessage());
            return null;
        }
    }
}
