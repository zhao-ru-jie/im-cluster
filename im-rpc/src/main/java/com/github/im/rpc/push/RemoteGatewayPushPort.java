package com.github.im.rpc.push;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.DownstreamPushGrpc;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.rpc.common.AddressResolver;
import com.github.im.rpc.common.GrpcChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * {@link PushPort} 的远程实现：IM 服务 → 各网关实例（gRPC {@code DownstreamPush}）。
 *
 * <p>这是「IM 服务独立部署」时唯一的下行通道，替代了阶段 1 单体里的
 * {@code GatewayPushPort}（那个实现能直接摸到本机会话表，拆分后不再适用）。
 *
 * <p>处理流程：
 * <ol>
 *   <li>读在线路由表，把 userId 按目标网关实例分组（一次任务可能跨多个实例）；</li>
 *   <li>逐实例 gRPC 推送，收集 delivered / failed；</li>
 *   <li>路由缺失、实例不可达、超时一律记 failed —— <b>本类永不抛异常</b>，
 *       由 IM 服务统一落离线，保证「可丢实时性、不可丢消息」。</li>
 * </ol>
 */
public class RemoteGatewayPushPort implements PushPort {

    private static final Logger log = LoggerFactory.getLogger(RemoteGatewayPushPort.class);

    private final GatewayRouteReader routeReader;
    private final GrpcChannelFactory channels;
    private final AddressResolver addressResolver;
    private final long timeoutMs;
    private final boolean waitForReady;

    /**
     * @param waitForReady 是否等待目标实例就绪（{@code im.push.wait-for-ready}，默认 {@code false}）。
     *                     下行不建议打开：本方向有「失败即落离线、客户端上线补发」的确定性兜底，
     *                     让 IM 服务把线程挂在「等网关就绪」上没有收益，反而会拖慢受理吞吐。
     */
    public RemoteGatewayPushPort(GatewayRouteReader routeReader,
                                 GrpcChannelFactory channels,
                                 AddressResolver addressResolver,
                                 long timeoutMs,
                                 boolean waitForReady) {
        this.routeReader = routeReader;
        this.channels = channels;
        this.addressResolver = addressResolver;
        this.timeoutMs = timeoutMs;
        this.waitForReady = waitForReady;
    }

    @Override
    public PushResult push(PushTask task) {
        List<String> delivered = new ArrayList<>();
        List<String> failed = new ArrayList<>();

        Map<String, List<String>> byInstance = new LinkedHashMap<>();
        for (String userId : task.getTargetUserIdsList()) {
            String instanceId = routeReader.instanceOf(userId);
            if (instanceId == null) {
                failed.add(userId);
                continue;
            }
            byInstance.computeIfAbsent(instanceId, _ -> new ArrayList<>()).add(userId);
        }

        for (Map.Entry<String, List<String>> entry : byInstance.entrySet()) {
            String address = addressResolver.resolve(entry.getKey());
            List<String> users = entry.getValue();
            PushTask subTask = task.toBuilder()
                    .clearTargetUserIds()
                    .addAllTargetUserIds(users)
                    .build();
            try {
                DownstreamPushGrpc.DownstreamPushBlockingStub stub =
                        DownstreamPushGrpc.newBlockingStub(channels.channelFor(address))
                                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
                if (waitForReady) {
                    stub = stub.withWaitForReady();
                }
                PushResult result = stub.push(subTask);
                delivered.addAll(result.getDeliveredUserIdsList());
                failed.addAll(result.getFailedUserIdsList());
            } catch (Exception e) {
                log.warn("push to gateway {} (address {}) failed, fallback to offline: {}",
                        entry.getKey(), address, e.getMessage());
                failed.addAll(users);
            }
        }

        return PushResult.newBuilder()
                .addAllDeliveredUserIds(delivered)
                .addAllFailedUserIds(failed)
                .build();
    }
}
