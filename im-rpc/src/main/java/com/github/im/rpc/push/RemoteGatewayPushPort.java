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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * {@link PushPort} 的远程实现：IM 服务 → 各网关实例（gRPC {@code DownstreamPush}）。
 *
 * <p>这是「IM 服务独立部署」时唯一的下行通道，替代了阶段 1 单体里的
 * {@code GatewayPushPort}（那个实现能直接摸到本机会话表，拆分后不再适用）。
 *
 * <p>处理流程：
 * <ol>
 *   <li>读在线路由表，把 userId 按目标网关实例分组（一次任务可能跨多个实例，
 *       多端时同一个 userId 会进入多个实例的分组）；</li>
 *   <li><b>并行</b>向各实例 gRPC 推送，收集 delivered；</li>
 *   <li>{@code failed = 目标 − delivered}（不能直接汇总各实例的 failed，否则多端下
 *       一个用户会同时出现在两份列表里，导致已收到消息的人被再排一份离线）；</li>
 *   <li>路由缺失、实例不可达、超时一律记 failed —— <b>本类永不抛异常</b>，
 *       由 IM 服务统一落离线，保证「可丢实时性、不可丢消息」。</li>
 * </ol>
 *
 * <p><b>为什么并行</b>：串行时一次群聊扇出要等「实例数 × gRPC RTT」，跨可用区（RTT 1ms 起）
 * 或者有实例慢/超时时，延迟会按实例数累加（最坏 N × 800ms）。改成并行后总耗时 ≈ 最慢的那个实例，
 * 而 {@code failed} 的语义完全不变。各实例的 {@code get()} 仍在所有任务都已提交之后才收集，
 * 所以不会退化成串行。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RemoteGatewayPushPort implements PushPort {

    private static final Logger log = LoggerFactory.getLogger(RemoteGatewayPushPort.class);

    private final GatewayRouteReader routeReader;
    private final GrpcChannelFactory channels;
    private final AddressResolver addressResolver;
    private final long timeoutMs;
    private final boolean waitForReady;

    /**
     * 跨实例扇出的执行器。虚拟线程：这里全是阻塞等 gRPC 的活，一条任务一个虚拟线程最直白；
     * 数量天然受「一次任务涉及的实例数」限制，不会无限膨胀。
     */
    private final ExecutorService fanout = Executors.newVirtualThreadPerTaskExecutor();

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
        List<String> targets = task.getTargetUserIdsList();
        Set<String> delivered = new LinkedHashSet<>();

        // 按「目标网关实例」分组。多端下同一个 userId 会出现在多个实例的分组里，
        // 这是对的：每个实例负责把它本机上的设备都推一遍。
        // 一次批量查完整组人的路由（分片 pipeline + 并发）：
        // 5000 人群原本要 5000 次串行往返（1~2.5s），现在 10 次分片往返、10ms 量级。
        Map<String, Set<String>> routes = routeReader.instancesOfAll(targets);
        Map<String, List<String>> byInstance = new LinkedHashMap<>();
        for (String userId : targets) {
            Set<String> instances = routes.getOrDefault(userId, Set.of());
            if (instances.isEmpty()) {
                continue;   // 不在线：最后统一算作 failed
            }
            for (String instanceId : sorted(instances)) {
                byInstance.computeIfAbsent(instanceId, _ -> new ArrayList<>()).add(userId);
            }
        }

        // 先全部提交再收集结果：任何一个实例慢/超时都不该拖住其它实例的发起
        List<Future<List<String>>> futures = new ArrayList<>(byInstance.size());
        for (Map.Entry<String, List<String>> entry : byInstance.entrySet()) {
            String instance = entry.getKey();
            List<String> users = List.copyOf(entry.getValue());
            futures.add(fanout.submit(() -> pushOne(instance, users, task)));
        }
        for (Future<List<String>> future : futures) {
            try {
                delivered.addAll(future.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("push interrupted, remaining instances treated as offline");
            } catch (ExecutionException e) {
                // pushOne 自己已经 catch 了所有异常并返回空列表，走到这里说明是 Error
                Throwable cause = e.getCause() == null ? e : e.getCause();
                log.warn("push task failed: {}", cause.getMessage());
            }
        }

        // failed 必须由「目标 − 已投递」算出来，不能直接汇总各实例的 failed：
        // 多端时某用户的手机推成功、网页所在实例推失败，合并两份列表会让他同时出现在
        // delivered 和 failed 里，IM 服务就会给一个已经收到消息的人再排一份离线。
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

    /** 推给一个实例；<b>永不抛异常</b>，失败返回空列表（由调用方算作 failed） */
    private List<String> pushOne(String instanceId, List<String> users, PushTask task) {
        String address = addressResolver.resolve(instanceId);
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
            return stub.push(subTask).getDeliveredUserIdsList();
        } catch (Exception e) {
            log.warn("push to gateway {} (address {}) failed, fallback to offline: {}",
                    instanceId, address, e.getMessage());
            return List.of();
        }
    }

    /** 实例顺序稳定一下：便于排障时对照日志，也让子任务顺序可复现 */
    private static List<String> sorted(Set<String> instances) {
        List<String> list = new ArrayList<>(instances);
        Collections.sort(list);
        return list;
    }
}
