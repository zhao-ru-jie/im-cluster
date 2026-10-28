package com.github.im.rpc.push;

import com.github.im.contract.DeviceControlPort;
import com.github.im.proto.push.v1.DownstreamPushGrpc;
import com.github.im.proto.push.v1.KickResult;
import com.github.im.proto.push.v1.KickTask;
import com.github.im.rpc.common.AddressResolver;
import com.github.im.rpc.common.GrpcChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
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
 * {@link DeviceControlPort} 的远程实现：IM 服务 → 各网关实例（gRPC {@code DownstreamPush.Kick}）。
 *
 * <p>与 {@link RemoteGatewayPushPort} 同一套寻址方式（在线路由表分组 + 地址解析 + 并行），
 * 但语义相反：那边<b>可丢</b>（失败转离线），这边<b>不可丢</b>（要的是连接真的被关掉）。
 * 因此这里的失败必须如实上报给调用方，而不是像推送那样静默吞掉。
 *
 * <p><b>为什么要按实例扇出</b>：同一账号的多台设备可能分散在多个网关实例上，
 * 「退出其他设备」必须打到每一台持有该用户连接的实例；只打本机看不出问题，
 * 但换个部署形态就会「手机上退不掉网页」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RemoteDeviceControl implements DeviceControlPort {

    private static final Logger log = LoggerFactory.getLogger(RemoteDeviceControl.class);

    private final GatewayRouteReader routeReader;
    private final GrpcChannelFactory channels;
    private final AddressResolver addressResolver;
    private final long timeoutMs;

    private final ExecutorService fanout = Executors.newVirtualThreadPerTaskExecutor();

    public RemoteDeviceControl(GatewayRouteReader routeReader,
                               GrpcChannelFactory channels,
                               AddressResolver addressResolver,
                               long timeoutMs) {
        this.routeReader = routeReader;
        this.channels = channels;
        this.addressResolver = addressResolver;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public KickResult kick(KickTask task) {
        String userId = task.getUserId();
        if (userId == null || userId.isBlank()) {
            return KickResult.getDefaultInstance();
        }
        Set<String> instances = routeReader.instancesOf(userId);
        if (instances == null || instances.isEmpty()) {
            // 不在线：调用方按「目标状态已达成」处理，这里不制造错误
            return KickResult.getDefaultInstance();
        }
        List<String> ordered = new ArrayList<>(instances);
        Collections.sort(ordered);

        List<Future<KickResult>> futures = new ArrayList<>(ordered.size());
        for (String instance : ordered) {
            futures.add(fanout.submit(() -> kickOne(instance, task)));
        }
        int kicked = 0;
        Set<String> devices = new LinkedHashSet<>();
        for (Future<KickResult> future : futures) {
            try {
                KickResult result = future.get();
                if (result != null) {
                    kicked += result.getKicked();
                    devices.addAll(result.getDeviceIdsList());
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.warn("[device] 踢下线被中断，剩余实例未执行（user={}）", userId);
            } catch (ExecutionException e) {
                // kickOne 自己已吞掉异常，走到这里只可能是 Error
                Throwable cause = e.getCause() == null ? e : e.getCause();
                log.warn("[device] 踢下线子任务失败 user={} err={}", userId, cause.getMessage());
            }
        }
        return KickResult.newBuilder()
                .setKicked(kicked)
                .addAllDeviceIds(devices)
                .build();
    }

    /** 打一个实例；<b>永不抛异常</b>，失败返回空结果（调用方从 kicked=0 看出没踢到） */
    private KickResult kickOne(String instanceId, KickTask task) {
        String address = addressResolver.resolve(instanceId);
        try {
            return DownstreamPushGrpc.newBlockingStub(channels.channelFor(address))
                    .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                    .kick(task);
        } catch (Exception e) {
            log.warn("[device] 向网关 {}（{}）发踢下线失败: {}", instanceId, address, e.getMessage());
            return KickResult.getDefaultInstance();
        }
    }

    /** 供排障：把「这批 userId 会被分发到哪些实例」打出来（与推送侧同一份路由读法） */
    public Map<String, Set<String>> routesOf(List<String> userIds) {
        return routeReader.instancesOfAll(userIds);
    }
}
