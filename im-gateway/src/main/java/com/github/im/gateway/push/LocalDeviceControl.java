package com.github.im.gateway.push;

import com.github.im.contract.DeviceControlPort;
import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import com.github.im.proto.push.v1.KickResult;
import com.github.im.proto.push.v1.KickTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link DeviceControlPort} 的网关侧实现：在本实例上找连接并关掉。
 *
 * <p>两种调用来源最终都落到这里：本进程内的 IM 服务（单体）、
 * 以及其它实例/服务经 gRPC {@code DownstreamPush.Kick} 发来的请求（{@link DownstreamPushGrpcService}）。
 *
 * <h2>为什么先发帧再关连接</h2>
 *
 * <p>顺序反了客户端只会看到「连接莫名其妙断了」，于是按网络故障重连——被踢的设备立刻又回来，
 * 踢了个寂寞。先写 {@code session.kick} 再 close，正常客户端能拿到理由并停止重连；
 * 恶意客户端忽略这帧也没关系，TCP 连接是真的被关掉了（这才是踢下线的实质）。
 *
 * <p>Netty 侧无需额外同步：{@code writeAndFlush} 与 {@code close} 都投递到同一个 event loop
 * 队列，按提交顺序执行，所以帧一定在关闭前刷出去。
 *
 * <h2>设备寻址</h2>
 *
 * <p>{@code deviceId} 与 {@code exceptDeviceId} 的语义互斥：前者是「只踢这台」，
 * 后者是「除了这台都踢」（设置页里的「退出其他设备」）。两者都给时以 {@code deviceId} 为准——
 * 同时表达「只踢 A」和「除了 B 都踢」没有确定含义，猜一个不如明确一个。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class LocalDeviceControl implements DeviceControlPort {

    private static final Logger log = LoggerFactory.getLogger(LocalDeviceControl.class);

    /** 与 {@link ConnectionManager} 里「没带 deviceId 的客户端」同一套槽位命名 */
    private static final String DEFAULT_DEVICE = "default";

    private final ConnectionManager connectionManager;
    private final ClientFrames frames;

    public LocalDeviceControl(ConnectionManager connectionManager, ClientFrames frames) {
        this.connectionManager = connectionManager;
        this.frames = frames;
    }

    @Override
    public KickResult kick(KickTask task) {
        String userId = task.getUserId();
        if (userId == null || userId.isBlank()) {
            return KickResult.getDefaultInstance();
        }
        String only = blankToNull(task.getDeviceId());
        String except = blankToNull(task.getExceptDeviceId());
        String reason = task.getReason() == null || task.getReason().isBlank()
                ? "kicked-by-server" : task.getReason();

        List<String> kicked = new ArrayList<>();
        for (ClientSession session : connectionManager.sessionsOf(userId)) {
            String device = deviceKeyOf(session);
            if (only != null) {
                if (!only.equals(device)) {
                    continue;
                }
            } else if (except != null && except.equals(device)) {
                // 「退出其他设备」：当前这台留着
                continue;
            }
            if (!session.isOpen()) {
                // 已经断了：不计入 kicked，但也不算失败——目标状态已经达成
                continue;
            }
            connectionManager.sendTo(userId, session, frames.kickFrame(device, reason));
            connectionManager.close(userId, session, "kicked:" + reason);
            kicked.add(device);
        }
        if (!kicked.isEmpty()) {
            log.info("[device] 踢下线 user={} devices={} reason={}", userId, kicked, reason);
        }
        return KickResult.newBuilder()
                .setKicked(kicked.size())
                .addAllDeviceIds(kicked)
                .build();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static String deviceKeyOf(ClientSession session) {
        String deviceId = session.deviceId();
        return deviceId == null || deviceId.isBlank() ? DEFAULT_DEVICE : deviceId;
    }
}
