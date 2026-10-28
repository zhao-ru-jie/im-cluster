package com.github.im.server.device;

import com.github.im.contract.DeviceControlPort;
import com.github.im.contract.PushPort;
import com.github.im.proto.presence.v1.DeviceAck;
import com.github.im.proto.presence.v1.DeviceReport;
import com.github.im.proto.push.v1.KickResult;
import com.github.im.proto.push.v1.KickTask;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 多端设备管理：设备台账、多端登录通知、踢下线。
 *
 * <h2>为什么「设备列表」不在网关侧实现</h2>
 *
 * <p>网关知道「本机有哪些连接」，但设备管理要回答的是「这个账号一共有几台设备、各自上次怎么登的」——
 * 这是<b>跨实例</b>的事实，只有把台账写在中心存储（Redis）里、由 IM 服务统一维护才自洽。
 * 网关只负责在连接建立/断开时上报事实（{@code ReportDevice}），不做聚合。
 *
 * <h2>登录通知为什么推给「整台账号」而不是「其它设备」</h2>
 *
 * <p>推送粒度只到 userId（见 {@code PushTask}），要在链路里加设备级排除，得让每个实例都理解
 * 「排除哪台」，收益只是省掉一条客户端本来就会忽略的帧。所以帧里带 {@code deviceId}，
 * 由客户端按「不是我自己」过滤——这条约定写进了 {@link ClientFrames#TYPE_SESSION_LOGIN}。
 *
 * <h2>踢下线为什么可能「成功但没踢到」</h2>
 *
 * <p>设备本来就不在线时，业务目标是<b>已经达成</b>的（它不在线了），所以回 {@code code=0}
 * 且 {@code kicked=0}，而不是给一个 404 让人去猜「是设备不存在还是没连上」。
 * 台账里的 online 位照样会被清掉——那正是这个接口要维护的不变量。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Service
public class DeviceService {

    private static final Logger log = LoggerFactory.getLogger(DeviceService.class);

    /** 登录通知的 TTL：半小时后再送达的「有新设备登录」已经没有意义 */
    private static final int NOTIFY_TTL_MS = 30_000;

    private final DeviceStore store;
    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final ObjectProvider<DeviceControlPort> controlProvider;
    private final boolean enabled;

    public DeviceService(DeviceStore store,
                         ClientFrames frames,
                         ObjectProvider<PushPort> pushPortProvider,
                         ObjectProvider<DeviceControlPort> controlProvider,
                         @Value("${im.device.enabled:true}") boolean enabled) {
        this.store = store;
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.controlProvider = controlProvider;
        this.enabled = enabled;
        log.info("[device] 多端设备管理{}（台账 + 登录通知 + 踢下线）", enabled ? "已开启" : "已关闭");
    }

    /**
     * 处理网关上报的设备登录/下线事件。
     *
     * <p>登录：写台账 + 给这个账号推一条 {@code session.login}（客户端据此提示
     * 「你的账号在新设备登录了」）；下线：只把台账的 online 位置 0。
     */
    public DeviceAck report(DeviceReport report) {
        String userId = report.getUserId();
        if (!enabled || userId == null || userId.isBlank()) {
            return DeviceAck.newBuilder().setCode(0).setMessage("ignored").build();
        }
        String deviceId = report.getDeviceId().isBlank() ? "default" : report.getDeviceId();
        long ts = report.getTs() > 0 ? report.getTs() : System.currentTimeMillis();
        if (!report.getLogin()) {
            store.recordLogout(userId, deviceId);
            return DeviceAck.newBuilder().setCode(0).setMessage("logout").build();
        }
        store.recordLogin(userId, deviceId, report.getTransport(), report.getGatewayInstance(), ts);
        int notified = notifyLogin(userId, deviceId, report.getTransport(), ts);
        return DeviceAck.newBuilder()
                .setCode(0)
                .setMessage("login")
                .setNotified(notified)
                .build();
    }

    /** 设备列表（在线优先、最近登录优先） */
    public List<DeviceStore.DeviceRow> list(String userId) {
        return store.devicesOf(userId);
    }

    /**
     * 踢下线。
     *
     * @param deviceId       只踢这台；为空表示「按 {@code exceptDeviceId} 决定」
     * @param exceptDeviceId 退出其他设备时保留的那台
     */
    public KickOutcome kick(String userId, String deviceId, String exceptDeviceId, String reason) {
        String target = blankToNull(deviceId);
        String keep = blankToNull(exceptDeviceId);
        if (userId == null || userId.isBlank()) {
            return KickOutcome.rejected("userId is required");
        }
        if (target == null && keep == null) {
            // 两个都空 = 「踢掉全部设备」：这会把用户所有端直接踹下线，
            // 只应该由「封号」这类动作触发，不该出现在设备管理的默认路径上
            return KickOutcome.rejected("deviceId or exceptDeviceId is required");
        }
        DeviceControlPort control = controlProvider == null ? null : controlProvider.getIfAvailable();
        if (control == null || !control.available()) {
            // 如实报告「控制通道不可用」，而不是回一个成功让调用方以为已经踢掉了
            return KickOutcome.rejected("device control is not available in this deployment");
        }
        KickResult result;
        try {
            result = control.kick(KickTask.newBuilder()
                    .setUserId(userId)
                    .setDeviceId(target == null ? "" : target)
                    .setExceptDeviceId(keep == null ? "" : keep)
                    .setReason(reason == null || reason.isBlank() ? "kicked-by-server" : reason)
                    .build());
        } catch (RuntimeException e) {
            log.warn("[device] 踢下线调用失败 user={} device={} err={}", userId, target, e.getMessage());
            return KickOutcome.rejected("kick failed: " + e.getMessage());
        }
        if (result == null || result.getKicked() == 0) {
            // 不在线：目标状态已达成。台账仍按「已离线」写一遍，避免残留 online=1 让列表显示错
            store.markOffline(userId, target != null ? List.of(target) : List.of());
            return new KickOutcome(0, "device is not online", 0, List.of(), true);
        }
        store.markOffline(userId, result.getDeviceIdsList());
        log.info("[device] 踢下线 user={} devices={} reason={}", userId, result.getDeviceIdsList(), reason);
        return new KickOutcome(0, "kicked", result.getKicked(), result.getDeviceIdsList(), true);
    }

    /**
     * 从账号下移除一台设备（设备管理页的「删除」）。
     *
     * <p>在线时先踢再移除：只删台账不踢的话，那台设备仍然连着，而列表里它消失了——
     * 用户会以为已经下线，实际还在收消息。
     */
    public boolean forget(String userId, String deviceId) {
        if (userId == null || userId.isBlank() || deviceId == null || deviceId.isBlank()) {
            return false;
        }
        boolean online = store.devicesOf(userId).stream()
                .anyMatch(device -> device.deviceId().equals(deviceId) && device.online());
        if (online) {
            kick(userId, deviceId, null, "device-removed");
        }
        store.forget(userId, deviceId);
        return true;
    }

    /** 踢下线的结果：{@code code != 0} 表示这次请求没被受理（不是「没踢到」） */
    public record KickOutcome(int code, String message, int kicked, List<String> deviceIds,
                              boolean controllable) {

        static KickOutcome rejected(String message) {
            return new KickOutcome(400, message, 0, List.of(), false);
        }

        public boolean ok() {
            return code == 0;
        }
    }

    /** @return 推送命中的连接数（用户级粒度，含刚登录的那台，客户端按 deviceId 过滤） */
    private int notifyLogin(String userId, String deviceId, String transport, long ts) {
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return 0;
        }
        try {
            PushResult result = pushPort.push(PushTask.newBuilder()
                    .setTraceId("session-login-" + userId + '-' + deviceId)
                    .addTargetUserIds(userId)
                    .setFrame(frames.loginFrame(userId, deviceId, transport, ts))
                    .setTtlMs(NOTIFY_TTL_MS)
                    .build());
            return result == null ? 0 : result.getDeliveredUserIdsCount();
        } catch (RuntimeException e) {
            // 登录通知失败不影响登录本身：客户端下次拉设备列表也能看到
            log.debug("[device] 登录通知失败 user={} device={} err={}", userId, deviceId, e.getMessage());
            return 0;
        }
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }
}
