package com.github.im.server.presence;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.PresencePort;
import com.github.im.proto.presence.v1.DeviceAck;
import com.github.im.proto.presence.v1.DeviceReport;
import com.github.im.proto.presence.v1.GetPresenceRequest;
import com.github.im.proto.presence.v1.GetPresenceResponse;
import com.github.im.proto.presence.v1.PresenceReport;
import com.github.im.proto.presence.v1.PresenceReportAck;
import com.github.im.proto.presence.v1.PresenceState;
import com.github.im.proto.presence.v1.TypingRequest;
import com.github.im.proto.presence.v1.TypingResponse;
import com.github.im.server.device.DeviceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/**
 * presence / typing 的业务实现（{@link PresencePort} 的本地实现）。
 *
 * <h2>三个动作的分工</h2>
 *
 * <ul>
 *   <li><b>输入状态</b>：只做「解析出真正的收件人」这一件事，然后交给
 *       {@link TypingNotifier} 去扇出。真正的判断（单聊对端 / 群成员 / 去抖）都在那边；</li>
 *   <li><b>查在线</b>：读 {@link PresenceStore}（复用网关写的在线路由表），不做任何缓存。
 *       缓存会让「刚下线的人还亮着」，而 presence 的唯一价值就是「现在」；</li>
 *   <li><b>上下线边沿</b>：这是唯一一处需要「判断」的逻辑，见 {@link #reportPresence}。</li>
 * </ul>
 *
 * <h2>为什么下线要再查一次在线</h2>
 *
 * <p>网关上报的是<b>它自己看到的边沿</b>（本机上最后一个设备断开）。但一个用户可能在
 * 手机（网关 A）和网页（网关 B）同时在线：A 上报下线时，B 上还有连接。
 * 直接把这当成「用户下线」广播给好友，会得到一个长期灰着的头像——
 * 这种「错的下线」比漏报上线严重得多（漏报只影响一次更新，错报会一直错）。
 *
 * <p>所以下线路径必须复核在线路由表：<b>只有确认真的没有任何实例还连着，才广播下线</b>。
 * 上线路径则不做这个复核——重复的 {@code online:true} 是无害的（客户端幂等覆盖），
 * 而漏报上线会让好友列表一直不亮。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Service
public class PresenceService implements PresencePort {

    private static final Logger log = LoggerFactory.getLogger(PresenceService.class);

    /**
     * 单次查询的上限。
     *
     * <p>好友列表可以很长，一次把几千人塞进来会让 Redis 出现一次 3000 条命令的 pipeline。
     * 超出的部分不返回（而不是截断后悄悄少几个人），客户端可以分批再查。
     */
    private static final int MAX_QUERY = 200;

    private final PresenceStore store;
    private final TypingNotifier typingNotifier;
    private final PresenceNotifier presenceNotifier;
    /** 设备级事件（台账 + 多端登录通知）委托给它：presence 管「用户在不在」，设备管「哪几台在」 */
    private final DeviceService deviceService;

    public PresenceService(PresenceStore store,
                           TypingNotifier typingNotifier,
                           PresenceNotifier presenceNotifier,
                           DeviceService deviceService) {
        this.store = store;
        this.typingNotifier = typingNotifier;
        this.presenceNotifier = presenceNotifier;
        this.deviceService = deviceService;
    }

    @Override
    public TypingResponse reportTyping(TypingRequest request) {
        String from = request.getUserId();
        if (from == null || from.isBlank()) {
            return TypingResponse.newBuilder().setCode(400).setMessage("userId is required").build();
        }
        String conversationId = request.getConversationId();
        String convType = request.getConvType();
        if (conversationId == null || conversationId.isBlank()) {
            // 客户端只给了「发给谁」：按单聊/群聊规范现场生成会话 ID。
            // 与消息路径共用 MessageEnvelope 的规则，避免两处对「同一个会话」算出不同的 ID
            String receiver = request.getReceiverId();
            if (receiver == null || receiver.isBlank()) {
                return TypingResponse.newBuilder()
                        .setCode(400)
                        .setMessage("conversationId or receiverId is required")
                        .build();
            }
            conversationId = MessageEnvelope.CONV_GROUP.equalsIgnoreCase(convType)
                    ? "g:" + receiver
                    : MessageEnvelope.singleConversationId(from, receiver);
        }
        int delivered = typingNotifier.notifyTyping(conversationId, convType, from,
                request.getReceiverId(), request.getTyping());
        return TypingResponse.newBuilder()
                .setCode(0)
                .setMessage("ok")
                .setDelivered(delivered)
                .build();
    }

    @Override
    public GetPresenceResponse presence(GetPresenceRequest request) {
        List<String> userIds = new ArrayList<>();
        for (String userId : request.getUserIdsList()) {
            if (userId != null && !userId.isBlank() && !userIds.contains(userId)) {
                userIds.add(userId);
            }
        }
        if (userIds.size() > MAX_QUERY) {
            log.debug("[presence] 查询 {} 个用户超过单次上限 {}，只返回前 {} 个",
                    userIds.size(), MAX_QUERY, MAX_QUERY);
            userIds = userIds.subList(0, MAX_QUERY);
        }
        if (userIds.isEmpty()) {
            return GetPresenceResponse.getDefaultInstance();
        }
        return GetPresenceResponse.newBuilder().addAllStates(statesOf(userIds)).build();
    }

    /**
     * 在线状态列表（REST 与 gRPC 共用同一个出口，避免两条路径给出不同的判定）。
     *
     * <p>结果顺序与入参一致：调用方拿到的第 i 个状态对应的就是它传的第 i 个人，
     * 不必再按 userId 做一次 join。
     */
    public List<PresenceState> statesOf(List<String> userIds) {
        List<String> distinct = new ArrayList<>(new LinkedHashSet<>(userIds));
        Map<String, PresenceStore.Snapshot> snapshots = store.snapshotsOf(distinct);
        List<PresenceState> states = new ArrayList<>(distinct.size());
        for (String userId : distinct) {
            PresenceStore.Snapshot snapshot = snapshots.getOrDefault(userId, PresenceStore.Snapshot.offline());
            states.add(PresenceState.newBuilder()
                    .setUserId(userId)
                    .setOnline(snapshot.online())
                    .setGateways(snapshot.gateways())
                    .setDevices(snapshot.devices())
                    .setLastSeen(snapshot.lastSeen())
                    .build());
        }
        return states;
    }

    /**
     * 设备级上报：台账 + 多端登录通知。
     *
     * <p>转发给 {@link DeviceService} 而不是在这里就地实现：用户级的在线状态（好友列表的亮点）
     * 与设备级的台账（设备管理页）是两份不同的数据，混在一个类里，改其中一侧时很容易踩到另一侧。
     */
    @Override
    public DeviceAck reportDevice(DeviceReport report) {
        return deviceService.report(report);
    }

    @Override
    public PresenceReportAck reportPresence(PresenceReport report) {
        String userId = report.getUserId();
        if (userId == null || userId.isBlank()) {
            return PresenceReportAck.newBuilder().setCode(400).setMessage("userId is required").build();
        }
        long ts = report.getTs() > 0 ? report.getTs() : System.currentTimeMillis();
        if (report.getOnline()) {
            int delivered = presenceNotifier.notifyChange(userId, true, ts);
            return PresenceReportAck.newBuilder()
                    .setCode(0)
                    .setMessage("online")
                    .setBroadcast(delivered > 0)
                    .build();
        }
        if (store.online(userId)) {
            // 别的网关实例上还有这个用户的连接：这次「下线」只是局部的，绝不能广播全局下线
            log.debug("[presence] 收到下线上报但仍在线，跳过广播 user={} gateway={}",
                    userId, report.getGatewayInstance());
            return PresenceReportAck.newBuilder()
                    .setCode(0)
                    .setMessage("still online on other gateway")
                    .setBroadcast(false)
                    .build();
        }
        // 只有「真的下线」才记 lastSeen：它在语义上是「上次在线到最后什么时刻」
        store.touchLastSeen(userId, ts);
        int delivered = presenceNotifier.notifyChange(userId, false, ts);
        return PresenceReportAck.newBuilder()
                .setCode(0)
                .setMessage("offline")
                .setBroadcast(delivered > 0)
                .build();
    }
}
