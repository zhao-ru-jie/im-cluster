package com.github.im.server.conversation;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 会话标记（置顶 / 免打扰）变更的多端同步。
 *
 * <h2>为什么必须广播</h2>
 *
 * <p>标记是<b>用户级</b>状态（{@code im:user:{id}:flags}，不分设备），但写入动作发生在某一台设备上。
 * 不广播的后果是「手机置顶了，网页上还是乱的」，而且<b>永远不会自愈</b>——直到下次冷启动重新拉
 * 会话列表。换句话说，这不是「让体验更好」，而是消除一处必然的不一致。
 *
 * <h2>为什么收件人是「整个账号」而不是「其它设备」</h2>
 *
 * <p>推送粒度只到 userId（见 {@code PushTask}），所以发起变更的那台设备也会收到这条帧。
 * 帧里带 {@code originDeviceId}，它据此忽略自己的回声即可。要在链路上做设备级排除，
 * 得让每个网关实例都理解「排除哪台」，代价远大于省下一条本来就会被忽略的帧。
 *
 * <h2>为什么没有去抖</h2>
 *
 * <p>置顶/免打扰是用户点一下改一次的动作，天然低频；而且<b>连续两次相反的操作必须都送达</b>
 * （先取消再置顶，如果被去抖吞掉一条，另一端就会停在错误的中间状态）。
 * 这与「正在输入」那种高频状态恰好相反。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class ConversationFlagNotifier {

    private static final Logger log = LoggerFactory.getLogger(ConversationFlagNotifier.class);

    /** 标记变更不是时效性信息，但也没必要在客户端离线很久后补发——重连拉一次会话列表就有 */
    private static final int TTL_MS = 60_000;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final boolean enabled;

    public ConversationFlagNotifier(ClientFrames frames,
                                    ObjectProvider<PushPort> pushPortProvider,
                                    @Value("${im.conv.flag-broadcast:true}") boolean enabled) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.enabled = enabled;
        log.info("[conv] 会话标记（置顶/免打扰）多端广播{}", enabled ? "已开启" : "已关闭");
    }

    /**
     * 广播一次标记变更。
     *
     * @param originDeviceId 发起变更的设备（客户端据此忽略自己的回声）；可为空
     * @return 推送命中的连接数；0 表示没装推送通道或在其它设备都不在线
     */
    public int notifyFlags(String userId, String conversationId, boolean pinned, boolean muted,
                           String originDeviceId) {
        if (!enabled || userId == null || userId.isBlank()
                || conversationId == null || conversationId.isBlank()) {
            return 0;
        }
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return 0;
        }
        String frame = frames.convUpdateFrame(conversationId, pinned, muted, originDeviceId,
                System.currentTimeMillis());
        try {
            PushResult result = pushPort.push(PushTask.newBuilder()
                    .setTraceId("conv-flag-" + conversationId + '-' + userId)
                    .addTargetUserIds(userId)
                    .setFrame(frame)
                    .setTtlMs(TTL_MS)
                    .setConversationId(conversationId)
                    .build());
            return result == null ? 0 : result.getDeliveredUserIdsCount();
        } catch (RuntimeException e) {
            // 广播失败不该让「设置成功」变成失败：本地已经写进 Redis，下次拉列表就能对齐
            log.debug("[conv] 标记广播失败 user={} conv={} err={}",
                    userId, conversationId, e.getMessage());
            return 0;
        }
    }
}
