package com.github.im.server.presence;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import com.github.im.server.relation.RelationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 在线状态变化的扇出：把「某人上线/下线」推给他的<b>好友</b>。
 *
 * <h2>推给谁</h2>
 *
 * <p>好友（{@link RelationRepository#friendsOf}）。不做「推给最近聊过的人」这种扩展：
 * 那需要在上下线的高频路径上读会话列表，而会话列表的权威源在 IM 服务本地，
 * 一个用户上下线一次就要拉一遍几十条会话，收益（多点亮几个头像）远小于成本。
 *
 * <p><b>拉黑不对称的问题</b>：关系表里 {@code blockedOf(userId)} 只能回答「我拉黑了谁」，
 * 回答不了「谁拉黑了我」。所以这里只按好友关系推，被对方拉黑的人仍可能收到对方的上线事件
 * （客户端收到后不展示即可）。要做得更严，需要在关系层增加反向索引——那是关系模型的事，
 * 不该由一个通知类顺手引入。
 *
 * <h2>为什么上下线的去重要按「用户 × 状态」</h2>
 *
 * <p>同一状态在 3 秒内重复到达是常态：客户端断网重连、同一个人两台设备先后上线，
 * 都会产生一串「上线」。重复的 {@code online:true} 本身无害（客户端幂等覆盖），
 * 但它会放大推送量、也会让「刚刚上线」的动画反复播放。窗口内同类事件只推一次。
 *
 * <p>反过来，<b>上线后立刻下线绝不能被去重吞掉</b>：这两条是不同的状态，键里带上了
 * {@code online} 位，所以它们永远不会互相抑制。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class PresenceNotifier {

    private static final Logger log = LoggerFactory.getLogger(PresenceNotifier.class);

    /** 在线状态只在「此刻」有意义，30 秒后送到的「上线」已经没有参考价值 */
    private static final int TTL_MS = 30_000;

    private static final int MAX_TRACKED = 200_000;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final RelationRepository relations;
    private final boolean enabled;
    private final long dedupWindowMs;

    /** {@code userId|online} → 上次推送的时刻 */
    private final Map<String, Long> pushed = new ConcurrentHashMap<>();

    @Autowired
    public PresenceNotifier(ClientFrames frames,
                            ObjectProvider<PushPort> pushPortProvider,
                            RelationRepository relations,
                            @Value("${im.presence.enabled:true}") boolean enabled) {
        this(frames, pushPortProvider, relations, enabled, 3_000);
    }

    /** 单测入口：可以缩短去重窗口 */
    PresenceNotifier(ClientFrames frames,
                     ObjectProvider<PushPort> pushPortProvider,
                     RelationRepository relations,
                     boolean enabled,
                     long dedupWindowMs) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.relations = relations;
        this.enabled = enabled;
        this.dedupWindowMs = dedupWindowMs;
        log.info("[presence] 在线状态广播（presence.push）{}，去重窗口 {}ms",
                enabled ? "已开启" : "已关闭", dedupWindowMs);
    }

    /**
     * 广播一次状态变化。
     *
     * @param userId 状态主体
     * @param online true=上线 / false=下线
     * @param ts     事件时间（毫秒）
     * @return 实际推送的好友数；0 表示没有好友、被去重吞掉或推送失败
     */
    public int notifyChange(String userId, boolean online, long ts) {
        if (!enabled || userId == null || userId.isBlank()) {
            return 0;
        }
        List<String> friends = friendsOf(userId);
        if (friends.isEmpty()) {
            // 没有好友就没有收件人：宁可不推，也不要退化成广播
            return 0;
        }
        if (!shouldPush(userId, online)) {
            return 0;
        }
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return 0;
        }
        String frame = frames.presenceFrame(userId, online, ts > 0 ? ts : System.currentTimeMillis());
        try {
            PushResult result = pushPort.push(PushTask.newBuilder()
                    .setTraceId("presence-" + userId + '-' + (online ? "on" : "off"))
                    .addAllTargetUserIds(friends)
                    .setFrame(frame)
                    .setTtlMs(TTL_MS)
                    .build());
            int delivered = result == null ? 0 : result.getDeliveredUserIdsCount();
            log.debug("[presence] user={} online={} friends={} delivered={}",
                    userId, online, friends.size(), delivered);
            return delivered;
        } catch (RuntimeException e) {
            // 状态广播失败不影响任何正确性：客户端下次拉好友列表就能拿到最新的在线状态
            log.debug("[presence] 广播失败 user={} online={} err={}", userId, online, e.getMessage());
            return 0;
        }
    }

    /** 已记录的去重键数量（观测 / 测试用） */
    public int tracked() {
        return pushed.size();
    }

    private List<String> friendsOf(String userId) {
        List<String> friends = relations == null ? List.of() : relations.friendsOf(userId);
        if (friends == null || friends.isEmpty()) {
            return List.of();
        }
        List<String> targets = new ArrayList<>(friends.size());
        for (String friend : friends) {
            if (friend != null && !friend.isBlank() && !friend.equals(userId)) {
                targets.add(friend);
            }
        }
        return targets;
    }

    /** @return true = 窗口内没有推过同一状态，这次值得推 */
    private boolean shouldPush(String userId, boolean online) {
        if (pushed.size() > MAX_TRACKED) {
            pushed.clear();
        }
        long now = System.currentTimeMillis();
        String key = userId + '|' + online;
        boolean[] moved = {false};
        pushed.compute(key, (k, previous) -> {
            if (previous != null && now - previous < dedupWindowMs) {
                return previous;
            }
            moved[0] = true;
            return now;
        });
        return moved[0];
    }
}
