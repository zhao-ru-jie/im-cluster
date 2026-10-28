package com.github.im.server.presence;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.message.ClientFrames;
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
 * 「正在输入」的扇出：把一个人的输入状态推给会话里的其他人。
 *
 * <h2>为什么这一个功能值得单独一个类</h2>
 *
 * <p>它和消息走的不是同一条路：<b>不落库、不占 seq、不进离线队列、失败静默</b>。
 * 塞进 {@code MessageService} 的话，所有为消息准备的东西（幂等表、发号器、补发游标）
 * 都会被迫回答「typing 的 seq 是几」这种没有意义的问题。
 *
 * <h2>三个刻意的选择</h2>
 *
 * <ul>
 *   <li><b>去抖（默认 3s）</b>：客户端在打字期间会持续上报（这是它对「用户还在打字」的唯一表达），
 *       每帧都扇出等于让一个打字的人给整个群刷屏。窗口内重复的 {@code typing=true} 只推一次，
 *       而 {@code typing=false} <b>永远转发</b>——那三个点必须能立刻熄灭，否则界面会一直闪。</li>
 *   <li><b>群成员扇出上限（默认 200）</b>：万人群里「某人正在输入」既没人会看，
 *       一次扇出又要给上万人各写一帧。超限直接不推，而不是退化成推给少数人
 *       （那会让一部分人看到输入状态、另一部分看不到，比全都不看到更像 bug）。</li>
 *   <li><b>同步推送并返回送达人数</b>：调用方（网关）本来就在虚拟线程上，
 *       同步换来的是一句可观测的「推给了几个人」。异步的话返回值只能恒为 0，
 *       那这个字段就成了摆设。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class TypingNotifier {

    private static final Logger log = LoggerFactory.getLogger(TypingNotifier.class);

    /** 输入状态的存活期：10 秒后送到的「正在输入」已经在误导人了（他可能早就发完消息） */
    private static final int TTL_MS = 10_000;

    /** 去重表上限：超了整表丢掉重来，宁可多推一条，也不让一张 map 无限长 */
    private static final int MAX_TRACKED = 200_000;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final GroupMemberRepository groupMembers;
    private final boolean enabled;
    private final int maxGroupMembers;
    private final long dedupWindowMs;

    /** {@code conversationId|fromUserId} → 上次推送的时刻 */
    private final Map<String, Long> pushed = new ConcurrentHashMap<>();

    @Autowired
    public TypingNotifier(ClientFrames frames,
                          ObjectProvider<PushPort> pushPortProvider,
                          GroupMemberRepository groupMembers,
                          @Value("${im.typing.enabled:true}") boolean enabled,
                          @Value("${im.typing.max-group-members:200}") int maxGroupMembers) {
        this(frames, pushPortProvider, groupMembers, enabled, maxGroupMembers, 3_000);
    }

    /** 单测入口：可以缩短去抖窗口，不必真的等 3 秒 */
    TypingNotifier(ClientFrames frames,
                   ObjectProvider<PushPort> pushPortProvider,
                   GroupMemberRepository groupMembers,
                   boolean enabled,
                   int maxGroupMembers,
                   long dedupWindowMs) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.groupMembers = groupMembers;
        this.enabled = enabled;
        this.maxGroupMembers = Math.max(maxGroupMembers, 0);
        this.dedupWindowMs = dedupWindowMs;
        log.info("[typing] 输入状态（typing.push）{}，去抖 {}ms，群扇出上限 {} 人",
                enabled ? "已开启" : "已关闭", dedupWindowMs, maxGroupMembers);
    }

    /**
     * 扇出一条输入状态。
     *
     * @param conversationId 会话 ID（单聊 {@code s:a:b} / 群聊 {@code g:xxx}）
     * @param convType       SINGLE / GROUP（空则按会话 ID 前缀判定）
     * @param fromUserId     谁在输入
     * @param receiverId     单聊对端；群聊留空且从会话 ID 里解析群号
     * @param typing         true=正在输入 / false=停止
     * @return 实际推送的在线用户数；0 表示「没有可推的人」或「去抖吞掉了」
     */
    public int notifyTyping(String conversationId, String convType, String fromUserId,
                            String receiverId, boolean typing) {
        if (!enabled || conversationId == null || conversationId.isBlank()
                || fromUserId == null || fromUserId.isBlank()) {
            return 0;
        }
        String type = convTypeOf(conversationId, convType);
        List<String> targets = targetsOf(conversationId, type, fromUserId, receiverId);
        if (targets.isEmpty()) {
            return 0;
        }
        // 停止输入必须立刻转发，并且把去抖标记清掉：否则「停止 → 3 秒内又打字」会被误吞，
        // 表现是对方的那三个点再也不出现
        if (!typing) {
            pushed.remove(conversationId + '|' + fromUserId);
        } else if (!shouldPush(conversationId, fromUserId)) {
            return 0;
        }
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return 0;
        }
        String frame = frames.typingFrame(conversationId, type, fromUserId, typing,
                System.currentTimeMillis());
        try {
            PushResult result = pushPort.push(PushTask.newBuilder()
                    .setTraceId("typing-" + conversationId + '-' + fromUserId)
                    .addAllTargetUserIds(targets)
                    .setFrame(frame)
                    .setTtlMs(TTL_MS)
                    .setConversationId(conversationId)
                    .build());
            int delivered = result == null ? 0 : result.getDeliveredUserIdsCount();
            log.debug("[typing] conv={} from={} typing={} targets={} delivered={}",
                    conversationId, fromUserId, typing, targets.size(), delivered);
            return delivered;
        } catch (RuntimeException e) {
            // 输入状态是装饰品：推不出去就丢掉，绝不让它把连接或 RPC 变成失败
            log.debug("[typing] 扇出失败 conv={} from={} err={}",
                    conversationId, fromUserId, e.getMessage());
            return 0;
        }
    }

    /** 已记录的会话数（观测 / 测试用） */
    public int tracked() {
        return pushed.size();
    }

    /**
     * 收件人：单聊是对端，群聊是「除自己以外的成员」。
     *
     * <p>永远排除发送者自己：多端登录时自己也有一堆设备，把「我在输入」推给自己
     * 会让另一个端莫名其妙地跳出三个点。
     */
    private List<String> targetsOf(String conversationId, String convType,
                                   String fromUserId, String receiverId) {
        if (MessageEnvelope.CONV_GROUP.equals(convType)) {
            String groupId = groupIdOf(conversationId, receiverId);
            if (groupId == null) {
                return List.of();
            }
            List<String> members = groupMembers == null ? List.of() : groupMembers.membersOf(groupId);
            if (members.isEmpty()) {
                // 空成员表 = 群不存在 / 没有成员：绝不能退化成「投给群 ID」
                return List.of();
            }
            if (members.size() > maxGroupMembers) {
                log.debug("[typing] 群 {} 成员 {} 超过扇出上限 {}，跳过输入状态",
                        groupId, members.size(), maxGroupMembers);
                return List.of();
            }
            List<String> others = new ArrayList<>(members.size());
            for (String member : members) {
                if (!fromUserId.equals(member)) {
                    others.add(member);
                }
            }
            return others;
        }
        String peer = receiverId != null && !receiverId.isBlank()
                ? receiverId
                : MessageEnvelope.peerOf(conversationId, fromUserId);
        if (peer == null || peer.isBlank() || peer.equals(fromUserId)) {
            // 解不出对端说明这条帧本身就是脏的（或自己在给自己发），丢掉比推出去安全
            return List.of();
        }
        return List.of(peer);
    }

    /** 会话类型：客户端没带时按会话 ID 前缀判断（与 {@code MessageService.convTypeOf} 同一口径） */
    private static String convTypeOf(String conversationId, String convType) {
        if (convType != null && !convType.isBlank()) {
            return convType.toUpperCase(java.util.Locale.ROOT);
        }
        return conversationId.startsWith("g:") ? MessageEnvelope.CONV_GROUP : MessageEnvelope.CONV_SINGLE;
    }

    /** 群号：优先取去掉 {@code g:} 前缀的会话 ID，其次用调用方给的 {@code to}（群聊的 to 就是群号） */
    private static String groupIdOf(String conversationId, String receiverId) {
        if (conversationId.startsWith("g:") && conversationId.length() > 2) {
            return conversationId.substring(2);
        }
        return receiverId == null || receiverId.isBlank() ? null : receiverId;
    }

    /** @return true = 窗口内没有推过，这次值得推 */
    private boolean shouldPush(String conversationId, String fromUserId) {
        if (pushed.size() > MAX_TRACKED) {
            pushed.clear();
        }
        long now = System.currentTimeMillis();
        String key = conversationId + '|' + fromUserId;
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
