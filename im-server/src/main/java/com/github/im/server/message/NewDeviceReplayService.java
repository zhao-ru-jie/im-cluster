package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 新端回放：一台设备第一次上线时，把「最近 M 个会话 × 每个最近 N 条」直接推给它。
 *
 * <h2>它补的是哪个洞</h2>
 *
 * <p>补发（{@code pullPending}）解决的是「<b>这台设备</b>错过了什么」，它的数据源是两条游标：
 * 单聊帧队列与群聊补发锚点。两者都有「消费完就退休」的语义——群锚点在
 * {@code allDevicesReached} 之后会被 {@link OfflineMessageStore#clearPending} 摘掉。
 * 于是出现一个很具体的洞：<b>所有老设备都补完了 → 用户换新手机 → 服务端手里一条待补都没有</b>，
 * 新设备拿到的补发是空的，只能靠宿主自己调历史接口一页页追平。
 *
 * <p>这里把「新端登录能看到最近消息」变成服务端的职责：不去碰任何游标，而是<b>从会话
 * 历史重新切一份快照</b>——最近活跃的 M 个会话，每个取最新的 N 条。这就是主流 IM 里
 * 「新设备登录后立刻看到聊天列表和最近几条」的实现方式。
 *
 * <h2>三个刻意的选择</h2>
 *
 * <ul>
 *   <li><b>不参与补发游标</b>：回放帧走 {@code PullPendingResponse.replay_frames}，与
 *       {@code ack_single_seq}/{@code ack_group_seqs} 完全无关。混在一起的话，
 *       新设备首次上线会把「离线队列已消费」一起确认掉——那才是真正的丢消息；</li>
 *   <li><b>标记在网关确认写成功之后才落</b>（{@link #markReplayed}）：回放生成时就落标记，
 *       一旦写失败，这台设备会永久停在「服务端以为它有了、它其实什么都没有」的状态。
 *       宁可写失败时下次再回放一遍（客户端按 {@code (conversationId, seq)} 去重）；</li>
 *   <li><b>独立预算</b>：回放不吃补发的 200 帧预算，补发也不吃回放的 300 帧预算。
 *       两者回答的是不同问题（「缺了什么」vs「最近有什么」），共用预算只会让其中一件事被饿死。</li>
 * </ul>
 *
 * <h2>和离线队列的重叠</h2>
 *
 * <p>新设备两条路都会走一遍，重叠的消息会出现两次。这是<b>故意的</b>：去重成本在客户端是
 * 一次哈希查找（本地消息表本来就按 {@code (conversationId, seq)} 去重），而「按集合精确求差」
 * 需要在服务端为每台设备维护一份已回放 seq，代价远大于收益。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class NewDeviceReplayService {

    private static final Logger log = LoggerFactory.getLogger(NewDeviceReplayService.class);

    /** 进程内标记表上限：它只是 Redis 的兜底，没有精确性要求 */
    private static final int MAX_LOCAL_MARKERS = 100_000;

    private static final String MARKER_VALUE = "1";

    private final ConversationStore conversations;
    private final GroupMemberRepository groupMembers;
    private final MessageRepository messageRepository;
    private final ClientFrames frames;
    /** 可空：单测里传 null，标记只记在进程内那份集合里 */
    private final StringRedisTemplate redis;
    private final boolean enabled;
    /** 回放多少个会话（按最后消息时间倒序取） */
    private final int maxConversations;
    /** 每个会话回放多少条 */
    private final int perConversation;
    /** 一轮回放的帧总数上限：会话数 × 每会话条数可能超出，这里兜住 */
    private final int maxFrames;
    private final Duration markerTtl;

    /** 无 Redis（单测）或 Redis 抖动时的进程内兜底 */
    private final Set<String> localMarkers = ConcurrentHashMap.newKeySet();

    public NewDeviceReplayService(ConversationStore conversations,
                                  GroupMemberRepository groupMembers,
                                  MessageRepository messageRepository,
                                  ClientFrames frames,
                                  StringRedisTemplate redis,
                                  @Value("${im.replay.enabled:true}") boolean enabled,
                                  @Value("${im.replay.max-conversations:20}") int maxConversations,
                                  @Value("${im.replay.per-conversation:20}") int perConversation,
                                  @Value("${im.replay.max-frames:300}") int maxFrames,
                                  @Value("${im.replay.marker-ttl:30d}") Duration markerTtl) {
        this.conversations = conversations;
        this.groupMembers = groupMembers;
        this.messageRepository = messageRepository;
        this.frames = frames;
        this.redis = redis;
        this.enabled = enabled;
        this.maxConversations = Math.max(maxConversations, 1);
        this.perConversation = Math.max(perConversation, 1);
        this.maxFrames = Math.max(maxFrames, 1);
        this.markerTtl = markerTtl == null ? Duration.ofDays(30) : markerTtl;
        log.info("[replay] 新端回放{}（最多 {} 个会话 × 每个 {} 条，总帧上限 {}）",
                enabled ? "已开启" : "已关闭", this.maxConversations, this.perConversation, this.maxFrames);
    }

    /**
     * 生成一次回放。
     *
     * @param explicit 客户端显式请求（{@code PullPendingRequest.replay}）：
     *                 跳过「已回放过」判定——用户的本地库可能被清空了，但 deviceId 没变
     * @return 帧列表 + 覆盖的会话数 + 是否不完整；不需要回放时返回 {@link Replay#empty()}
     */
    public Replay build(String userId, String deviceId, boolean explicit) {
        if (!enabled || !StringUtils.hasText(userId)) {
            return Replay.empty();
        }
        String device = OfflineMessageStore.normalizeDevice(deviceId);
        if (!explicit && replayed(userId, device)) {
            return Replay.empty();
        }
        try {
            List<String> conversationIds = conversations.recentConversations(
                    userId, maxConversations, groupIdsOf(userId));
            if (conversationIds.isEmpty()) {
                return Replay.empty();
            }
            List<String> out = new ArrayList<>();
            int budget = maxFrames;
            int covered = 0;
            boolean truncated = false;
            for (String conversationId : conversationIds) {
                if (budget <= 0) {
                    truncated = true;
                    break;
                }
                int limit = Math.min(perConversation, budget);
                List<StoredMessage> messages;
                try {
                    // beforeSeq <= 0 = 不设上界，取最新一页（升序返回），正好是「最近 N 条」
                    messages = messageRepository.before(conversationId, 0, limit);
                } catch (RuntimeException e) {
                    log.warn("[replay] 读会话历史失败，跳过该会话 conv={} err={}",
                            conversationId, e.getMessage());
                    truncated = true;
                    continue;
                }
                if (messages == null || messages.isEmpty()) {
                    continue;
                }
                int sent = appendFrames(userId, conversationId, messages, out);
                budget -= sent;
                if (sent > 0) {
                    covered++;
                }
            }
            if (out.isEmpty()) {
                return Replay.empty();
            }
            log.info("[replay] 新端回放 user={} device={} conversations={} frames={} truncated={}",
                    userId, device, covered, out.size(), truncated);
            return new Replay(List.copyOf(out), covered, truncated);
        } catch (RuntimeException e) {
            // 回放是可选的「体验增强」：它失败绝不能把补发一起带走
            log.error("[replay] 生成回放失败，本次跳过 user={} device={} err={}",
                    userId, device, e.getMessage());
            return Replay.empty();
        }
    }

    /**
     * 记录「这台设备已经回放过」。
     *
     * <p>调用点在 {@code ackPending}——网关把回放帧全部写进连接之后。
     * 见类注释：提前落标记会让写失败的设备永远补不到回放。
     */
    public void markReplayed(String userId, String deviceId) {
        if (!StringUtils.hasText(userId)) {
            return;
        }
        String key = markerKey(userId, deviceId);
        if (localMarkers.size() > MAX_LOCAL_MARKERS) {
            localMarkers.clear();
        }
        localMarkers.add(key);
        if (redis == null) {
            return;
        }
        try {
            redis.opsForValue().set(key, MARKER_VALUE, markerTtl);
        } catch (RuntimeException e) {
            log.warn("[replay] 记录回放标记失败（下次连接会再回放一遍，客户端按 seq 去重）user={} device={} err={}",
                    userId, deviceId, e.getMessage());
        }
    }

    /**
     * 这台设备是否已经回放过。
     *
     * <p>Redis 读失败时按「没回放过」处理：多推一遍是可自愈的（客户端去重），
     * 而误判成「回放过」会让一台真的空设备再也拿不到数据。
     */
    public boolean replayed(String userId, String deviceId) {
        String key = markerKey(userId, deviceId);
        if (redis != null) {
            try {
                if (Boolean.TRUE.equals(redis.hasKey(key))) {
                    return true;
                }
            } catch (RuntimeException e) {
                log.warn("[replay] 读取回放标记失败，按未回放处理 user={} device={} err={}",
                        userId, deviceId, e.getMessage());
            }
        }
        return localMarkers.contains(key);
    }

    // ==== 内部 ====

    /**
     * 把一个会话的消息渲染成帧，过滤本地删除、脱敏撤回。
     *
     * <p>与补发出口同一套取舍：撤回/删除状态<b>查询失败时按原文下发</b>。
     * 反过来会在存储抖动时把正常消息永久标成「已撤回」，那不可恢复。
     *
     * @return 实际产出的帧数
     */
    private int appendFrames(String userId, String conversationId,
                             List<StoredMessage> messages, List<String> out) {
        Set<Long> recalled;
        Set<Long> deleted;
        try {
            Set<Long> seqs = new LinkedHashSet<>(messages.size());
            for (StoredMessage message : messages) {
                seqs.add(message.seq());
            }
            recalled = messageRepository.recalledSeqs(conversationId, seqs);
            deleted = messageRepository.deletedSeqs(userId, conversationId);
        } catch (RuntimeException e) {
            log.warn("[replay] 查询撤回/删除状态失败，本会话按原文回放 conv={} err={}",
                    conversationId, e.getMessage());
            recalled = Set.of();
            deleted = Set.of();
        }
        int sent = 0;
        for (StoredMessage message : messages) {
            if (deleted.contains(message.seq())) {
                continue;
            }
            out.add(frames.frameOf(message, recalled.contains(message.seq())));
            sent++;
        }
        return sent;
    }

    /** 用户加入的群：大群的会话列表是写扩散降级的，只能靠反向索引补出来 */
    private Set<String> groupIdsOf(String userId) {
        try {
            return new LinkedHashSet<>(groupMembers.groupsOf(userId));
        } catch (RuntimeException e) {
            log.warn("[replay] 读用户群列表失败，本次回放只覆盖单聊会话 user={} err={}",
                    userId, e.getMessage());
            return Set.of();
        }
    }

    private static String markerKey(String userId, String deviceId) {
        return RedisKeys.userReplay(userId, OfflineMessageStore.normalizeDevice(deviceId));
    }

    /**
     * 一次回放的结果。
     *
     * @param frames        已序列化的 {@code msg.push} 帧，按会话分组、会话内按 seq 升序
     * @param conversations 实际覆盖到的会话数
     * @param truncated     回放不完整（预算用尽或某会话读失败）
     */
    public record Replay(List<String> frames, int conversations, boolean truncated) {

        public static Replay empty() {
            return new Replay(List.of(), 0, false);
        }

        public boolean included() {
            return !frames.isEmpty();
        }
    }
}
