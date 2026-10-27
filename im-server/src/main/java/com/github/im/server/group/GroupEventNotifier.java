package com.github.im.server.group;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 群变更事件的下行广播。
 *
 * <h2>为什么需要它</h2>
 *
 * <p>没有这个通道时，群变更只落在存储里：一个人被踢出群，他手机上的会话列表还留着这个群，
 * 点进去发消息才收到 403；管理员改了群名，其他成员的列表还是旧名字，直到下次登录。
 * <b>「变更已经发生」和「所有人知道变更发生了」是两件事</b>，这个类负责后者。
 *
 * <h2>与另外两个通知器的分工</h2>
 *
 * <table border="1">
 *   <caption>三个下行事件通道</caption>
 *   <tr><th>类</th><th>帧</th><th>主体</th><th>接收方</th></tr>
 *   <tr><td>{@code MessageStateNotifier}</td><td>{@code msg.state}</td><td>消息 (conv+seq)</td><td>单聊对端</td></tr>
 *   <tr><td>{@code MessageUpdateNotifier}</td><td>{@code msg.update}</td><td>消息 (conv+seq)</td><td>撤回=全会话；删除=本人其它端</td></tr>
 *   <tr><td>本类</td><td>{@code group.update}</td><td>群（无 seq）</td><td>群成员 ∪ 被操作者</td></tr>
 * </table>
 *
 * <p>接收方要并上「被操作者」：被踢出群的人此刻可能<b>已经不是成员</b>了，
 * 只按成员集合推的话他收不到「你已被移出群」——那正是他最需要知道的一条。
 *
 * <p>去重键 {@code change|groupId|targetId}，且只在 {@link #DEDUP_WINDOW_MS} 窗口内生效：
 * 同一动作被重复触发（客户端重试）不该二次提示，但窗口之外的同名动作是另一件事
 * （同一个群先后踢两个人、先后改两次名），必须照常通知。
 * 与另外两个通道一样，推送失败静默、无重试——变更已经落库，客户端下次拉群资料仍能纠正。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class GroupEventNotifier {

    private static final Logger log = LoggerFactory.getLogger(GroupEventNotifier.class);

    private static final int TTL_MS = 30_000;
    /** 去重表的硬上限：极端情况下（窗口内几十万条不同动作）整表丢弃，退化成「不去重」也不会出错 */
    private static final int MAX_TRACKED = 200_000;
    /**
     * 同一动作的去重窗口。
     *
     * <p>客户端重试通常发生在几秒内；过了窗口再发生就是新事件（同一个群先后被改两次名），
     * 不能因为「键一样」就永久静音——那会让第二次改名、第二次踢人整条事件消失。
     */
    private static final long DEDUP_WINDOW_MS = 3_000;
    /**
     * 去重表的清理水位。
     *
     * <p>键是「群 × 被操作者」，会随群的治理历史一直增长；键本身带窗口语义，所以过期条目
     * 必须回收。但每条事件都扫一遍全表太重，改成「超过水位 + 距上次清理过了窗口」才扫一次，
     * 摊还下来是 O(1)，稳态下表的规模就是「窗口内的动作数」。
     */
    private static final int PRUNE_THRESHOLD = 1_024;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final boolean enabled;
    private final long dedupWindowMs;
    private final int pruneThreshold;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** {@code change|groupId|targetId} → 上次推送时间 */
    private final Map<String, Long> recent = new ConcurrentHashMap<>();
    /** 上次清理去重表的时间；避免高频写入时反复全表扫描 */
    private final AtomicLong lastPruneMillis = new AtomicLong();

    /**
     * 容器用的构造器。
     *
     * <p>类里有两个构造器（另一个是给单测注入窗口/水位的），所以这里必须显式标 {@link Autowired}：
     * Spring 只在「唯一构造器」时才自动选用，多个且都没标注时会退回无参构造器，
     * 而这个类没有无参构造器——症状是启动直接失败在 {@code No default constructor found}。
     */
    @Autowired
    public GroupEventNotifier(ClientFrames frames,
                              ObjectProvider<PushPort> pushPortProvider,
                              @Value("${im.group-events.enabled:true}") boolean enabled) {
        this(frames, pushPortProvider, enabled, DEDUP_WINDOW_MS, PRUNE_THRESHOLD);
    }

    /** 窗口与水位可注入：让去重与回收能被单测精确验证，生产走上面的常量 */
    GroupEventNotifier(ClientFrames frames, ObjectProvider<PushPort> pushPortProvider, boolean enabled,
                       long dedupWindowMs, int pruneThreshold) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.enabled = enabled;
        this.dedupWindowMs = dedupWindowMs;
        this.pruneThreshold = pruneThreshold;
        log.info("[group] 群变更事件（group.update）{}", enabled ? "已开启" : "已关闭");
    }

    /**
     * 广播一条群变更。
     *
     * @param change   {@link ClientFrames#GROUP_MEMBER_REMOVED} 等
     * @param actorId  发起人
     * @param targetId 被操作者（可为 null）
     * @param targets  接收方（通常是群成员 + 被操作者）
     */
    public void notifyChange(String groupId, String change, String actorId, String targetId,
                             Collection<String> targets) {
        if (!enabled || groupId == null || change == null || targets == null || targets.isEmpty()) {
            return;
        }
        // 去重按「动作」而不是「收件人」：按收件人去重时，同一个群的第二次踢人会被整条跳过——
        // 全群（包括刚被踢的那个人）都收不到任何提示，而他从成员集合里已经消失了
        if (!mark(change, groupId, targetId)) {
            return;
        }
        List<String> recipients = new ArrayList<>(targets.size());
        for (String target : new LinkedHashSet<>(targets)) {
            if (target != null && !target.isBlank()) {
                recipients.add(target);
            }
        }
        if (recipients.isEmpty()) {
            return;
        }
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return;
        }
        String frame = frames.groupUpdateFrame(groupId, change, actorId, targetId, System.currentTimeMillis());
        executor.execute(() -> {
            try {
                pushPort.push(PushTask.newBuilder()
                        .setTraceId("group-" + change + "-" + groupId)
                        .addAllTargetUserIds(recipients)
                        .setFrame(frame)
                        .setTtlMs(TTL_MS)
                        .setConversationId("g:" + groupId)
                        .build());
            } catch (RuntimeException e) {
                log.debug("[group] 群变更推送失败（客户端下次拉群资料会纠正）group={} change={} err={}",
                        groupId, change, e.getMessage());
            }
        });
    }

    /**
     * 记录一次动作，并回答「要不要推」。
     *
     * @return 窗口内重复发生返回 {@code false}（视为客户端重试）
     */
    private boolean mark(String change, String groupId, String targetId) {
        long now = System.currentTimeMillis();
        String key = change + '|' + groupId + '|' + (targetId == null ? "" : targetId);
        Long previous = recent.put(key, now);
        // 先写再清理：本次动作自己的时间戳不会把自己判成过期
        pruneIfDue(now);
        return previous == null || now - previous >= dedupWindowMs;
    }

    /**
     * 清掉已经出窗口的键。
     *
     * <p>触发条件是「表超过水位」且「距上次清理超过一个窗口」——两个条件都是为了避免
     * 每条事件都扫全表；窗口内的键本来就还在生效，扫了也删不掉。
     */
    private void pruneIfDue(long now) {
        if (recent.size() <= pruneThreshold) {
            return;
        }
        long lastPrune = lastPruneMillis.get();
        if (now - lastPrune < dedupWindowMs || !lastPruneMillis.compareAndSet(lastPrune, now)) {
            return;
        }
        recent.entrySet().removeIf(entry -> now - entry.getValue() >= dedupWindowMs);
        if (recent.size() > MAX_TRACKED) {
            recent.clear();
        }
    }

    /** 去重表里记着的动作数（观测/测试用） */
    public int tracked() {
        return recent.size();
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
