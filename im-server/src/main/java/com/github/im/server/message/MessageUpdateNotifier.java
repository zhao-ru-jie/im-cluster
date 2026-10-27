package com.github.im.server.message;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushTask;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 消息变更事件（撤回 / 本地删除）的下行广播。
 *
 * <h2>与 {@link MessageStateNotifier} 的分工</h2>
 *
 * <p>两个通知器共用同一条下行通道（{@code PushPort} + {@link ClientFrames}），但<b>去重口径不同</b>：
 *
 * <table border="1">
 *   <caption>两个通知器的差别</caption>
 *   <tr><th></th><th>{@code MessageStateNotifier}</th><th>{@code MessageUpdateNotifier}</th></tr>
 *   <tr><td>帧</td><td>{@code msg.state}</td><td>{@code msg.update}</td></tr>
 *   <tr><td>语义</td><td>走到哪一步（DELIVERED / READ）</td><td>内容或可见性变了（RECALLED / DELETED）</td></tr>
 *   <tr><td>单调性</td><td>单调前进，<b>只推新高</b></td><td>不单调（可撤回任意 seq），<b>只推一次</b></td></tr>
 *   <tr><td>范围</td><td>单聊对端</td><td>撤回=会话内所有人；删除=操作者自己的其他端</td></tr>
 *   <tr><td>去重键</td><td>{@code state|conversationId}</td><td>{@code change|conversationId|seq|userId}</td></tr>
 * </table>
 *
 * <h2>为什么要「只推一次」</h2>
 *
 * <p>撤回在存储层已经是幂等的（{@code recall} 返回 false 就不广播），但并发下仍可能出现
 * 「两个实例几乎同时判定为首次」的窗口。多推一条变更帧的后果不是错误显示，而是
 * <b>客户端多一次无效动画/日志</b>——所以这里只做进程内去重，不追求跨实例强一致：
 * 跨实例去重要么加分布式锁，要么把事件写进队列，代价远大于收益。
 *
 * <p>推送失败静默：变更事件丢了，客户端下次拉历史依然能看到正确状态（撤回/删除都落在存储上，
 * 读路径统一脱敏）。这一点与 {@code msg.state} 一致——都遵循「存储是权威，事件只是加速」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class MessageUpdateNotifier {

    private static final Logger log = LoggerFactory.getLogger(MessageUpdateNotifier.class);

    /** 与状态回执一致：事件自带「下次拉历史会纠正」的自愈能力，TTL 不必长 */
    private static final int TTL_MS = 30_000;

    /** 去重表上限：超了整体丢掉重来，宁可多推几条也不让一张 map 无限长 */
    private static final int MAX_TRACKED = 200_000;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final boolean enabled;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** {@code change|conversationId|seq|userId}：这条事件已经推给这个用户了 */
    private final Set<String> pushed = ConcurrentHashMap.newKeySet();

    public MessageUpdateNotifier(ClientFrames frames,
                                 ObjectProvider<PushPort> pushPortProvider,
                                 @Value("${im.message-events.enabled:true}") boolean enabled) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.enabled = enabled;
        log.info("[update] 消息变更事件（msg.update）{}", enabled ? "已开启" : "已关闭");
    }

    /**
     * 广播一条变更事件。
     *
     * @param change  {@link ClientFrames#CHANGE_RECALLED} 或 {@link ClientFrames#CHANGE_DELETED}
     * @param targets 接收方（撤回=会话成员；删除=操作者的其它端）
     */
    public void notifyChange(String conversationId, String convType, long seq, String change,
                             String actorId, Collection<String> targets) {
        if (!enabled || seq <= 0 || conversationId == null || change == null
                || targets == null || targets.isEmpty()) {
            return;
        }
        List<String> recipients = new ArrayList<>(targets.size());
        for (String target : new LinkedHashSet<>(targets)) {
            if (target == null || target.isBlank()) {
                continue;
            }
            if (mark(change, conversationId, seq, target)) {
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
        String frame = frames.updateFrame(conversationId, convType, seq, change, actorId,
                System.currentTimeMillis());
        String traceId = "update-" + change + "-" + conversationId + "-" + seq;
        executor.execute(() -> {
            try {
                pushPort.push(PushTask.newBuilder()
                        .setTraceId(traceId)
                        .addAllTargetUserIds(recipients)
                        .setFrame(frame)
                        .setTtlMs(TTL_MS)
                        .setConversationId(conversationId)
                        .build());
            } catch (RuntimeException e) {
                log.debug("[update] 变更事件推送失败（下次拉历史会看到正确状态）conv={} seq={} change={} err={}",
                        conversationId, seq, change, e.getMessage());
            }
        });
    }

    /** @return true = 这次真的需要推（该用户还没收到过这条事件） */
    private boolean mark(String change, String conversationId, long seq, String userId) {
        if (pushed.size() > MAX_TRACKED) {
            pushed.clear();
        }
        return pushed.add(change + '|' + conversationId + '|' + seq + '|' + userId);
    }

    /** 已记录的事件数（观测/测试用） */
    public int tracked() {
        return pushed.size();
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
