package com.github.im.server.relation;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 好友申请相关的下行通知（{@code relation.request} / {@code relation.update}）。
 *
 * <h2>为什么申请流必须要这一层</h2>
 *
 * <p>没有它的时候，「加好友」这个动作只落在服务端存储里：被申请人必须主动刷新通讯录
 * 才知道有人在等他同意，而同意/拒绝的结果申请人也永远不会收到 —— 他会一直看到
 * 「等待验证」，直到下次冷启动重新拉列表。申请流的价值全在<b>实时性</b>上，
 * 慢半拍的申请等于没有申请。
 *
 * <h2>为什么不做去重</h2>
 *
 * <p>与 {@code GroupEventNotifier} 不同，这里的重复推送从源头就被堵住了：
 * <ul>
 *   <li>同一对用户只有一条申请记录，重复申请只更新附言、{@link RelationService} 不再推送；</li>
 *   <li>accept/reject 是终态迁移，第二次调用直接返回 404，不会再触发通知。</li>
 * </ul>
 * 再加一张去重表只会增加一处「窗口内的合法事件被吞掉」的风险。
 *
 * <h2>失败静默</h2>
 *
 * <p>推送失败不重试也不回滚：申请已经落库，对方下次打开「新的朋友」列表一样能看到；
 * 而把「通知没发出去」变成「加好友失败」才是真正的错——用户会以为对方拒绝了他。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class RelationEventNotifier {

    private static final Logger log = LoggerFactory.getLogger(RelationEventNotifier.class);

    /** 申请类通知的存活期：入口已经落库，过久送达只会在对方列表里造成一次「晚到的提示」 */
    private static final int TTL_MS = 30_000;

    private final ClientFrames frames;
    private final ObjectProvider<PushPort> pushPortProvider;
    private final boolean enabled;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public RelationEventNotifier(ClientFrames frames,
                                 ObjectProvider<PushPort> pushPortProvider,
                                 @Value("${im.relation-events.enabled:true}") boolean enabled) {
        this.frames = frames;
        this.pushPortProvider = pushPortProvider;
        this.enabled = enabled;
        log.info("[relation] 好友申请通知（relation.request / relation.update）{}", enabled ? "已开启" : "已关闭");
    }

    /**
     * 「有人申请加你好友」。
     *
     * @param fromId  申请人
     * @param toId    被申请人（收件人）
     * @param message 验证消息，可为空
     */
    public void notifyRequest(String fromId, String toId, String message) {
        push(toId, "relation.request", frames.friendRequestFrame(fromId, message, System.currentTimeMillis()),
                "request-" + fromId + "->" + toId);
    }

    /**
     * 「你的申请被处理了」/「对方的申请撤回了」。
     *
     * @param recipientId 收件人（ACCEPTED/REJECTED 时是申请人；CANCELLED 时是被申请人）
     * @param peerId      另一方
     * @param change      {@link ClientFrames#RELATION_ACCEPTED} 等
     */
    public void notifyUpdate(String recipientId, String peerId, String change) {
        push(recipientId, "relation.update", frames.relationUpdateFrame(peerId, change, System.currentTimeMillis()),
                "update-" + change + "-" + peerId + "->" + recipientId);
    }

    private void push(String recipientId, String trace, String frame, String traceId) {
        if (!enabled || recipientId == null || recipientId.isBlank()) {
            return;
        }
        PushPort pushPort = pushPortProvider == null ? null : pushPortProvider.getIfAvailable();
        if (pushPort == null) {
            return;
        }
        List<String> recipients = List.of(recipientId);
        executor.execute(() -> {
            try {
                pushPort.push(PushTask.newBuilder()
                        .setTraceId(traceId)
                        .addAllTargetUserIds(recipients)
                        .setFrame(frame)
                        .setTtlMs(TTL_MS)
                        .build());
            } catch (RuntimeException e) {
                log.debug("[relation] {} 推送失败（列表接口兜底，不影响结果）to={} err={}",
                        trace, recipientId, e.getMessage());
            }
        });
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
