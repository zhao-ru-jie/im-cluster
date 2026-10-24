package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * 下行推送的执行器：把推送从 ack 关键路径上摘下来。
 *
 * <h2>为什么要摘</h2>
 *
 * <p>改动前 {@code MessageService.submit} 的顺序是「落库 → 同步推送（等所有网关实例返回）→ 回 ack」。
 * 于是用户的「发送成功」耗时 = 落库 + <b>最慢的那个接收者所在实例</b>的 gRPC 往返；
 * 群聊扇出时还要把多个实例的往返串起来。这些跟「我这条消息有没有被受理」毫无关系——
 * 受理成功的判据只有两个：幂等记录写入 + 落库成功。
 *
 * <h2>ack 先行会不会丢消息</h2>
 *
 * <p>不会。推送失败仍然由本类落离线队列（与改动前完全一样），客户端上线后按 seq 拉齐。
 * 代价只有两条，都在预期内：
 * <ul>
 *   <li>实时到达顺序可能乱（seq=2 比 seq=1 先到）——客户端本来就<b>必须按 seq 排序</b>，
 *       这是 IM 的常规约定，不是新增要求；</li>
 *   <li>进程在「已 ack、未推送」的窗口内崩溃时，这条消息只在库里、不在离线队列里。
 *       所以关闭时会 drain（见 {@link #shutdown()}），且客户端重连后能按 seq 拉历史。</li>
 * </ul>
 *
 * <h2>为什么不无限排队</h2>
 *
 * <p>并发任务超过 {@code im.delivery.max-inflight}（默认 1024）时<b>就地同步执行</b>，
 * 也就是退化成改动前的行为：宁可慢一点，也不能让推送任务把堆内存堆穿——
 * 那时丢的是整条链路，比 ack 慢几十毫秒严重得多。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class DeliveryDispatcher {

    private static final Logger log = LoggerFactory.getLogger(DeliveryDispatcher.class);

    /** 关闭时等待推送完成的上限：这段时间内已 ack 的消息要推出去（或落离线） */
    private static final long SHUTDOWN_WAIT_SECONDS = 5L;

    private final MessageRouter router;
    private final OfflineMessageStore offlineStore;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** 并发上限：拿不到许可就地同步执行，用「慢」换「内存有界」 */
    private final int maxInflight;
    private final Semaphore inflight;

    public DeliveryDispatcher(MessageRouter router,
                              OfflineMessageStore offlineStore,
                              @Value("${im.delivery.max-inflight:1024}") int maxInflight) {
        this.router = router;
        this.offlineStore = offlineStore;
        // 至少 1：配成 0 会让所有推送都退化成同步，等于改动全白做
        this.maxInflight = Math.max(maxInflight, 1);
        this.inflight = new Semaphore(this.maxInflight);
        log.info("[delivery] 下行推送执行器就绪：虚拟线程池，并发上限 {}", this.maxInflight);
    }

    /**
     * 提交一次投递。<b>永不抛异常</b>，也永不丢消息：拿不到并发许可时就地同步执行。
     *
     * @param seq 仅用于日志对齐（这里不参与投递逻辑，seq 由帧自己携带）
     */
    public void dispatch(MessageEnvelope envelope, String frame, String traceId, long seq) {
        if (inflight.tryAcquire()) {
            try {
                executor.execute(() -> {
                    try {
                        deliverNow(envelope, frame, traceId, seq);
                    } finally {
                        inflight.release();
                    }
                });
                return;
            } catch (RejectedExecutionException e) {
                // 只在关闭过程中发生：许可要还回去，然后同步做完
                inflight.release();
                log.debug("[delivery] 执行器已关闭，改为同步投递 conv={} seq={}", envelope.conversationId(), seq);
            }
        }
        deliverNow(envelope, frame, traceId, seq);
    }

    /** 真正的投递 + 失败落离线；异常在这里终结，不能传回已经返回 ack 的调用方 */
    private void deliverNow(MessageEnvelope envelope, String frame, String traceId, long seq) {
        long start = System.nanoTime();
        List<String> failed;
        try {
            failed = router.deliver(envelope, frame, traceId);
        } catch (RuntimeException e) {
            // MessageRouter 内部已经兜过一层；这是最后一道，防止推送异常把消息彻底丢掉
            log.warn("[delivery] conv={} seq={} 路由异常，接收方按离线处理: {}",
                    envelope.conversationId(), seq, e.getMessage());
            failed = List.of(envelope.to());
        }
        int stored = 0;
        for (String userId : failed) {
            try {
                offlineStore.store(userId, frame);
                stored++;
            } catch (RuntimeException e) {
                // 落离线都失败只能记 error：Redis 抖动时至少日志里留有 traceId 可追
                log.error("[delivery] conv={} seq={} user={} 落离线失败: {}",
                        envelope.conversationId(), seq, userId, e.getMessage());
            }
        }
        if (log.isDebugEnabled()) {
            log.debug("[delivery] conv={} seq={} traceId={} offline={}/{} cost={}ms",
                    envelope.conversationId(), seq, traceId, stored, failed.size(),
                    (System.nanoTime() - start) / 1_000_000);
        }
    }

    /** 当前占用中的并发许可数（{@code 上限 - 剩余}），供观测/测试 */
    public int inFlight() {
        return maxInflight - inflight.availablePermits();
    }

    /**
     * 关闭前排空：已 ack 的消息必须推出去（或落离线），否则客户端会「拿到了 seq 却收不到消息，
     * 离线队列里也没有」——那才是真正的丢消息。
     */
    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("[delivery] {}s 内仍有推送未完成，强制结束（这部分由客户端按 seq 拉历史兜底）",
                        SHUTDOWN_WAIT_SECONDS);
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
        log.info("[delivery] 下行推送执行器已停止");
    }
}
