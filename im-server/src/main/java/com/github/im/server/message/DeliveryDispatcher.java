package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.VendorPushPort;
import com.github.im.contract.VendorPushRequest;
import com.github.im.server.metrics.MessageMetrics;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
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

    /** 离线通知的接收方上限：一个 500 人群九成不在线时，逐人推等于自己给自己限流 */
    private static final int VENDOR_PUSH_MAX_TARGETS = 200;
    /** 通知正文截断长度：厂商侧对 payload 也有长度限制，摘要只用来让用户判断要不要点开 */
    private static final int VENDOR_PUSH_MAX_CHARS = 80;

    private final MessageRouter router;
    private final OfflineMessageStore offlineStore;
    /** 服务端指标：registry 为 null 时退化为空操作 */
    private final MessageMetrics metrics;
    /** 可空：未开启厂商推送时没有任何实现，这条通道整体消失 */
    private final VendorPushPort vendorPush;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    /** 并发上限：拿不到许可就地同步执行，用「慢」换「内存有界」 */
    private final int maxInflight;
    private final Semaphore inflight;

    /** 单测/无厂商推送的部署用：只装配长连接下发 */
    public DeliveryDispatcher(MessageRouter router, OfflineMessageStore offlineStore, int maxInflight) {
        this(router, offlineStore, maxInflight, null, new MessageMetrics(null));
    }

    /** 单测用：接厂商推送但不接 registry */
    public DeliveryDispatcher(MessageRouter router, OfflineMessageStore offlineStore, int maxInflight,
                              ObjectProvider<VendorPushPort> vendorPushProvider) {
        this(router, offlineStore, maxInflight, vendorPushProvider, new MessageMetrics(null));
    }

    @Autowired
    public DeliveryDispatcher(MessageRouter router,
                              OfflineMessageStore offlineStore,
                              @Value("${im.delivery.max-inflight:1024}") int maxInflight,
                              ObjectProvider<VendorPushPort> vendorPushProvider,
                              MessageMetrics metrics) {
        this.router = router;
        this.offlineStore = offlineStore;
        this.metrics = metrics;
        this.vendorPush = vendorPushProvider == null ? null : vendorPushProvider.getIfAvailable();
        // 至少 1：配成 0 会让所有推送都退化成同步，等于改动全白做
        this.maxInflight = Math.max(maxInflight, 1);
        this.inflight = new Semaphore(this.maxInflight);
        log.info("[delivery] 下行推送执行器就绪：虚拟线程池，并发上限 {}，厂商推送 {}",
                this.maxInflight, this.vendorPush == null ? "未开启" : "已开启");
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
        // 落离线：分两条通道，区别只在「放大倍数」（详见 OfflineMessageStore 类注释）
        int stored = 0;
        if (!failed.isEmpty()) {
            try {
                stored = envelope.isGroup()
                        // 群聊：只记「这批人在这个会话上补到哪了」，帧由会话历史重建。
                        // 存整帧的话，一条群消息 = 不在线人数份相同内容（500 人群 9 成不在线就是 450 份），
                        // 且随消息数线性增长。
                        ? offlineStore.recordGroupPending(failed, envelope.conversationId(), seq)
                        // 单聊：一个收件人一份帧，直接存最省，补发也不需要回查历史
                        : offlineStore.storeAll(failed, frame);
            } catch (RuntimeException e) {
                log.error("[delivery] conv={} seq={} 落离线异常（{} 人）: {}",
                        envelope.conversationId(), seq, failed.size(), e.getMessage());
            }
            if (stored < failed.size()) {
                // 落离线都失败只能记 error：Redis 抖动时至少日志里留有 traceId 可追
                log.error("[delivery] conv={} seq={} 落离线只成功 {}/{}",
                        envelope.conversationId(), seq, stored, failed.size());
                // 这批人是真正的「消息可能要靠上线拉历史才看得到」的人群，必须有指标盯住
                metrics.offlineAppendFailed(failed.size() - stored);
            }
        }
        // 厂商推送：连接不在的用户，只有手机厂商的通道能叫醒 App。
        // 放在落离线之后——先保证消息有归宿（离线队列），再谈「提醒」。
        if (!failed.isEmpty()) {
            notifyVendors(envelope, failed, seq);
        }
        if (log.isDebugEnabled()) {
            log.debug("[delivery] conv={} seq={} traceId={} offline={}/{} cost={}ms",
                    envelope.conversationId(), seq, traceId, stored, failed.size(),
                    (System.nanoTime() - start) / 1_000_000);
        }
    }

    /**
     * 给「长连接没投到」的用户发厂商推送。
     *
     * <p>两个上限都是必要的：<b>人数</b>上限防止一个大群九成不在线时把中转服务打爆
     * （那等于自己给自己限流，还会让真实用户的通知排在后面）；<b>正文长度</b>上限是因为
     * 各家厂商对 payload 大小都有硬限制，而通知正文只需要让用户判断「要不要点开」。
     *
     * <p>失败不重试：通知丢了用户下次打开 App 照样能从离线队列拿到消息，
     * 为它维护一个重试队列是拿可靠性去换一件非权威的事。
     */
    private void notifyVendors(MessageEnvelope envelope, List<String> failed, long seq) {
        if (vendorPush == null) {
            return;
        }
        int targets = Math.min(failed.size(), VENDOR_PUSH_MAX_TARGETS);
        if (failed.size() > targets) {
            log.debug("[delivery] conv={} 离线 {} 人，只给前 {} 人发厂商推送",
                    envelope.conversationId(), failed.size(), targets);
        }
        // 群聊的 to 是 groupId，客户端据此显示群名；单聊显示发送方
        String title = envelope.isGroup() ? envelope.to() : envelope.from();
        String body = vendorBodyOf(envelope);
        for (int i = 0; i < targets; i++) {
            try {
                vendorPush.push(new VendorPushRequest(failed.get(i), title, body,
                        envelope.conversationId(), seq, envelope.contentType()));
            } catch (RuntimeException e) {
                // 端口约定是永不抛，这里再兜一层：一条通知不该让整批投递失败
                log.debug("[delivery] 厂商推送异常 conv={} seq={} err={}",
                        envelope.conversationId(), seq, e.getMessage());
                return;
            }
        }
    }

    /** 通知正文：媒体消息用占位符，文本消息截断——理由见 {@link #notifyVendors} */
    private static String vendorBodyOf(MessageEnvelope envelope) {
        String digest = MessageDigests.digestOf(envelope, VENDOR_PUSH_MAX_CHARS);
        return digest.isBlank() ? "你有一条新消息" : digest;
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
