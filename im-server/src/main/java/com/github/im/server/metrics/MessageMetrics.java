package com.github.im.server.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IM 服务侧的核心指标。
 *
 * <h2>为什么需要它</h2>
 *
 * <p>在此之前服务端是<b>黑盒</b>：网关侧有连接数、背压、上行 gRPC 失败按状态码这些指标，
 * 但它们只能回答「上行变慢了」，回答不了<b>慢在哪一段</b>——发号？落库？会话更新？
 * 还是 Redis/MySQL 本身。线上出问题时只能翻日志估算，而日志里没有耗时分段。
 *
 * <p>这里埋的是「一条消息从受理到落离线」这条链路上的关键分段，配合网关侧指标形成完整归因：
 * 网关说「上行超时」→ 这里说「卡在 persist」→ 再去看 MySQL。
 *
 * <h2>tag 基数</h2>
 *
 * <p>只用<b>有限枚举</b>做 tag（outcome / phase / op / result），绝不把 userId、conversationId
 * 放进 tag：那会让时间序列数量随用户数线性增长，最终把 Prometheus 打挂。
 * 需要按会话排查时用 traceId 查日志，不用指标。
 *
 * <h2>为什么 tag 化的 meter 自己缓存</h2>
 *
 * <p>{@code Counter.builder(...).register(registry)} 每次都会做一次注册表查找。
 * 这些方法都在消息热路径上，所以按 tag 值缓存住 meter 实例（tag 值都是有限枚举，Map 不会无界增长）。
 *
 * <h2>registry 可空</h2>
 *
 * <p>单测里不启动 Spring 上下文，构造时传 {@code null} 即可：所有方法退化为空操作
 * （零开销、不需要 mock）。生产由 Spring 注入 {@code MeterRegistry}（actuator 自动配置；
 * 有 {@code micrometer-registry-prometheus} 时就是 Prometheus 实现）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class MessageMetrics {

    /** 受理结果：正常受理 / 幂等命中 / 被 ACL 拒 / 服务端异常 */
    public static final String OUTCOME_ACCEPTED = "accepted";
    public static final String OUTCOME_DUPLICATE = "duplicate";
    public static final String OUTCOME_REJECTED = "rejected";
    public static final String OUTCOME_ERROR = "error";

    /** 受理链路上的分段，用于定位「慢在哪一段」 */
    public static final String PHASE_SEQ = "seq";
    public static final String PHASE_PERSIST = "persist";
    public static final String PHASE_CONVERSATION = "conversation";
    public static final String PHASE_DISPATCH = "dispatch";

    /** 上行 gRPC 结果 */
    public static final String RPC_OK = "ok";
    public static final String RPC_INVALID = "invalid";
    public static final String RPC_ERROR = "error";

    private static final String TAG_OUTCOME = "outcome";
    private static final String TAG_PHASE = "phase";
    private static final String TAG_OP = "op";
    private static final String TAG_RESULT = "result";

    /** 可空：单测里不建 registry */
    private final MeterRegistry registry;

    private final Timer submitTimer;
    private final Timer deliveryTimer;

    private final Map<String, Counter> submitCounters = new ConcurrentHashMap<>();
    private final Map<String, Timer> phaseTimers = new ConcurrentHashMap<>();
    private final Map<String, Timer> ingressTimers = new ConcurrentHashMap<>();

    private final Counter offlineAppended;
    private final Counter offlineTrimmed;
    private final Counter offlineAppendFailed;
    private final Counter offlinePulled;
    private final Counter recipientsDelivered;
    private final Counter recipientsOffline;
    private final Counter replayFrames;
    private final Counter replaySessions;

    /**
     * 最近一次单聊落离线后观测到的队列深度。
     *
     * <p>是<b>近似值</b>：只在写入路径上采样，没有写入时不更新。用它回答的问题是
     * 「积压是否在持续增长」，这已经够了；精确值要额外一次 {@code ZCARD}，不值得为监控多加一次往返。
     */
    private final AtomicLong offlineDepth = new AtomicLong();

    public MessageMetrics(MeterRegistry registry) {
        this.registry = registry;
        if (registry == null) {
            this.submitTimer = null;
            this.deliveryTimer = null;
            this.offlineAppended = null;
            this.offlineTrimmed = null;
            this.offlineAppendFailed = null;
            this.offlinePulled = null;
            this.recipientsDelivered = null;
            this.recipientsOffline = null;
            this.replayFrames = null;
            this.replaySessions = null;
            return;
        }
        // 分位数在客户端算：Prometheus 侧不必存 histogram bucket，
        // /actuator/prometheus 里直接出现 im_message_submit_duration_seconds{quantile="0.99"}
        this.submitTimer = timer("im.message.submit.duration")
                .description("单条消息从进入 submit 到返回 ack 的耗时（含发号、落库、会话更新）")
                .register(registry);
        this.deliveryTimer = timer("im.message.delivery.duration")
                .description("异步投递耗时（查路由 + 推送 + 落离线），不含厂商推送")
                .register(registry);
        this.offlineAppended = Counter.builder("im.offline.frames.appended")
                .description("写入离线队列的帧数（单聊按人计，群聊按会话补发指针计）")
                .register(registry);
        this.offlineTrimmed = Counter.builder("im.offline.frames.trimmed")
                .description("因超过 im.offline.max-frames 被裁掉的最老帧数（持续增长说明客户端长期不消费）")
                .register(registry);
        this.offlineAppendFailed = Counter.builder("im.offline.append.failed")
                .description("落离线失败的接收方人数（这些人只能靠上线后拉历史补齐）")
                .register(registry);
        this.offlinePulled = Counter.builder("im.offline.frames.pulled")
                .description("补发给客户端的帧数")
                .register(registry);
        this.recipientsDelivered = Counter.builder("im.message.recipients")
                .description("消息接收方按投递结果计数")
                .tag(TAG_RESULT, "delivered")
                .register(registry);
        this.recipientsOffline = Counter.builder("im.message.recipients")
                .description("消息接收方按投递结果计数")
                .tag(TAG_RESULT, "offline")
                .register(registry);
        this.replayFrames = Counter.builder("im.replay.frames")
                .description("新端回放推出的帧数（一次回放 = 最近 M 会话 × N 条）")
                .register(registry);
        this.replaySessions = Counter.builder("im.replay.sessions")
                .description("触发过新端回放的连接次数（设备维度：每台设备只自动回放一次）")
                .register(registry);
        Gauge.builder("im.offline.queue.depth", offlineDepth, AtomicLong::get)
                .description("最近一次写入后观测到的单聊离线队列深度（近似值，见字段注释）")
                .register(registry);
    }

    /** 一条消息的受理结果与总耗时 */
    public void submit(String outcome, long nanos) {
        if (registry == null) {
            return;
        }
        submitCounters.computeIfAbsent(outcome, tag -> Counter.builder("im.message.submit")
                .description("消息受理结果")
                .tag(TAG_OUTCOME, tag)
                .register(registry)).increment();
        submitTimer.record(Duration.ofNanos(nanos));
    }

    /** 受理链路某一段的耗时 */
    public void phase(String phase, long nanos) {
        if (registry == null) {
            return;
        }
        phaseTimers.computeIfAbsent(phase, tag -> timer("im.message.phase.duration")
                .description("受理链路各分段耗时，用来定位瓶颈在哪一段")
                .tag(TAG_PHASE, tag)
                .register(registry)).record(Duration.ofNanos(nanos));
    }

    /** 投递结果：耗时 + 送达/离线人数（一条群消息的成功与失败会同时出现） */
    public void delivery(long nanos, int delivered, int offline) {
        if (registry == null) {
            return;
        }
        deliveryTimer.record(Duration.ofNanos(nanos));
        if (delivered > 0) {
            recipientsDelivered.increment(delivered);
        }
        if (offline > 0) {
            recipientsOffline.increment(offline);
        }
    }

    /** 离线队列写入结果 */
    public void offlineAppended(int frames, long trimmed, long depth) {
        if (registry == null) {
            return;
        }
        if (frames > 0) {
            offlineAppended.increment(frames);
        }
        if (trimmed > 0) {
            offlineTrimmed.increment(trimmed);
        }
        if (depth > 0) {
            offlineDepth.set(depth);
        }
    }

    /** 落离线失败的人数 */
    public void offlineAppendFailed(int users) {
        if (registry == null || users <= 0) {
            return;
        }
        offlineAppendFailed.increment(users);
    }

    /** 补发出去的帧数 */
    public void offlinePulled(int frames) {
        if (registry == null || frames <= 0) {
            return;
        }
        offlinePulled.increment(frames);
    }

    /**
     * 新端回放：帧数 + 触发次数。
     *
     * <p>这两个数与 {@code im.offline.frames.pulled} 分开：补发是「欠账在还」，
     * 回放是「新设备开户」。回放次数持续增长说明有设备反复被当成新设备
     * （deviceId 不稳定、或回放标记没落下来），那是要查的信号，混进补发就看不出来了。
     */
    public void replay(int frames, int conversations) {
        if (registry == null || frames <= 0) {
            return;
        }
        replayFrames.increment(frames);
        replaySessions.increment();
    }

    /** 上行 gRPC 入口的耗时与结果（result: ok / invalid / error） */
    public void ingress(String op, long nanos, String result) {
        if (registry == null) {
            return;
        }
        ingressTimers.computeIfAbsent(op + '|' + result, key -> timer("im.ingress.rpc.duration")
                .description("上行 gRPC 入口每个方法的耗时（网关侧指标只能看到整体超时）")
                .tag(TAG_OP, op)
                .tag(TAG_RESULT, result)
                .register(registry)).record(Duration.ofNanos(nanos));
    }

    /** 当前离线队列深度（观测用；未开启 registry 时恒为 0） */
    public long offlineDepth() {
        return offlineDepth.get();
    }

    private static Timer.Builder timer(String name) {
        return Timer.builder(name).publishPercentiles(0.5, 0.95, 0.99);
    }

    /** 便于测试/调试：暴露注册表（可能为 null） */
    public MeterRegistry registry() {
        return registry;
    }
}
