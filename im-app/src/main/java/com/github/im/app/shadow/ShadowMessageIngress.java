package com.github.im.app.shadow;

import com.github.im.contract.MessageIngress;
import com.github.im.proto.ingress.v1.AckPendingRequest;
import com.github.im.proto.ingress.v1.AckPendingResponse;
import com.github.im.proto.ingress.v1.BatchSubmitRequest;
import com.github.im.proto.ingress.v1.BatchSubmitResponse;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;

/**
 * Shadow 双跑：同一上行消息同时走「本地直调」与「远程 gRPC」，比对结果，<b>只以本地为准</b>。
 *
 * <p>这是阶段 2 切流前的验证手段，验证的是<b>新传输通道的语义等价性</b>：
 * 序列化是否正确、幂等键是否原样传递、seq 是否一致、幂等回放是否返回同一个 serverMsgId、
 * 错误码是否可比。
 * 落库差异需要「双套独立 Redis/DB 的 Shadow 环境」才能验证，单体联调下两条路径共享同一 Redis，
 * 因此 shadow 侧必然命中幂等回放（{@code duplicate=true}）——这是<b>预期</b>，不算差异。
 *
 * <p>关键约束：
 * <ul>
 *   <li>shadow 在<b>虚拟线程池异步</b>执行，不阻塞主路径、不影响返回值、失败只记指标；</li>
 *   <li>{@code pullPending} <b>绝不做 shadow</b>：拉取有消费语义，双跑会把离线消息消费两次。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class ShadowMessageIngress implements MessageIngress {

    private static final Logger log = LoggerFactory.getLogger(ShadowMessageIngress.class);

    private final MessageIngress primary;
    private final MessageIngress shadow;
    private final double sampleRate;

    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    private final LongAdder primaryCalls = new LongAdder();
    private final LongAdder shadowCalls = new LongAdder();
    private final LongAdder matched = new LongAdder();
    private final LongAdder mismatched = new LongAdder();
    private final LongAdder shadowErrors = new LongAdder();
    private final LongAdder shadowLatencyMs = new LongAdder();

    public ShadowMessageIngress(MessageIngress primary, MessageIngress shadow, double sampleRate) {
        this.primary = primary;
        this.shadow = shadow;
        this.sampleRate = sampleRate <= 0 ? 1.0 : Math.min(sampleRate, 1.0);
    }

    @Override
    public SubmitMessageResponse submit(SubmitMessageRequest request) {
        primaryCalls.increment();
        // 主路径异常直接向上抛：由网关转成 503 可重试，shadow 不得吞掉主路径故障
        SubmitMessageResponse primaryResult = primary.submit(request);
        if (sampled()) {
            executor.execute(() -> compareOne(request, primaryResult));
        }
        return primaryResult;
    }

    @Override
    public BatchSubmitResponse submitBatch(BatchSubmitRequest request) {
        primaryCalls.increment();
        BatchSubmitResponse primaryResult = primary.submitBatch(request);
        if (sampled()) {
            executor.execute(() -> {
                try {
                    BatchSubmitResponse shadowResult = shadow.submitBatch(request);
                    int size = Math.min(primaryResult.getResultsCount(), shadowResult.getResultsCount());
                    for (int i = 0; i < size; i++) {
                        record(primaryResult.getResults(i), shadowResult.getResults(i), request.getMessages(i));
                    }
                    if (primaryResult.getResultsCount() != shadowResult.getResultsCount()) {
                        mismatched.increment();
                        log.warn("[shadow] batch size mismatch: primary={} shadow={}",
                                primaryResult.getResultsCount(), shadowResult.getResultsCount());
                    }
                } catch (Exception e) {
                    onShadowError(e);
                }
            });
        }
        return primaryResult;
    }

    /**
     * 拉取不双跑。
     *
     * <p>即使现在 `pullPending` 已经改成「读不删 + 确认后推进」，双跑仍然是错的：
     * 两条链路会各自算出一份「补到哪」，而 ack 只回传给 primary，shadow 侧那份永远停在原地，
     * 白白占着补发预算。
     */
    @Override
    public PullPendingResponse pullPending(PullPendingRequest request) {
        return primary.pullPending(request);
    }

    /**
     * 确认同样不双跑，且必须显式转发。
     *
     * <p>{@link MessageIngress#ackPending} 的默认实现是空操作——如果这里不覆盖，
     * shadow 模式下就真的「没人推进游标」，表现为每次重连都把同一批离线消息重发一遍
     * （不丢，但会持续重复）。默认实现存在的意义是给单测替身留余地，不是给装饰器偷懒的。
     */
    @Override
    public AckPendingResponse ackPending(AckPendingRequest request) {
        return primary.ackPending(request);
    }

    private void compareOne(SubmitMessageRequest request, SubmitMessageResponse primaryResult) {
        long start = System.nanoTime();
        try {
            SubmitMessageResponse shadowResult = shadow.submit(request);
            shadowCalls.increment();
            shadowLatencyMs.add((System.nanoTime() - start) / 1_000_000);
            record(primaryResult, shadowResult, request);
        } catch (Exception e) {
            onShadowError(e);
        }
    }

    private void record(SubmitMessageResponse primaryResult, SubmitMessageResponse shadowResult,
                        SubmitMessageRequest request) {
        boolean sameCode = primaryResult.getCode() == shadowResult.getCode();
        boolean sameClientMsgId = Objects.equals(primaryResult.getClientMsgId(), shadowResult.getClientMsgId());
        boolean sameSeq = primaryResult.getSeq() == shadowResult.getSeq();
        // serverMsgId 由 MessageIds 独立生成（UUIDv7），两条链路各自落库时 ID 本就不同，
        // 所以只在 shadow 命中幂等回放（同一条逻辑消息的重复提交）时才要求 ID 完全一致——
        // 那才是「幂等键跨进程语义没漂移」的有效证据。
        boolean idConsistent = shadowResult.getDuplicate()
                ? Objects.equals(primaryResult.getServerMsgId(), shadowResult.getServerMsgId())
                : !primaryResult.getServerMsgId().isBlank();
        if (sameCode && sameClientMsgId && sameSeq && idConsistent) {
            matched.increment();
            return;
        }
        mismatched.increment();
        log.warn("[shadow] MISMATCH clientMsgId={} primary(code={}, seq={}, msgId={}) shadow(code={}, seq={}, msgId={}, dup={})",
                request.getClientMsgId(),
                primaryResult.getCode(), primaryResult.getSeq(), primaryResult.getServerMsgId(),
                shadowResult.getCode(), shadowResult.getSeq(), shadowResult.getServerMsgId(),
                shadowResult.getDuplicate());
    }

    private void onShadowError(Exception e) {
        shadowErrors.increment();
        log.warn("[shadow] shadow call failed (主路径不受影响): {}", e.getMessage());
    }

    private boolean sampled() {
        return sampleRate >= 1.0 || ThreadLocalRandom.current().nextDouble() < sampleRate;
    }

    public Map<String, Object> stats() {
        long compared = matched.sum() + mismatched.sum();
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("transport", "shadow");
        stats.put("sampleRate", sampleRate);
        stats.put("primaryCalls", primaryCalls.sum());
        stats.put("shadowCalls", shadowCalls.sum());
        stats.put("matched", matched.sum());
        stats.put("mismatched", mismatched.sum());
        stats.put("shadowErrors", shadowErrors.sum());
        stats.put("matchRate", compared == 0 ? 0.0 : Math.round(matched.sum() * 10000.0 / compared) / 10000.0);
        stats.put("shadowAvgLatencyMs",
                shadowCalls.sum() == 0 ? 0.0 : Math.round(shadowLatencyMs.sum() * 10.0 / shadowCalls.sum()) / 10.0);
        return stats;
    }

    public void close() {
        executor.shutdownNow();
    }
}
