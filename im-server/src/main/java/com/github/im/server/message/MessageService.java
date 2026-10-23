package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.MessageEnvelopes;
import com.github.im.contract.MessageIngress;
import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * 消息受理主流程（IM 服务的业务核心，也是 {@link MessageIngress} 的本地实现）。
 *
 * <p>处理顺序严格遵守「<b>先落库、后推送</b>」：
 * <ol>
 *   <li>幂等判定：{@code (from, clientMsgId)} 命中直接回放上次结果（seq + serverMsgId），不重复落库；</li>
 *   <li>分配会话内 seq（{@link SeqAllocator} 逐条 Redis 原子递增，保序的唯一依据）；</li>
 *   <li>生成服务端消息 ID（{@link MessageIds}，UUIDv7，与 seq <b>解耦</b>）；</li>
 *   <li>写消息存储（write-ahead，经 {@link MessageRepository} 落结构化记录，失败即整体失败）；</li>
 *   <li>通过 {@code PushPort} 尽力推送，失败的 userId 落离线队列。</li>
 * </ol>
 * 因此「推送失败」只影响实时性，不会丢消息；客户端上线后按 seq 拉齐。
 *
 * <p><b>为什么 ID 与 seq 要解耦</b>：seq 的权威状态在 Redis，可能丢（重启/淘汰/切主）。
 * 若 ID 由 {@code conversationId#seq} 派生，seq 回绕就等于消息 ID 撞车；拆开后
 * 「唯一性」交给 UUIDv7（本地生成、不回绕），「顺序性」交给 seq（只要求会话内单调、
 * 允许有洞），两者的故障域彻底分开。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Service
public class MessageService implements MessageIngress {

    private static final Logger log = LoggerFactory.getLogger(MessageService.class);

    /** 幂等键 / 序号 key 见 {@link RedisKeys}（跨服务共享的 key 契约） */
    private static final Duration IDEM_TTL = Duration.ofHours(24);
    /** 幂等表里同时存「消息 ID 与 seq」，两者已解耦、无法互相推导，必须一起回放 */
    private static final String IDEM_SEP = "|";
    private static final int DEFAULT_PULL_MAX = 200;

    private static final int CODE_OK = 0;
    private static final int CODE_BAD_REQUEST = 400;
    /** 可重试：网关据此让客户端退避重发 */
    private static final int CODE_UNAVAILABLE = 503;

    private final StringRedisTemplate redis;
    private final MessageRepository messageRepository;
    private final OfflineMessageStore offlineMessageStore;
    private final ClientFrames frames;
    private final MessageRouter router;
    private final SeqAllocator seqAllocator;
    private final ClientWatermarkStore watermarkStore;

    public MessageService(StringRedisTemplate redis,
                          MessageRepository messageRepository,
                          OfflineMessageStore offlineMessageStore,
                          ClientFrames frames,
                          MessageRouter router,
                          SeqAllocator seqAllocator,
                          ClientWatermarkStore watermarkStore) {
        this.redis = redis;
        this.messageRepository = messageRepository;
        this.offlineMessageStore = offlineMessageStore;
        this.frames = frames;
        this.router = router;
        this.seqAllocator = seqAllocator;
        this.watermarkStore = watermarkStore;
    }

    @Override
    public SubmitMessageResponse submit(SubmitMessageRequest request) {
        long receivedAt = System.currentTimeMillis();
        MessageEnvelope envelope;
        try {
            envelope = MessageEnvelopes.toEnvelope(request);
        } catch (IllegalArgumentException e) {
            return SubmitMessageResponse.newBuilder()
                    .setCode(CODE_BAD_REQUEST)
                    .setMessage(e.getMessage())
                    .setClientMsgId(request.getClientMsgId())
                    .build();
        }

        try {
            String idemKey = RedisKeys.IDEM_PREFIX + envelope.idempotencyKey();
            String cached = redis.opsForValue().get(idemKey);
            if (cached != null) {
                IdempotentHit hit = IdempotentHit.parse(envelope.conversationId(), cached);
                log.debug("duplicate message {} -> seq {}", envelope.idempotencyKey(), hit.seq());
                return response(envelope, hit.serverMsgId(), hit.seq(), receivedAt, true);
            }

            // 只算一次：客户端没带 traceId 时这里会生成，多算一次就会「日志一个 ID、推送另一个 ID」
            String traceId = traceId(request);
            long seq = seqAllocator.next(envelope.conversationId());
            String serverMsgId = MessageIds.next();
            String frame = frames.pushFrame(envelope, serverMsgId, seq, receivedAt);

            // 落库存结构化的 StoredMessage（不是下行帧）：建唯一索引做幂等、按 conversation_id+seq 拉漫游，
            // 帧只在读路径上用 ClientFrames 重建。换成 MySQL 只需 im.storage=jdbc，本流程不动。
            messageRepository.append(new StoredMessage(
                    envelope.conversationId(), seq, serverMsgId,
                    envelope.from(), envelope.to(), envelope.convType(),
                    envelope.clientMsgId(), envelope.body(), receivedAt));
            redis.opsForValue().set(idemKey, serverMsgId + IDEM_SEP + seq, IDEM_TTL);

            List<String> failed = router.deliver(envelope, frame, traceId);
            for (String userId : failed) {
                offlineMessageStore.store(userId, frame);
            }
            log.info("message accepted: traceId={} conv={} seq={} msgId={} from={} to={} offline={}",
                    traceId, envelope.conversationId(), seq, serverMsgId,
                    envelope.from(), envelope.to(), failed.size());
            return response(envelope, serverMsgId, seq, receivedAt, false);
        } catch (RuntimeException e) {
            // 竞态下的重复插入由唯一索引兜底（生产环境），这里按可重试返回
            log.error("submit message from {} failed: {}", envelope.from(), e.getMessage(), e);
            return SubmitMessageResponse.newBuilder()
                    .setCode(CODE_UNAVAILABLE)
                    .setMessage("internal error: " + e.getMessage())
                    .setClientMsgId(envelope.clientMsgId())
                    .build();
        }
    }

    /**
     * 网关侧 REST 测试入口复用同一套受理流程，避免出现「测试通路绕过业务」。
     */
    public SubmitMessageResponse dispatch(MessageEnvelope envelope) {
        return submit(MessageEnvelopes.toRequest(envelope, null, null, null));
    }

    @Override
    public PullPendingResponse pullPending(PullPendingRequest request) {
        String userId = request.getUserId();
        if (!StringUtils.hasText(userId)) {
            return PullPendingResponse.getDefaultInstance();
        }
        // 先登记客户端上报的消费水位，再补发离线帧：这个接口顺带承担「上报水位」的职责，
        // 让发号器多一道与 Redis 无关的下界（见 ClientWatermarkStore 的说明）。
        watermarkStore.observe(request.getWatermarksMap());
        int max = request.getMax() > 0 ? request.getMax() : DEFAULT_PULL_MAX;
        List<String> frames = offlineMessageStore.drain(userId, max);
        return PullPendingResponse.newBuilder()
                .addAllFrames(frames)
                .setRemaining((int) offlineMessageStore.size(userId))
                .build();
    }

    /**
     * 取本次受理的链路追踪 ID：优先用网关透传的 traceId（客户端带来的或网关生成的），缺了才现场生成。
     *
     * <p>以前 {@code PushTask.trace_id} 里装的是 {@code serverMsgId}——那是消息标识，
     * 既搜不到请求也串不起链路，等于「看着有追踪、实际查不出东西」。改成真正的 traceId 后，
     * 同一个 ID 会出现在：网关日志 → 上行 RPC → 受理日志 → 下行 PushTask → 网关推送日志。
     */
    private static String traceId(SubmitMessageRequest request) {
        if (StringUtils.hasText(request.getTraceId())) {
            return request.getTraceId();
        }
        return UUID.randomUUID().toString().replace("-", "");
    }

    /**
     * 封装响应内容
     * @param envelope 信封
     * @param serverMsgId 服务端消息 ID（UUIDv7，与 seq 无推导关系）
     * @param seq 序号
     * @param receivedAt 收到时间
     * @param duplicate 是否重复
     * @return 响应内容
     */
    private SubmitMessageResponse response(MessageEnvelope envelope, String serverMsgId,
                                           long seq, long receivedAt, boolean duplicate) {
        return SubmitMessageResponse.newBuilder()
                .setCode(CODE_OK)
                .setMessage(duplicate ? "duplicate" : "ok")
                .setServerMsgId(serverMsgId)
                .setSeq(seq)
                .setServerTs(receivedAt)
                .setClientMsgId(envelope.clientMsgId())
                .setDuplicate(duplicate)
                .build();
    }

    /** 幂等命中时要原样回放的两样东西 */
    private record IdempotentHit(String serverMsgId, long seq) {

        /**
         * 兼容解析：新格式为 {@code "{serverMsgId}|{seq}"}；旧格式只存了 seq，
         * 此时按旧的 {@code conversationId#seq} 规则还原 ID，让升级瞬间的历史幂等记录继续有效。
         */
        static IdempotentHit parse(String conversationId, String cached) {
            int sep = cached.lastIndexOf(IDEM_SEP);
            if (sep > 0) {
                return new IdempotentHit(cached.substring(0, sep), Long.parseLong(cached.substring(sep + 1)));
            }
            long seq = Long.parseLong(cached);
            return new IdempotentHit(conversationId + "#" + seq, seq);
        }
    }
}
