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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * 消息受理主流程（IM 服务的业务核心，也是 {@link MessageIngress} 的本地实现）。
 *
 * <p>处理顺序严格遵守「<b>先落库、后推送</b>」：
 * <ol>
 *   <li>幂等判定 + 发号：<b>一次 Redis 往返</b>同时完成（{@link SeqAllocator#allocate}），命中就直接回放
 *       上次结果（seq + serverMsgId），既不重复落库、也不白耗一个 seq；</li>
 *   <li>生成服务端消息 ID（{@link MessageIds}，UUIDv7，与 seq <b>解耦</b>）；</li>
 *   <li>写消息存储（write-ahead，经 {@link MessageRepository} 落结构化记录，失败即整体失败）；</li>
 *   <li>写幂等记录——<b>必须在落库之后</b>，否则落库失败时客户端重试会命中一个实际不存在的 seq；</li>
 *   <li>通过 {@code PushPort} 尽力推送，失败的 userId 落离线队列。</li>
 * </ol>
 * 因此「推送失败」只影响实时性，不会丢消息；客户端上线后按 seq 拉齐。
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
    /**
     * 单次 pull 最多处理多少个「有待补群消息」的会话。
     *
     * <p>每个会话要查一次消息历史（MySQL 时是一条 SQL），必须封顶，否则一个加了几百个群、
     * 又长期离线的用户可以靠一次 pull 打出几百条查询。超出部分留在指针里，下次 pull 继续
     * （{@code pendingConversations} 按会话 ID 字典序取，保证每轮都能推进）。
     */
    private static final int MAX_PULL_CONVERSATIONS = 20;

    private static final int CODE_OK = 0;
    private static final int CODE_BAD_REQUEST = 400;
    /** 可重试：网关据此让客户端退避重发 */
    private static final int CODE_UNAVAILABLE = 503;

    private final StringRedisTemplate redis;
    private final MessageRepository messageRepository;
    private final OfflineMessageStore offlineMessageStore;
    private final ClientFrames frames;
    private final DeliveryDispatcher delivery;
    private final SeqAllocator seqAllocator;
    private final ClientWatermarkStore watermarkStore;

    public MessageService(StringRedisTemplate redis,
                          MessageRepository messageRepository,
                          OfflineMessageStore offlineMessageStore,
                          ClientFrames frames,
                          DeliveryDispatcher delivery,
                          SeqAllocator seqAllocator,
                          ClientWatermarkStore watermarkStore) {
        this.redis = redis;
        this.messageRepository = messageRepository;
        this.offlineMessageStore = offlineMessageStore;
        this.frames = frames;
        this.delivery = delivery;
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
            // 幂等判定与发号合成一次 Redis 往返（见 SeqAllocator.allocate）：
            // 重复消息连 seq 都不会白耗一个，新消息则省掉一次 GET。
            SeqAllocator.Allocation allocation = seqAllocator.allocate(idemKey, envelope.conversationId());
            if (allocation.duplicate()) {
                IdempotentHit hit = IdempotentHit.parse(envelope.conversationId(), allocation.idemValue());
                log.debug("duplicate message {} -> seq {}", envelope.idempotencyKey(), hit.seq());
                return response(envelope, hit.serverMsgId(), hit.seq(), receivedAt, true);
            }

            // 只算一次：客户端没带 traceId 时这里会生成，多算一次就会「日志一个 ID、推送另一个 ID」
            String traceId = traceId(request);
            long seq = allocation.seq();
            String serverMsgId = MessageIds.next();
            String frame = frames.pushFrame(envelope, serverMsgId, seq, receivedAt);

            // 落库存结构化的 StoredMessage（不是下行帧）：建唯一索引做幂等、按 conversation_id+seq 拉漫游，
            // 帧只在读路径上用 ClientFrames 重建。换成 MySQL 只需 im.storage=jdbc，本流程不动。
            messageRepository.append(new StoredMessage(
                    envelope.conversationId(), seq, serverMsgId,
                    envelope.from(), envelope.to(), envelope.convType(),
                    envelope.clientMsgId(), envelope.body(), receivedAt));
            redis.opsForValue().set(idemKey, serverMsgId + IDEM_SEP + seq, IDEM_TTL);

            // ack 先行：落库 + 幂等记录都已完成就算受理成功，推送交给 DeliveryDispatcher 异步做。
            // 推送失败仍由它落离线队列，所以「先 ack」不丢消息，只是让 ack 不再等最慢的接收者。
            delivery.dispatch(envelope, frame, traceId, seq);
            log.info("message accepted: traceId={} conv={} seq={} msgId={} from={} to={}",
                    traceId, envelope.conversationId(), seq, serverMsgId,
                    envelope.from(), envelope.to());
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

    /**
     * 补发离线消息：两条通道，对应两种放大倍数（详见 {@link OfflineMessageStore} 类注释）。
     *
     * <ol>
     *   <li><b>单聊帧队列</b>：直接 {@code drain} 出来，顺带把升级前写下的群聊整帧也带出去；</li>
     *   <li><b>群聊指针</b>：离线期间只记了「这个人在这个会话上补到哪」，这里按 seq 区间
     *       从会话历史取消息、用 {@link ClientFrames#frameOf} 重建帧。</li>
     * </ol>
     *
     * <p>为什么要用客户端上报的水位兜一层：用户离线时记下指针后，可能又连着收了一段时间的
     * 实时推送（指针不会因为实时推送而前移，少一次每条消息的写）。补发时从
     * {@code max(指针, 客户端水位 + 1)} 开始，避免把已经收到过的消息再补一遍。
     */
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

        List<String> out = new ArrayList<>(max);
        out.addAll(offlineMessageStore.drain(userId, max));
        int pendingConversations = 0;
        if (out.size() < max) {
            pendingConversations = pullGroupPending(userId, request.getWatermarksMap(), max - out.size(), out);
        }
        return PullPendingResponse.newBuilder()
                .addAllFrames(out)
                .setRemaining((int) (offlineMessageStore.size(userId) + pendingConversations))
                .build();
    }

    /**
     * 群聊补发：按会话指针从历史里取 {@code seq > 起点} 的消息并重建帧。
     *
     * <p>两条硬约定：
     * <ul>
     *   <li><b>历史查询失败绝不推进指针</b>：宁可下次多补一遍（客户端按 seq 去重），
     *       也不能把指针推过去，那等于永久丢一段消息；</li>
     *   <li><b>指针只前进</b>：并发 pull（多端同时上线、重连风暴）可能读到同一个旧指针，
     *       各自补发一遍是重复但无害的，回退才会造成死循环。</li>
     * </ul>
     *
     * @param budget 本次还能补多少帧（全局上限，避免一个用户把整轮 pull 撑爆）
     * @return 本次之后仍有待补消息的会话数
     */
    private int pullGroupPending(String userId, Map<String, Long> watermarks, int budget, List<String> out) {
        Map<String, Long> pending = offlineMessageStore.pendingConversations(userId, MAX_PULL_CONVERSATIONS);
        if (pending.isEmpty()) {
            return 0;
        }
        // 超出单轮处理上限的会话数也要计进 remaining，否则客户端会以为已经拉干净了
        int remaining = (int) Math.max(0, offlineMessageStore.pendingConversationCount(userId) - pending.size());
        for (Map.Entry<String, Long> entry : pending.entrySet()) {
            if (budget <= 0) {
                remaining++;
                continue;
            }
            String conversationId = entry.getKey();
            long pendingFrom = entry.getValue();
            long start = Math.max(pendingFrom, watermarks.getOrDefault(conversationId, 0L) + 1);
            List<StoredMessage> messages;
            int limit = budget;
            try {
                messages = messageRepository.since(conversationId, start - 1, limit);
            } catch (RuntimeException e) {
                log.error("[offline] 补拉会话历史失败，指针保持不动待下轮重试 conv={} from={} err={}",
                        conversationId, start, e.getMessage());
                remaining++;
                continue;
            }
            for (StoredMessage message : messages) {
                out.add(frames.frameOf(message));
            }
            budget -= messages.size();
            boolean caughtUp = messages.size() < limit;
            long nextSeq = messages.isEmpty() ? pendingFrom : messages.get(messages.size() - 1).seq() + 1;
            offlineMessageStore.advancePending(userId, conversationId, pendingFrom, nextSeq, caughtUp);
            if (!caughtUp) {
                remaining++;
            }
        }
        return remaining;
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
