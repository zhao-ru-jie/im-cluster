package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.MessageEnvelopes;
import com.github.im.contract.MessageIngress;
import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.AckPendingRequest;
import com.github.im.proto.ingress.v1.AckPendingResponse;
import com.github.im.proto.ingress.v1.ConversationSummary;
import com.github.im.proto.ingress.v1.DeliveredAck;
import com.github.im.proto.ingress.v1.DeliveredAckRequest;
import com.github.im.proto.ingress.v1.DeliveredAckResponse;
import com.github.im.proto.ingress.v1.ListConversationsRequest;
import com.github.im.proto.ingress.v1.ListConversationsResponse;
import com.github.im.proto.ingress.v1.MarkReadRequest;
import com.github.im.proto.ingress.v1.MarkReadResponse;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.conversation.ConversationStore.ConversationFlag;
import com.github.im.server.conversation.ConversationStore.ConversationMeta;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.metrics.MessageMetrics;
import com.github.im.server.relation.MessageAcl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
    /** 消息不存在（可能已被历史裁剪） */
    private static final int CODE_NOT_FOUND = 404;
    /** 无权执行该操作（不是发送方 / 超出撤回时限） */
    private static final int CODE_FORBIDDEN = 403;
    /** 可重试：网关据此让客户端退避重发 */
    private static final int CODE_UNAVAILABLE = 503;

    /** 会话列表里「最后一条消息摘要」截断长度：列表只用来显示，不该把整条消息拖出来 */
    private static final int DIGEST_MAX_CHARS = 64;
    /** 会话列表默认页大小 */
    private static final int DEFAULT_CONVERSATION_LIMIT = 50;

    /** 历史消息默认页大小（客户端不传 limit 时） */
    private static final int HISTORY_DEFAULT_LIMIT = 50;
    /**
     * 历史消息单页上限。
     *
     * <p>与离线补拉量级（{@code DEFAULT_PULL_MAX}）对齐：一次请求最多重建 200 条帧，
     * 再多就是「一条 HTTP 请求拖垮客户端」的典型写法。要更多可以翻页，游标就是为这个准备的。
     */
    private static final int HISTORY_MAX_LIMIT = 200;

    private final StringRedisTemplate redis;
    private final MessageRepository messageRepository;
    private final OfflineMessageStore offlineMessageStore;
    private final ClientFrames frames;
    private final DeliveryDispatcher delivery;
    private final SeqAllocator seqAllocator;
    private final ClientWatermarkStore watermarkStore;
    private final ConversationStore conversations;
    private final GroupMemberRepository groupMembers;
    /** 可空：只有需要推 msg.state 的部署才装配（单测里直接传 null 即可） */
    private final MessageStateNotifier stateNotifier;
    /** 可空：投递前准入（{@code im.acl.mode=off} 时它内部恒放行，不产生任何往返） */
    private final MessageAcl acl;
    /** 可空：消息变更事件广播（撤回 / 本地删除） */
    private final MessageUpdateNotifier updateNotifier;
    /** 撤回时限：超过之后只有服务端管理员能处理，客户端不再有这个动作 */
    private final Duration recallWindow;
    /** 服务端指标：registry 为 null 时全部退化为空操作，单测直接传 {@code new MessageMetrics(null)} */
    private final MessageMetrics metrics;

    public MessageService(StringRedisTemplate redis,
                          MessageRepository messageRepository,
                          OfflineMessageStore offlineMessageStore,
                          ClientFrames frames,
                          DeliveryDispatcher delivery,
                          SeqAllocator seqAllocator,
                          ClientWatermarkStore watermarkStore,
                          ConversationStore conversations,
                          GroupMemberRepository groupMembers,
                          MessageStateNotifier stateNotifier,
                          MessageAcl acl,
                          MessageUpdateNotifier updateNotifier,
                          @Value("${im.recall.window:2m}") Duration recallWindow,
                          MessageMetrics metrics) {
        this.redis = redis;
        this.messageRepository = messageRepository;
        this.offlineMessageStore = offlineMessageStore;
        this.frames = frames;
        this.delivery = delivery;
        this.seqAllocator = seqAllocator;
        this.watermarkStore = watermarkStore;
        this.conversations = conversations;
        this.groupMembers = groupMembers;
        this.stateNotifier = stateNotifier;
        this.acl = acl;
        this.updateNotifier = updateNotifier;
        this.recallWindow = recallWindow == null ? Duration.ofMinutes(2) : recallWindow;
        this.metrics = metrics;
    }

    @Override
    public SubmitMessageResponse submit(SubmitMessageRequest request) {
        long startedAt = System.nanoTime();
        long receivedAt = System.currentTimeMillis();
        MessageEnvelope envelope;
        try {
            envelope = MessageEnvelopes.toEnvelope(request);
        } catch (IllegalArgumentException e) {
            metrics.submit(MessageMetrics.OUTCOME_REJECTED, System.nanoTime() - startedAt);
            return SubmitMessageResponse.newBuilder()
                    .setCode(CODE_BAD_REQUEST)
                    .setMessage(e.getMessage())
                    .setClientMsgId(request.getClientMsgId())
                    .build();
        }

        // 投递前准入：位置刻意在「幂等 + 发号」之前。被拒的消息不该白耗一个 seq、
        // 不该进幂等表、更不该在离线队列里留下痕迹（否则对方上线会收到一条本不该存在的消息）。
        // off 模式下 MessageAcl 内部直接放行，这里不产生任何 Redis 往返。
        if (acl != null) {
            MessageAcl.Verdict verdict = acl.check(envelope);
            if (!verdict.allowed()) {
                log.debug("message rejected by acl: from={} to={} conv={} reason={}",
                        envelope.from(), envelope.to(), envelope.conversationId(), verdict.reason());
                metrics.submit(MessageMetrics.OUTCOME_REJECTED, System.nanoTime() - startedAt);
                return SubmitMessageResponse.newBuilder()
                        .setCode(verdict.code())
                        .setMessage(verdict.reason())
                        .setClientMsgId(envelope.clientMsgId())
                        .build();
            }
        }

        try {
            // 幂等键挂在会话维度上：它要与 seq 计数器出现在同一个发号脚本里（Cluster 下必须同 slot）
            String idemKey = RedisKeys.convIdem(envelope.conversationId(), envelope.idempotencyKey());
            // 幂等判定与发号合成一次 Redis 往返（见 SeqAllocator.allocate）：
            // 重复消息连 seq 都不会白耗一个，新消息则省掉一次 GET。
            long seqStartedAt = System.nanoTime();
            SeqAllocator.Allocation allocation = seqAllocator.allocate(idemKey, envelope.conversationId());
            metrics.phase(MessageMetrics.PHASE_SEQ, System.nanoTime() - seqStartedAt);
            if (allocation.duplicate()) {
                IdempotentHit hit = IdempotentHit.parse(envelope.conversationId(), allocation.idemValue());
                log.debug("duplicate message {} -> seq {}", envelope.idempotencyKey(), hit.seq());
                metrics.submit(MessageMetrics.OUTCOME_DUPLICATE, System.nanoTime() - startedAt);
                return response(envelope, hit.serverMsgId(), hit.seq(), receivedAt, true);
            }

            // 只算一次：客户端没带 traceId 时这里会生成，多算一次就会「日志一个 ID、推送另一个 ID」
            String traceId = traceId(request);
            long seq = allocation.seq();
            String serverMsgId = MessageIds.next();
            String frame = frames.pushFrame(envelope, serverMsgId, seq, receivedAt);

            // 落库存结构化的 StoredMessage（不是下行帧）：建唯一索引做幂等、按 conversation_id+seq 拉漫游，
            // 帧只在读路径上用 ClientFrames 重建。换成 MySQL 只需 im.storage=jdbc，本流程不动。
            long persistStartedAt = System.nanoTime();
            messageRepository.append(new StoredMessage(
                    envelope.conversationId(), seq, serverMsgId,
                    envelope.from(), envelope.to(), envelope.convType(),
                    envelope.clientMsgId(), envelope.body(), receivedAt,
                    envelope.contentType(), envelope.attrs()));
            redis.opsForValue().set(idemKey, serverMsgId + IDEM_SEP + seq, IDEM_TTL);
            metrics.phase(MessageMetrics.PHASE_PERSIST, System.nanoTime() - persistStartedAt);

            // 会话模型：元信息 + 成员会话列表（写扩散）。放在推送之前，因为
            // 「会话列表里能看到」比「这条消息实时到了」更早被用户感知。
            // 失败不影响受理：消息已落库，补发与历史拉取都不依赖会话列表。
            long convStartedAt = System.nanoTime();
            updateConversation(envelope, serverMsgId, seq, receivedAt);
            metrics.phase(MessageMetrics.PHASE_CONVERSATION, System.nanoTime() - convStartedAt);

            // ack 先行：落库 + 幂等记录都已完成就算受理成功，推送交给 DeliveryDispatcher 异步做。
            // 推送失败仍由它落离线队列，所以「先 ack」不丢消息，只是让 ack 不再等最慢的接收者。
            // 这一段只计「提交投递任务」的开销，真正的投递耗时在 MessageMetrics.delivery 里。
            long dispatchStartedAt = System.nanoTime();
            delivery.dispatch(envelope, frame, traceId, seq);
            metrics.phase(MessageMetrics.PHASE_DISPATCH, System.nanoTime() - dispatchStartedAt);

            log.info("message accepted: traceId={} conv={} seq={} msgId={} from={} to={}",
                    traceId, envelope.conversationId(), seq, serverMsgId,
                    envelope.from(), envelope.to());
            metrics.submit(MessageMetrics.OUTCOME_ACCEPTED, System.nanoTime() - startedAt);
            return response(envelope, serverMsgId, seq, receivedAt, false);
        } catch (RuntimeException e) {
            // 竞态下的重复插入由唯一索引兜底（生产环境），这里按可重试返回
            log.error("submit message from {} failed: {}", envelope.from(), e.getMessage(), e);
            metrics.submit(MessageMetrics.OUTCOME_ERROR, System.nanoTime() - startedAt);
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

    // ==== 会话模型 ====

    /**
     * 更新会话元信息，并把会话推进到每个成员的会话列表。
     *
     * <p>两条写入的顺序是「先元信息，再成员列表」：元信息是权威源（会话现在到 seq 几了），
     * 成员列表只是索引。顺序反过来的话，用户会先看到列表里出现一个 {@code last_seq=0} 的会话。
     */
    private void updateConversation(MessageEnvelope envelope, String serverMsgId, long seq, long time) {
        String conversationId = envelope.conversationId();
        try {
            boolean advanced = conversations.updateMeta(conversationId, envelope.convType(), seq, time,
                    envelope.from(), digestOf(envelope), serverMsgId);
            if (!advanced) {
                // 乱序到达的旧消息（后分配的 seq 先落库）：元信息已经是更新的状态，别再往回推
                return;
            }
            List<String> members = conversationMembers(envelope);
            if (members.isEmpty()) {
                log.warn("[conv] 会话 {} 找不到可写的成员，跳过会话列表扇出", conversationId);
                return;
            }
            // 首次出现在某个成员列表里时，已读基线设成 seq-1：这条消息本身算 1 条未读，
            // 但更早的历史不算——否则新人进一个十万条历史的群，未读数一开始就是 100000
            int touched = conversations.touchAll(members, conversationId, time, Math.max(seq - 1, 0));
            log.debug("[conv] 会话 {} 推进到 seq={}，成员 {} 人（写扩散成功 {} 人）",
                    conversationId, seq, members.size(), touched);
        } catch (RuntimeException e) {
            // 会话列表是「入口」而不是可靠性的组成部分：消息已经落库，
            // 补发与历史拉取都不依赖它，所以这里只记错，不影响受理结果
            log.error("[conv] 更新会话失败 conv={} seq={} err={}", conversationId, seq, e.getMessage());
        }
    }

    /** 单聊取发送方与接收方，群聊取全体成员（群聊的 {@code to} 是 groupId） */
    private List<String> conversationMembers(MessageEnvelope envelope) {
        if ("GROUP".equalsIgnoreCase(envelope.convType())) {
            return groupMembers.membersOf(envelope.to());
        }
        return List.of(envelope.from(), envelope.to());
    }

    /**
     * 会话列表的「最后一条消息摘要」。
     *
     * <p>非文本消息不显示 body：图片/语音的 body 往往是空的或占位符，
     * 列表里显示 {@code [图片]} 比显示一个空字符串有用。这也让「媒体消息」在不解包 attrs
     * 的前提下就能被渲染出来——摘要走的是消息类型，不是媒体地址。
     */
    private static String digestOf(MessageEnvelope envelope) {
        return MessageDigests.digestOf(envelope, DIGEST_MAX_CHARS);
    }

    /**
     * 用户的会话列表，按最后一条消息时间倒序，每条带未读数。
     *
     * <p>大群的成员会话列表在写入时被跳过（写扩散上限），这里用
     * {@code im:group:user:{userId}} 反查补齐——补齐是读时做的，所以大群不需要消息级扇出。
     */
    @Override
    public ListConversationsResponse listConversations(ListConversationsRequest request) {
        String userId = request.getUserId();
        if (!StringUtils.hasText(userId)) {
            return ListConversationsResponse.getDefaultInstance();
        }
        int limit = request.getLimit() > 0 ? request.getLimit() : DEFAULT_CONVERSATION_LIMIT;
        try {
            Set<String> groups = new LinkedHashSet<>(groupMembers.groupsOf(userId));
            List<String> conversationIds = conversations.recentConversations(userId, limit, groups);
            if (conversationIds.isEmpty()) {
                return ListConversationsResponse.getDefaultInstance();
            }
            Map<String, ConversationMeta> metas = conversations.metas(conversationIds);
            Map<String, Long> readSeqs = conversations.readSeqs(userId, conversationIds);
            Map<String, ConversationFlag> flags = conversations.flags(userId, conversationIds);
            List<ConversationSummary> summaries = new ArrayList<>(conversationIds.size());
            for (String conversationId : conversationIds) {
                ConversationMeta meta = metas.getOrDefault(conversationId,
                        ConversationMeta.empty(conversationId));
                long readSeq = readSeqs.getOrDefault(conversationId, 0L);
                ConversationFlag flag = flags.getOrDefault(conversationId, ConversationFlag.NONE);
                summaries.add(ConversationSummary.newBuilder()
                        .setConversationId(conversationId)
                        .setType(meta.type())
                        .setLastSeq(meta.lastSeq())
                        .setLastTime(meta.lastTime())
                        .setLastFrom(meta.lastFrom())
                        .setLastDigest(meta.lastDigest())
                        .setLastMsgId(meta.lastMsgId())
                        .setReadSeq(readSeq)
                        // 游标相减，负数兜成 0（历史上可能出现 readSeq > lastSeq 的旧数据）
                        .setUnread(Math.max(0, meta.lastSeq() - readSeq))
                        .setPinned(flag.pinned())
                        // 免打扰不影响未读数：只是客户端不响铃/不打角标，未读该算还得算
                        .setMuted(flag.muted())
                        .build());
            }
            // 置顶优先，其次按最后消息时间倒序。
            // 反查补齐进来的群 lastTime 是准的，但排在 ZSet 之后，所以这里必须统一重排一遍。
            summaries.sort(Comparator
                    .comparing(ConversationSummary::getPinned).reversed()
                    .thenComparing(Comparator.comparingLong(ConversationSummary::getLastTime).reversed()));
            return ListConversationsResponse.newBuilder().addAllConversations(summaries).build();
        } catch (RuntimeException e) {
            log.error("[conv] 拉取会话列表失败 user={} err={}", userId, e.getMessage());
            return ListConversationsResponse.getDefaultInstance();
        }
    }

    /**
     * 上报已读位置（用户级，只前进）。
     *
     * <p>不区分设备是有意的：已读是「人看到哪了」，手机读完网页的角标就该清零。
     * 需要按设备区分的是<b>同步</b>水位（{@code im:wm:{userId}:{deviceId}}），两件事别混。
     */
    @Override
    public MarkReadResponse markRead(MarkReadRequest request) {
        String userId = request.getUserId();
        String conversationId = request.getConversationId();
        if (!StringUtils.hasText(userId) || !StringUtils.hasText(conversationId)) {
            return MarkReadResponse.newBuilder()
                    .setCode(CODE_BAD_REQUEST)
                    .setMessage("user_id and conversation_id are required")
                    .build();
        }
        try {
            long lastSeq = conversations.meta(conversationId).lastSeq();
            // 先读旧值再推进：只有真的前进了才值得给对端推一条已读回执。
            // 多花一次 HGET 是划算的——上报已读是用户动作（低频），而一条重复回执会让
            // 发送方的气泡闪一下再回到原位。
            long before = conversations.readSeq(userId, conversationId);
            // cap 到会话当前 lastSeq：客户端可能上报未来的 seq（本地乐观计数），不 cap 未读数会变负
            long readSeq = conversations.markRead(userId, conversationId, request.getSeq(), lastSeq);
            if (readSeq > before) {
                notifyState(Map.of(conversationId, readSeq), userId, ClientFrames.STATE_READ);
            }
            return MarkReadResponse.newBuilder()
                    .setCode(CODE_OK)
                    .setMessage("ok")
                    .setReadSeq(readSeq)
                    .setUnread(Math.max(0, lastSeq - readSeq))
                    .build();
        } catch (RuntimeException e) {
            log.error("[conv] 上报已读失败 user={} conv={} err={}", userId, conversationId, e.getMessage());
            return MarkReadResponse.newBuilder()
                    .setCode(CODE_UNAVAILABLE)
                    .setMessage("mark read failed: " + e.getMessage())
                    .build();
        }
    }

    /**
     * 撤回一条消息（会话级，会话内所有人都会看不到原文）。
     *
     * <p>三条规则，都写在这里而不是交给调用方：
     * <ul>
     *   <li><b>只有发送方能撤回</b>。群管理员撤回他人消息需要群角色，那是另一个体系（见已知限制）；</li>
     *   <li><b>有时限</b>（{@code im.recall.window}，默认 2 分钟）。没有时限的撤回等于允许
     *       事后修改双方共同的历史，那是「编辑」而不是「撤回」；</li>
     *   <li><b>幂等</b>：重复撤回返回成功但<b>不再广播</b>——存储层的 {@code recall} 用
     *       {@code recalled_at IS NULL} 判定首次，这一次判定同时挡掉了并发重复广播。</li>
     * </ul>
     *
     * <p>撤回后不再返回原文：消息本体留在存储里（多端要看到「已撤回」占位、审计也要留痕），
     * 但所有读路径（历史、补发、定向拉取）都会经过统一脱敏，把 {@code body}/{@code attrs} 抹掉。
     */
    public MessageChangeResult recall(String conversationId, long seq, String operatorId) {
        if (!StringUtils.hasText(conversationId) || seq <= 0 || !StringUtils.hasText(operatorId)) {
            return MessageChangeResult.reject(CODE_BAD_REQUEST,
                    "conversation_id, seq and operator_id are required");
        }
        StoredMessage message;
        try {
            message = messageRepository.find(conversationId, seq).orElse(null);
        } catch (RuntimeException e) {
            log.error("[recall] 读取消息失败 conv={} seq={} err={}", conversationId, seq, e.getMessage());
            return MessageChangeResult.reject(CODE_UNAVAILABLE, "recall failed: " + e.getMessage());
        }
        if (message == null) {
            return MessageChangeResult.reject(CODE_NOT_FOUND, "message not found");
        }
        if (!operatorId.equals(message.from())) {
            return MessageChangeResult.reject(CODE_FORBIDDEN, "only the sender can recall this message");
        }
        long age = System.currentTimeMillis() - message.createdAt();
        if (age > recallWindow.toMillis()) {
            return MessageChangeResult.reject(CODE_FORBIDDEN,
                    "recall window exceeded (" + recallWindow.toMinutes() + "min)");
        }
        try {
            if (messageRepository.recall(conversationId, seq)) {
                // 撤回要广播给会话内所有人，包括操作者自己的其他端（那台设备上还显示着原文）
                notifyChange(conversationId, seq, ClientFrames.CHANGE_RECALLED, operatorId,
                        membersOf(message));
            }
            log.info("[recall] conv={} seq={} by={}", conversationId, seq, operatorId);
            return MessageChangeResult.ok(seq);
        } catch (RuntimeException e) {
            log.error("[recall] 撤回失败 conv={} seq={} err={}", conversationId, seq, e.getMessage());
            return MessageChangeResult.reject(CODE_UNAVAILABLE, "recall failed: " + e.getMessage());
        }
    }

    /**
     * 本地删除一条消息（用户级：只影响这个用户自己的视图）。
     *
     * <p>与撤回的区别是<b>影响范围</b>，不是实现细节：撤回是会话级（对方也看不到），
     * 删除是用户级（对方照旧）。所以这里只推给操作者自己——目标列表就一个 userId，
     * 由推送层再扇出到他的各台设备。
     *
     * <p>不校验消息是否存在：删除是幂等且允许「删一条服务端已经裁剪掉的消息」的本地动作，
     * 多一次存在性查询换不来任何正确性。
     */
    public MessageChangeResult deleteForUser(String conversationId, long seq, String userId) {
        if (!StringUtils.hasText(conversationId) || seq <= 0 || !StringUtils.hasText(userId)) {
            return MessageChangeResult.reject(CODE_BAD_REQUEST,
                    "conversation_id, seq and user_id are required");
        }
        try {
            messageRepository.markDeleted(userId, conversationId, seq);
            notifyChange(conversationId, seq, ClientFrames.CHANGE_DELETED, userId, List.of(userId));
            return MessageChangeResult.ok(seq);
        } catch (RuntimeException e) {
            log.error("[delete] 删除失败 user={} conv={} seq={} err={}",
                    userId, conversationId, seq, e.getMessage());
            return MessageChangeResult.reject(CODE_UNAVAILABLE, "delete failed: " + e.getMessage());
        }
    }

    /**
     * 会话历史（漫游 + 游标翻页）：带「本地删除过滤」与「撤回脱敏」的读路径。
     *
     * <p>这两个动作都必须在这里生效，否则功能是假的：
     * <ul>
     *   <li>删除只落了一条记录、读时不过滤 —— 用户重装 App 又看到已删除的消息；</li>
     *   <li>撤回只广播了事件、读时不脱敏 —— 断线重连拉历史就把原文重新拉回来了。</li>
     * </ul>
     *
     * <h2>翻页游标为什么用 seq，而不是 offset</h2>
     *
     * <p>seq 是会话内单调的落库序号，新消息只会追加在更大的 seq 上，{@code seq < 游标} 切片天然
     * 不受「翻页期间来了新消息」影响；用 {@code offset} 会因前面插入而整体错位，
     * 表现是「上滑时看到重复或漏掉的消息」。
     *
     * <h2>{@code nextBeforeSeq} 取的是「本页从存储里读到的最老一条」，不是「本页可见的最老一条」</h2>
     *
     * <p>差别在整页都被本地删除过滤掉的情形：如果游标跟着<b>可见</b>消息走，游标永远不动，
     * 客户端会无限重试同一页（看起来像卡死）。跟着<b>存储</b>走则游标一定前进，
     * 客户端最多看到几个空页，一滑就过去了。
     *
     * <p>{@code hasMore} 用「多要一条」探测（{@code limit + 1}）：比在存储侧做 count 便宜，
     * 而且不受过滤影响——它的语义是「更早的存储里还有没有」，客户端据此决定还要不要继续上滑。
     *
     * @param userId    视角用户；为空时不做删除过滤（服务端调试视角）
     * @param beforeSeq 取 {@code seq < beforeSeq} 的一页；{@code <= 0} 表示第一页（最新的一页）
     * @return 本页帧（按 seq 升序）+ 下一页游标 + 是否还有更早的
     */
    public HistoryPage history(String userId, String conversationId, long beforeSeq, int limit) {
        int effectiveLimit = limit <= 0 ? HISTORY_DEFAULT_LIMIT : Math.min(limit, HISTORY_MAX_LIMIT);
        // 多要一条用来判断 hasMore，不额外做一次 count 查询
        List<StoredMessage> fetched = messageRepository.before(conversationId, beforeSeq, effectiveLimit + 1);
        boolean hasMore = fetched.size() > effectiveLimit;
        if (hasMore) {
            // before() 返回升序，多出来的那条在头部（最老的一条）
            fetched = fetched.subList(1, fetched.size());
        }
        if (fetched.isEmpty()) {
            return new HistoryPage(List.of(), 0L, false, effectiveLimit);
        }
        long nextBeforeSeq = fetched.get(0).seq();

        Set<Long> deleted = StringUtils.hasText(userId)
                ? messageRepository.deletedSeqs(userId, conversationId) : Set.of();
        Set<Long> visible = new LinkedHashSet<>(fetched.size());
        for (StoredMessage message : fetched) {
            if (!deleted.contains(message.seq())) {
                visible.add(message.seq());
            }
        }
        Set<Long> recalled = visible.isEmpty()
                ? Set.of() : messageRepository.recalledSeqs(conversationId, visible);
        List<Map<String, Object>> frames = new ArrayList<>(visible.size());
        for (StoredMessage message : fetched) {
            if (deleted.contains(message.seq())) {
                continue;
            }
            frames.add(this.frames.frameMap(message, recalled.contains(message.seq())));
        }
        return new HistoryPage(frames, nextBeforeSeq, hasMore, effectiveLimit);
    }

    /**
     * 一页历史消息。
     *
     * @param messages     本页帧（按 seq 升序；与实时 {@code msg.push} 同构，撤回的条目已脱敏）
     * @param nextBeforeSeq 下一页游标（取 {@code seq < 它}）；{@code 0} = 本页之后没有了
     * @param hasMore      存储里是否还有更早的消息
     * @param limit        服务端实际生效的条数上限（客户端传 0 或超限时被夹取）
     */
    public record HistoryPage(List<Map<String, Object>> messages, long nextBeforeSeq, boolean hasMore, int limit) {
    }

    /** 变更事件的接收方：单聊双方，群聊全体成员 */
    private List<String> membersOf(StoredMessage message) {
        if (MessageEnvelope.CONV_GROUP.equalsIgnoreCase(message.convType())) {
            return groupMembers.membersOf(message.to());
        }
        return List.of(message.from(), message.to());
    }

    private void notifyChange(String conversationId, long seq, String change, String actorId,
                              List<String> targets) {
        if (updateNotifier == null) {
            return;
        }
        updateNotifier.notifyChange(conversationId, convTypeOf(conversationId), seq, change, actorId, targets);
    }

    /**
     * 设置会话标记（置顶 / 免打扰）。
     *
     * <p>参数用 {@code Boolean} 而不是 {@code boolean}：设置页里两个开关是独立的，
     * 只改置顶时不能把免打扰顺手清掉。{@code null} = 这一项不动。
     *
     * @throws IllegalArgumentException 用户或会话 ID 为空
     */
    public ConversationFlag setConversationFlags(String userId, String conversationId,
                                                 Boolean pinned, Boolean muted) {
        if (!StringUtils.hasText(userId) || !StringUtils.hasText(conversationId)) {
            throw new IllegalArgumentException("userId and conversationId are required");
        }
        return conversations.setFlags(userId, conversationId, pinned, muted);
    }

    /**
     * 补发离线消息：两条通道，对应两种放大倍数（详见 {@link OfflineMessageStore} 类注释）。
     *
     * <ol>
     *   <li><b>单聊帧队列</b>：按「该设备已确认的位置」往后读，<b>读完不删</b>；</li>
     *   <li><b>群聊指针</b>：离线期间只记了「这个人在这个会话上补到哪」，这里按 seq 区间
     *       从会话历史取消息、用 {@link ClientFrames#frameOf} 重建帧。</li>
     * </ol>
     *
     * <p><b>本方法只读不移除，也不推进任何游标</b>。这是「至少一次」的实现方式：
     * 返回的帧等网关真的写进客户端连接之后，由它调 {@link #ackPending} 推进。
     * 写失败 / 进程崩溃 = 游标不动，下次重连从原位置重读（客户端按 seq 去重）。
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
        String deviceId = OfflineMessageStore.normalizeDevice(request.getDeviceId());
        // 先登记客户端上报的消费水位，再补发离线帧：这个接口顺带承担「上报水位」的职责。
        // 双写：进程内那份给发号器当 Redis 丢失时的下界，Redis 那份是「已送达」的可查询口径。
        watermarkStore.observe(userId, deviceId, request.getWatermarksMap());
        int max = request.getMax() > 0 ? request.getMax() : DEFAULT_PULL_MAX;

        List<String> out = new ArrayList<>(max);
        OfflineMessageStore.FrameBatch batch = offlineMessageStore.peek(userId, deviceId, max);
        // 离线队列存的是「发送时刻的帧快照」，它不知道后来发生的撤回/删除，
        // 所以出口处必须按最新状态再判定一次（否则撤回过的消息会随补发重新出现原文）
        out.addAll(redactQueued(userId, batch.legacyFrames()));
        out.addAll(redactQueued(userId, batch.frames()));

        Map<String, Long> groupAcks = new LinkedHashMap<>();
        int pendingConversations = 0;
        if (out.size() < max) {
            pendingConversations = pullGroupPending(userId, deviceId, request.getWatermarksMap(),
                    max - out.size(), out, groupAcks);
        }
        // 补发量是「离线积压是否在消化」的直接证据：入队与补发两条曲线应该大致贴合
        metrics.offlinePulled(out.size());
        return PullPendingResponse.newBuilder()
                .addAllFrames(out)
                // 只数「该设备还没确认的」，不能数队列总条数：已确认的帧要等裁剪才消失，
                // 用总数会让客户端永远看到「还剩一堆」而无限重拉
                .setRemaining((int) (offlineMessageStore.pendingFrames(userId, deviceId) + pendingConversations))
                .setAckSingleSeq(batch.ackSeq())
                .putAllAckGroupSeqs(groupAcks)
                .build();
    }

    /**
     * 确认一批补发帧已经写进客户端连接，推进对应游标。
     *
     * <p>幂等且只前进：同一批重复确认、或旧连接的确认晚到，都只是空操作。
     * 确认失败不影响正确性（最多重复补发），所以这里把异常转成非 0 code 而不是抛出去——
     * 网关不该因为一次确认失败就把连接搞坏。
     */
    @Override
    public AckPendingResponse ackPending(AckPendingRequest request) {
        String userId = request.getUserId();
        if (!StringUtils.hasText(userId)) {
            return AckPendingResponse.newBuilder().setCode(CODE_BAD_REQUEST).setMessage("user_id is required").build();
        }
        String deviceId = OfflineMessageStore.normalizeDevice(request.getDeviceId());
        try {
            long cursor = offlineMessageStore.ackFrames(userId, deviceId, request.getAckSingleSeq());
            int advanced = 0;
            // 设备集合一次取好：一次 ack 里通常带多个会话，逐个 SMEMBERS 就是 N 倍放大
            Set<String> devices = request.getAckGroupSeqsCount() == 0
                    ? Set.of()
                    : offlineMessageStore.knownDevices(userId);
            for (Map.Entry<String, Long> entry : request.getAckGroupSeqsMap().entrySet()) {
                String conversationId = entry.getKey();
                long nextSeq = entry.getValue();
                if (offlineMessageStore.ackGroupPending(userId, deviceId, conversationId, nextSeq)) {
                    advanced++;
                }
                // 所有已登记设备都补过这个位置了，锚点才能退休（否则新设备上线会从更早的位置重拉）
                if (offlineMessageStore.allDevicesReached(devices, userId, conversationId, nextSeq)) {
                    offlineMessageStore.clearPending(userId, conversationId);
                }
            }
            log.debug("[offline] 补发确认 user={} device={} singleCursor={} groupAdvanced={}",
                    userId, deviceId, cursor, advanced);
            return AckPendingResponse.newBuilder()
                    .setCode(CODE_OK)
                    .setMessage("ok")
                    .setSingleCursor(cursor)
                    .build();
        } catch (RuntimeException e) {
            log.error("[offline] 补发确认失败（最多导致重复补发，不影响不丢）user={} device={} err={}",
                    userId, deviceId, e.getMessage());
            return AckPendingResponse.newBuilder()
                    .setCode(CODE_UNAVAILABLE)
                    .setMessage("ack failed: " + e.getMessage())
                    .build();
        }
    }

    /**
     * 客户端端到端确认：把该设备的 delivered 水位推进到这些 seq。
     *
     * <p>只做一件事——按会话取 max 后写 {@code im:wm:{userId}:{deviceId}}。
     * <b>不删离线队列、不推进补发游标</b>：那两件事由「网关写成功」（{@link #ackPending}）驱动。
     * 原因很实际：客户端 ACK 可能永远不来（进程被杀、用户直接关机），等它才清队列等于让队列无限堆积。
     *
     * <p>按会话取 max 是必要的：同一批 ACK 里同一会话往往有好几条（客户端攒批），
     * 逐条写 Redis 就是「消息数」次 HSET，压成「会话数」次之后少一个量级，
     * 而水位的语义本来就是「见到第几号」，取 max 不损失信息。
     */
    @Override
    public DeliveredAckResponse ackDelivered(DeliveredAckRequest request) {
        String userId = request.getUserId();
        if (!StringUtils.hasText(userId)) {
            return DeliveredAckResponse.newBuilder()
                    .setCode(CODE_BAD_REQUEST).setMessage("user_id is required").build();
        }
        Map<String, Long> highWater = new LinkedHashMap<>();
        for (DeliveredAck ack : request.getAcksList()) {
            String conversationId = ack.getConversationId();
            if (!StringUtils.hasText(conversationId) || ack.getSeq() <= 0) {
                continue;
            }
            highWater.merge(conversationId, ack.getSeq(), Math::max);
        }
        if (highWater.isEmpty()) {
            return DeliveredAckResponse.newBuilder().setCode(CODE_OK).setMessage("ok").setAccepted(0).build();
        }
        // delivered 水位是设备级的：手机收到了，不代表网页收到了
        String deviceId = OfflineMessageStore.normalizeDevice(request.getDeviceId());
        try {
            Map<String, Long> advanced = watermarkStore.observe(userId, deviceId, highWater);
            log.debug("[delivered] user={} device={} 推进 {}/{} 个会话的水位",
                    userId, deviceId, advanced.size(), highWater.size());
            // 只对「真的前进了」的会话回推：重复 ACK 不该让对端收到一串相同的回执
            notifyState(advanced, userId, ClientFrames.STATE_DELIVERED);
            return DeliveredAckResponse.newBuilder()
                    .setCode(CODE_OK).setMessage("ok").setAccepted(highWater.size()).build();
        } catch (RuntimeException e) {
            // ACK 丢失是可恢复的：客户端下次 sync 会重新上报，且水位只按 max 推进，重复无害
            log.warn("[delivered] 写 delivered 水位失败（下次 sync 会重报，不影响投递）user={} err={}",
                    userId, e.getMessage());
            return DeliveredAckResponse.newBuilder()
                    .setCode(CODE_UNAVAILABLE).setMessage("ack failed: " + e.getMessage()).build();
        }
    }

    /**
     * 对离线队列里的帧快照做「撤回脱敏 + 本地删除过滤」。
     *
     * <p>这里不是多余的一道保险，而是<b>唯一</b>能拦住"补发把已撤回内容重新发出去"的地方：
     * 队列里存的是发送时刻的完整帧，撤回发生在之后，帧本身不会自己变。
     *
     * <p>三个取舍写在这里，避免以后被当成可以优化的开销：
     * <ul>
     *   <li>逐帧解析 JSON（一次补发最多几百帧）：只有解析出 seq 才能判定撤回，
     *       而队列的物理结构（List/ZSet 里的字符串）没有别的地方存这个对应关系；</li>
     *   <li><b>查不到撤回状态时按未撤回下发</b>并记 error：反过来（当成已撤回）会在
     *       存储抖动时把正常消息永久变成「已撤回占位」，那是不可恢复的错误；
     *       而按原文下发的窗口只有一次补发，且同一次故障下系统整体已经不可用；</li>
     *   <li><b>被用户删除的帧直接跳过</b>：删除是用户明确表达"不要再给我看"，
     *       跳过不影响游标推进（游标由 {@code ackSeq} 决定），下次也不会重发。</li>
     * </ul>
     */
    private List<String> redactQueued(String userId, List<String> queued) {
        if (queued == null || queued.isEmpty()) {
            return List.of();
        }
        List<ClientFrames.FrameRef> refs = new ArrayList<>(queued.size());
        Map<String, Set<Long>> seqsByConversation = new LinkedHashMap<>();
        for (String raw : queued) {
            ClientFrames.FrameRef ref = frames.refOf(raw);
            refs.add(ref);
            if (ref != null) {
                seqsByConversation.computeIfAbsent(ref.conversationId(), _ -> new LinkedHashSet<>())
                        .add(ref.seq());
            }
        }
        if (seqsByConversation.isEmpty()) {
            return queued;
        }
        Map<String, Set<Long>> recalled = new LinkedHashMap<>();
        Map<String, Set<Long>> deleted = new LinkedHashMap<>();
        for (Map.Entry<String, Set<Long>> entry : seqsByConversation.entrySet()) {
            try {
                recalled.put(entry.getKey(), messageRepository.recalledSeqs(entry.getKey(), entry.getValue()));
                deleted.put(entry.getKey(), messageRepository.deletedSeqs(userId, entry.getKey()));
            } catch (RuntimeException e) {
                // 见方法注释：失败时按「未撤回、未删除」处理，宁可短暂不一致也不制造不可恢复的错误
                log.error("[offline] 查询撤回/删除状态失败，本批按原文下发 conv={} err={}",
                        entry.getKey(), e.getMessage());
                recalled.put(entry.getKey(), Set.of());
                deleted.put(entry.getKey(), Set.of());
            }
        }
        List<String> out = new ArrayList<>(queued.size());
        for (int i = 0; i < queued.size(); i++) {
            ClientFrames.FrameRef ref = refs.get(i);
            if (ref == null) {
                out.add(queued.get(i));
                continue;
            }
            if (deleted.getOrDefault(ref.conversationId(), Set.of()).contains(ref.seq())) {
                continue;
            }
            out.add(recalled.getOrDefault(ref.conversationId(), Set.of()).contains(ref.seq())
                    ? frames.recalledFrame(queued.get(i))
                    : queued.get(i));
        }
        return out;
    }

    /**
     * 群聊补发：按会话指针从历史里取 {@code seq > 起点} 的消息并重建帧。
     *
     * <p>三条硬约定：
     * <ul>
     *   <li><b>历史查询失败绝不推进指针</b>：宁可下次多补一遍（客户端按 seq 去重），
     *       也不能把指针推过去，那等于永久丢一段消息；</li>
     *   <li><b>这里不推进指针</b>：只把「本批补到哪」登记进 {@code acks}，由网关写成功后再确认。
     *       以前在读路径直接推进，网关写失败那段消息就永久跳过了；</li>
     *   <li><b>确实查不到数据才清指针</b>：那是唯一可以安全删除的时机——指针之后没有消息了。</li>
     * </ul>
     *
     * @param deviceId 发起补发的设备；起点要按<b>这台设备</b>的进度算，不是用户的
     * @param budget   本次还能补多少帧（全局上限，避免一个用户把整轮 pull 撑爆）
     * @param acks     输出参数：本次待确认的「会话 → 下一条待补的 seq」
     * @return 本次之后仍有待补消息的会话数
     */
    private int pullGroupPending(String userId, String deviceId, Map<String, Long> watermarks, int budget,
                                 List<String> out, Map<String, Long> acks) {
        Map<String, Long> pending = offlineMessageStore.pendingConversations(userId, MAX_PULL_CONVERSATIONS);
        if (pending.isEmpty()) {
            return 0;
        }
        // 超出单轮处理上限的会话数也要计进 remaining，否则客户端会以为已经拉干净了
        int remaining = (int) Math.max(0, offlineMessageStore.pendingConversationCount(userId) - pending.size());
        // 已落库的 delivered 水位（客户端之前 ACK 累积下来的）。只查这批会话，一次 pipeline。
        Map<String, Long> delivered = watermarkStore.deliveredSeqs(userId, deviceId, pending.keySet());
        for (Map.Entry<String, Long> entry : pending.entrySet()) {
            if (budget <= 0) {
                remaining++;
                continue;
            }
            String conversationId = entry.getKey();
            // anchor = 用户级「最早的未补发位置」，只用来兜底新设备（从 seq=1 拉整个历史是灾难）
            long anchor = entry.getValue();
            // 真正的起点按设备算：手机补过的部分，网页重连时仍然要能补到
            long deviceCursor = offlineMessageStore.groupCursor(userId, deviceId, conversationId);
            // 客户端确认过的位置有两个来源，取大的：
            //  * 本次 sync 帧上报的 watermarks（显式对账）
            //  * 之前实时 ACK 累积在 im:wm 里的水位（增量确认）
            // 少了后者，端到端 ACK 就只是个指标，不会真的让「已确认的消息」不再补发一遍。
            long confirmedByClient = Math.max(
                    watermarks.getOrDefault(conversationId, 0L),
                    delivered.getOrDefault(conversationId, 0L));
            long start = Math.max(Math.max(anchor, deviceCursor + 1), confirmedByClient + 1);
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
            if (messages.isEmpty()) {
                // 只有当起点就是锚点（没有被某台设备的游标或客户端水位抬高）时，才能断言
                // 「锚点之后对所有设备都没有数据」。否则这里查空只证明<b>这台设备</b>没东西了，
                // 清掉锚点会把另一台设备还没读的消息永久标记成已补发。
                if (start <= anchor) {
                    offlineMessageStore.clearPending(userId, conversationId);
                }
                continue;
            }
            // 撤回脱敏 + 本地删除过滤：补发不能把用户删掉的、或已撤回的内容再发一遍
            Set<Long> groupRecalled;
            Set<Long> groupDeleted;
            try {
                Set<Long> seqs = new LinkedHashSet<>(messages.size());
                for (StoredMessage message : messages) {
                    seqs.add(message.seq());
                }
                groupRecalled = messageRepository.recalledSeqs(conversationId, seqs);
                groupDeleted = messageRepository.deletedSeqs(userId, conversationId);
            } catch (RuntimeException e) {
                // 与单聊补发同一取舍：查询失败按「未撤回、未删除」处理，不制造不可恢复的错误
                log.error("[offline] 查询撤回/删除状态失败，本批按原文补发 conv={} err={}",
                        conversationId, e.getMessage());
                groupRecalled = Set.of();
                groupDeleted = Set.of();
            }
            int sent = 0;
            for (StoredMessage message : messages) {
                if (groupDeleted.contains(message.seq())) {
                    continue;
                }
                out.add(frames.frameOf(message, groupRecalled.contains(message.seq())));
                sent++;
            }
            // 预算按实际下发的条数扣，否则跳过删除项会让某个会话把整轮预算吃满却什么也没发
            budget -= sent;
            // 只登记待确认位置，不推进：网关写成功后会带着它回调 ackPending
            acks.put(conversationId, messages.getLast().seq() + 1);
            if (messages.size() >= limit) {
                // 本次预算被这个会话吃满，很可能还有更多
                remaining++;
            }
        }
        return remaining;
    }

    /**
     * 把「该用户在某个会话上确认到 seq」的状态回推给对端（发送方）。
     *
     * <p>两处调用：{@code ackDelivered}（DELIVERED）、{@code markRead}（READ）。
     * 只推<b>真的前进了</b>的会话——重复上报是常态（sync 与增量 ACK 重叠、多端各报一次），
     * 不去重的话对端会收到一串 seq 相同的回执。
     *
     * @param confirmed conversationId → 该用户确认到的 seq
     */
    private void notifyState(Map<String, Long> confirmed, String userId, String state) {
        if (stateNotifier == null || confirmed == null || confirmed.isEmpty()) {
            return;
        }
        for (Map.Entry<String, Long> entry : confirmed.entrySet()) {
            stateNotifier.notifyState(entry.getKey(), convTypeOf(entry.getKey()), userId, state, entry.getValue());
        }
    }

    /** 会话 ID 前缀即类型：{@code g:xxx} 是群聊，其余按单聊处理 */
    private static String convTypeOf(String conversationId) {
        return conversationId != null && conversationId.startsWith("g:")
                ? MessageEnvelope.CONV_GROUP : MessageEnvelope.CONV_SINGLE;
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

    /**
     * 消息变更（撤回 / 本地删除）的结果。
     *
     * <p>不复用 {@code SubmitMessageResponse}：那是「受理」的响应（带 seq/serverMsgId/幂等标记），
     * 而变更动作只关心「成没成、为什么没成」。
     */
    public record MessageChangeResult(int code, String message, long seq) {

        static MessageChangeResult ok(long seq) {
            return new MessageChangeResult(CODE_OK, "ok", seq);
        }

        static MessageChangeResult reject(int code, String message) {
            return new MessageChangeResult(code, message, 0);
        }

        public boolean isOk() {
            return code == CODE_OK;
        }
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
