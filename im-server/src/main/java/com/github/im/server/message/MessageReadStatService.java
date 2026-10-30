package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 群聊已读聚合：「这条消息，谁读了、还有几个人没读」。
 *
 * <h2>它补的是哪一环</h2>
 *
 * <p>已读位置一直存在（{@code im:user:read:{userId}} 里 {@code conversationId -> seq}），
 * 但那是<b>每个人自己的一行</b>：单聊回执能把「对端读到哪」直接推给对方
 * （{@link MessageStateNotifier}），群聊却没法这么做——群里的「已读」是一个集合
 * （成员数 × 消息数），逐人逐条推等于把扇出放大到不可控。所以群聊此前<b>完全没有出口</b>：
 * 数据在，但客户端拿不到。
 *
 * <p>这里给出的是<b>按需聚合</b>：给定一个会话与一个 seq，把「读到这个位置的人」与
 * 「还没读到的人」算出来。写入侧一行都不用改，因此没有新增的写放大，也没有新 key。
 *
 * <h2>三个刻意的选择</h2>
 *
 * <ul>
 *   <li><b>只读、不落库</b>：聚合结果不进 Redis/MySQL。它随时可由「成员列表 + 每人的
 *       readSeq」重算，存一份就等于多一份会不一致的真相；</li>
 *   <li><b>短 TTL 快照缓存</b>：一次聚合要读全量成员的已读位置（5000 人群 = 5000 个 HGET，
 *       走 pipeline 分批）。客户端滑一屏消息会连续问十几次，缓存 {@code im.read-stat.cache-ttl}
 *       （默认 2 秒）把这类重复压掉。缓存的是「成员 + 各自 readSeq」，与 seq 无关，
 *       所以同一屏的不同消息共用一份；</li>
 *   <li><b>调用者自己那一条永远取实时值</b>：缓存最多滞后 2 秒，但「我刚读完却看到自己
 *       是未读」是最刺眼的错，为此多付一次 HGET 值得。</li>
 * </ul>
 *
 * <h2>计数口径</h2>
 *
 * <p>{@code total} 不含<b>该消息的发送者</b>：他当然读过自己发的消息，算进分母只会让
 * 「已读 3/3」永远差一个。发送者靠 seq 反查消息得到；消息已被裁剪（Redis 形态只保留最近
 * 1000 条）时查不到，此时不做排除——宁可分母大一号，也不要因为历史缺失把整条消息的已读状态变空。
 *
 * <h2>权限</h2>
 *
 * <p>已读名单本身就是敏感信息（谁在线、谁在潜水），所以<b>必须是会话参与者</b>才查得到：
 * 群聊要求是群成员，单聊要求 userId 属于 {@code s:a:b}。这一层不依赖网关鉴权——
 * 见 README 里「REST 无鉴权」的已知限制，真正上线前仍需在接入层补齐身份校验。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Service
public class MessageReadStatService {

    private static final Logger log = LoggerFactory.getLogger(MessageReadStatService.class);

    private static final int CODE_OK = 0;
    private static final int CODE_BAD_REQUEST = 400;
    private static final int CODE_FORBIDDEN = 403;
    /** 可重试：存储抖动时不要给客户端一个「零人已读」的假结论 */
    private static final int CODE_UNAVAILABLE = 503;

    /** 明细默认条数：够渲染一个「已读/未读」面板，又不会把万人群的名单一次性拖出来 */
    private static final int DEFAULT_LIMIT = 100;
    /** 明细上限。注意计数（total/readCount）始终是全量，这里只封顶名单 */
    private static final int MAX_LIMIT = 500;

    /**
     * 快照缓存上限：超了整体清空。
     *
     * <p>跟 {@link MessageStateNotifier#tracked()} 背后同一个取舍——一张按会话增长的 map
     * 必须有界，而精确 LRU 的复杂度不值得，宁可偶尔多查一次 Redis。
     */
    private static final int MAX_CACHED = 512;

    /** 已读的排前面，各自按 readSeq 倒序（刚读完的靠前），最后按 userId 稳定排序 */
    private static final Comparator<ReadStat.Member> READER_FIRST =
            Comparator.comparing(ReadStat.Member::read).reversed()
                    .thenComparing(Comparator.comparingLong(ReadStat.Member::readSeq).reversed())
                    .thenComparing(ReadStat.Member::userId);

    private final ConversationStore conversations;
    private final GroupMemberRepository groupMembers;
    private final MessageRepository messageRepository;
    /** {@code <= 0} 表示关闭快照缓存（单测与排障用） */
    private final Duration cacheTtl;

    private final Map<String, Snapshot> cache = new ConcurrentHashMap<>();

    public MessageReadStatService(ConversationStore conversations,
                                  GroupMemberRepository groupMembers,
                                  MessageRepository messageRepository,
                                  @Value("${im.read-stat.cache-ttl:2s}") Duration cacheTtl) {
        this.conversations = conversations;
        this.groupMembers = groupMembers;
        this.messageRepository = messageRepository;
        this.cacheTtl = cacheTtl == null ? Duration.ZERO : cacheTtl;
        log.info("[read-stat] 群已读聚合快照缓存 {}",
                this.cacheTtl.isZero() || this.cacheTtl.isNegative() ? "已关闭" : this.cacheTtl.toString());
    }

    /**
     * 聚合某个会话在 {@code seq} 这个位置上的已读情况。
     *
     * @param conversationId {@code g:{groupId}} 或 {@code s:a:b}
     * @param viewerId       调用者（必须属于该会话）
     * @param seq            统计位置；{@code <= 0} 表示「会话当前最新一条」
     * @param limit          明细条数；{@code <= 0} 用默认值
     */
    public ReadStatResult stat(String conversationId, String viewerId, long seq, int limit) {
        if (!StringUtils.hasText(conversationId) || !StringUtils.hasText(viewerId)) {
            return ReadStatResult.reject(CODE_BAD_REQUEST, "conversation_id and user_id are required");
        }
        boolean group = isGroup(conversationId);
        if (!authorized(conversationId, viewerId, group)) {
            return ReadStatResult.reject(CODE_FORBIDDEN,
                    viewerId + " is not a participant of " + conversationId);
        }
        try {
            long target = seq > 0 ? seq : conversations.meta(conversationId).lastSeq();
            if (target <= 0) {
                return ReadStatResult.reject(CODE_BAD_REQUEST,
                        "seq is required: " + conversationId + " has no message yet");
            }
            Snapshot snapshot = snapshotOf(conversationId, group, viewerId);
            String sender = senderOf(conversationId, target);
            // 调用者自己的已读位置取实时值，其余人走快照
            long viewerReadSeq = conversations.readSeq(viewerId, conversationId);

            List<ReadStat.Member> details = new ArrayList<>(snapshot.members().size());
            int read = 0;
            for (String member : snapshot.members()) {
                if (!member.equals(sender)) {
                    long readSeq = member.equals(viewerId)
                            ? viewerReadSeq
                            : snapshot.readSeqs().getOrDefault(member, 0L);
                    boolean done = readSeq >= target;
                    if (done) {
                        read++;
                    }
                    details.add(new ReadStat.Member(member, readSeq, done));
                }
            }
            int total = details.size();
            details.sort(READER_FIRST);

            int safeLimit = limit <= 0 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
            boolean truncated = details.size() > safeLimit;
            List<ReadStat.Member> page = truncated
                    ? new ArrayList<>(details.subList(0, safeLimit))
                    : details;

            ReadStat stat = new ReadStat(conversationId,
                    group ? MessageEnvelope.CONV_GROUP : MessageEnvelope.CONV_SINGLE,
                    target, viewerReadSeq, total, read, Math.max(0, total - read), page, truncated);
            return ReadStatResult.ok(stat);
        } catch (RuntimeException e) {
            log.error("[read-stat] 聚合失败 conv={} seq={} viewer={} err={}",
                    conversationId, seq, viewerId, e.getMessage());
            return ReadStatResult.reject(CODE_UNAVAILABLE, "read-state unavailable: " + e.getMessage());
        }
    }

    // ==== 内部 ====

    /**
     * 群聊必须是群成员；单聊必须是 {@code s:a:b} 里的那一方。
     *
     * <p>单聊用 {@link MessageEnvelope#peerOf} 判定而不是自己拆字符串：会话 ID 的格式只有
     * 契约层一个出处，多写一份就会在某次协议微调后只有一半的地方生效。
     */
    private boolean authorized(String conversationId, String viewerId, boolean group) {
        if (!group) {
            return MessageEnvelope.peerOf(conversationId, viewerId) != null;
        }
        String groupId = groupIdOf(conversationId);
        return groupId != null && groupMembers.isMember(groupId, viewerId);
    }

    /** 快照：成员名单 + 每人的已读位置，与会话 ID 绑定、与 seq 无关 */
    private Snapshot snapshotOf(String conversationId, boolean group, String viewerId) {
        long now = System.currentTimeMillis();
        boolean caching = !cacheTtl.isZero() && !cacheTtl.isNegative();
        if (caching) {
            Snapshot cached = cache.get(conversationId);
            if (cached != null && cached.expiresAt() > now) {
                return cached;
            }
        }
        List<String> members = membersOf(conversationId, group, viewerId);
        Map<String, Long> readSeqs = conversations.readSeqsOf(conversationId, members);
        Snapshot snapshot = new Snapshot(List.copyOf(members), Map.copyOf(readSeqs),
                now + cacheTtl.toMillis());
        if (caching) {
            if (cache.size() > MAX_CACHED) {
                cache.clear();
            }
            cache.put(conversationId, snapshot);
        }
        return snapshot;
    }

    private List<String> membersOf(String conversationId, boolean group, String viewerId) {
        if (group) {
            String groupId = groupIdOf(conversationId);
            return groupId == null ? List.of() : groupMembers.membersOf(groupId);
        }
        List<String> members = new ArrayList<>(2);
        members.add(viewerId);
        String peer = MessageEnvelope.peerOf(conversationId, viewerId);
        if (peer != null && !peer.equals(viewerId)) {
            members.add(peer);
        }
        return members;
    }

    /**
     * 该 seq 的发送者（用于从分母里剔除）。
     *
     * <p>查不到就算了：Redis 形态的消息列表只保留最近 1000 条，老消息已无可查；
     * 为此把请求判失败，等于「越久远的消息越查不了已读」，方向反了。
     */
    private String senderOf(String conversationId, long seq) {
        try {
            return messageRepository.find(conversationId, seq).map(StoredMessage::from).orElse(null);
        } catch (RuntimeException e) {
            log.debug("[read-stat] 取发送者失败，按「不排除发送者」统计 conv={} seq={} err={}",
                    conversationId, seq, e.getMessage());
            return null;
        }
    }

    private static boolean isGroup(String conversationId) {
        return conversationId.startsWith("g:") && conversationId.length() > 2;
    }

    /** 群号：会话 ID 去掉 {@code g:} 前缀 */
    private static String groupIdOf(String conversationId) {
        if (!conversationId.startsWith("g:")) {
            return null;
        }
        String groupId = conversationId.substring(2);
        return groupId.isBlank() ? null : groupId;
    }

    // ==== 对外结构 ====

    /**
     * 一次聚合的结果。
     *
     * @param seq          实际统计的位置（{@code seq <= 0} 时被解析成会话当前 lastSeq）
     * @param viewerReadSeq 调用者自己在该会话上的已读位置
     * @param total        参与统计的成员数（已排除该消息的发送者）
     * @param members      明细，已读在前、各自按 readSeq 倒序，最多 {@code limit} 条
     * @param truncated    明细是否被截断（{@code total} 仍然是全量）
     */
    public record ReadStat(String conversationId,
                           String convType,
                           long seq,
                           long viewerReadSeq,
                           int total,
                           int readCount,
                           int unreadCount,
                           List<Member> members,
                           boolean truncated) {

        /** 单个成员的已读情况 */
        public record Member(String userId, long readSeq, boolean read) {
        }
    }

    public record ReadStatResult(int code, String message, ReadStat stat) {

        static ReadStatResult ok(ReadStat stat) {
            return new ReadStatResult(CODE_OK, "ok", stat);
        }

        static ReadStatResult reject(int code, String message) {
            return new ReadStatResult(code, message, null);
        }
    }

    private record Snapshot(List<String> members, Map<String, Long> readSeqs, long expiresAt) {
    }
}
