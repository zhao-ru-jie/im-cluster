package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.ConversationSummary;
import com.github.im.proto.ingress.v1.ListConversationsRequest;
import com.github.im.proto.ingress.v1.ListConversationsResponse;
import com.github.im.proto.ingress.v1.MarkReadRequest;
import com.github.im.proto.ingress.v1.MarkReadResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.metrics.MessageMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 「发消息 → 会话列表 → 未读数 → 已读」的端到端约定。
 *
 * <p>这条链路是这次新增的会话模型与既有消息流水线的接缝，锁住三件事：
 * <ol>
 *   <li><b>消息一落库，双方（或全体群成员）的会话列表里就该有它</b>，且未读数为 1；</li>
 *   <li><b>未读数是游标相减</b>，不是存的计数——所以已读上报之后必须精确变成 0，
 *       不需要任何额外的「扣减」写入；</li>
 *   <li><b>已读是用户级的</b>：一端标了已读，另一端拉列表看到的也是 0。</li>
 * </ol>
 *
 * <p>需要本机 Redis，不可用时自动跳过。
 */
class ConversationFlowTest {

    private static final Duration TTL = Duration.ofHours(1);

    private final MessageRepository repository = mock(MessageRepository.class);
    private final DeliveryDispatcher delivery = mock(DeliveryDispatcher.class);
    private final SeqAllocator seqAllocator = mock(SeqAllocator.class);
    private final ClientWatermarkStore watermarkStore = mock(ClientWatermarkStore.class);
    private final GroupMemberRepository groupMembers = mock(GroupMemberRepository.class);
    private final AtomicLong seq = new AtomicLong(0);
    private final List<String> conversationIds = new ArrayList<>();
    private final List<String> userIds = new ArrayList<>();

    /**
     * stub 只注册一次。
     *
     * <p>放在 {@code service()} 里每次重建 MessageService 时重新 {@code when(...)} 会多消耗 seq：
     * {@code when(mock.foo(any()))} 本身是一次真实的 mock 调用，第二次注册时就会命中上一次的
     * {@code thenAnswer}，白白推进了计数器——表现为「两条消息的 seq 变成 1 和 3」。
     */
    @org.junit.jupiter.api.BeforeEach
    void stubSeq() {
        when(seqAllocator.allocate(anyString(), anyString()))
                .thenAnswer(inv -> new SeqAllocator.Allocation(false, null, seq.incrementAndGet()));
    }

    @AfterEach
    void cleanup() {
        StringRedisTemplate redis = TestRedis.template();
        for (String conversationId : conversationIds) {
            redis.delete(RedisKeys.convMeta(conversationId));
        }
        for (String userId : userIds) {
            redis.delete(ConversationStore.convsKeyOf(userId));
            redis.delete(ConversationStore.readKeyOf(userId));
            redis.delete(ConversationStore.flagsKeyOf(userId));
            redis.delete(RedisKeys.userOfflinePtr(userId));
        }
    }

    @Test
    @DisplayName("单聊：消息落库后出现在双方的会话列表里，各 1 条未读")
    void singleChatAppearsInBothListsAsUnread() {
        String alice = newUser();
        String bob = newUser();
        String conversation = newConversation();

        assertEquals(0, submit(conversation, "SINGLE", alice, bob, "你好").getCode());

        List<ConversationSummary> forAlice = list(alice);
        assertEquals(1, forAlice.size());
        assertEquals(conversation, forAlice.get(0).getConversationId());
        assertEquals("SINGLE", forAlice.get(0).getType());
        assertEquals(1, forAlice.get(0).getLastSeq());
        assertEquals(alice, forAlice.get(0).getLastFrom());
        assertEquals("你好", forAlice.get(0).getLastDigest());
        assertEquals(1, forAlice.get(0).getUnread());

        List<ConversationSummary> forBob = list(bob);
        assertEquals(1, forBob.size(), "接收方也必须看到这个会话，否则他永远不知道有人找他");
        assertEquals(1, forBob.get(0).getUnread());
    }

    @Test
    @DisplayName("已读上报后未读数归零，且不需要任何扣减写入")
    void markReadClearsUnread() {
        String alice = newUser();
        String bob = newUser();
        String conversation = newConversation();
        submit(conversation, "SINGLE", alice, bob, "hi");
        submit(conversation, "SINGLE", bob, alice, "hi back");

        assertEquals(2, list(alice).get(0).getUnread());

        MarkReadResponse response = service().markRead(MarkReadRequest.newBuilder()
                .setUserId(alice).setConversationId(conversation).setSeq(2).build());

        assertEquals(0, response.getCode());
        assertEquals(2, response.getReadSeq());
        assertEquals(0, response.getUnread());
        assertEquals(0, list(alice).get(0).getUnread(), "未读是算出来的，已读一推进就自动归零");
        assertEquals(2, list(bob).get(0).getUnread(), "另一方的未读不受影响");
    }

    @Test
    @DisplayName("已读是用户级的：一端读完，另一端拉列表也是 0")
    void markReadIsSharedAcrossDevices() {
        String alice = newUser();
        String bob = newUser();
        String conversation = newConversation();
        submit(conversation, "SINGLE", bob, alice, "hi");

        service().markRead(MarkReadRequest.newBuilder()
                .setUserId(alice).setConversationId(conversation).setSeq(1).build());

        // 列表接口本来就没有 deviceId 维度：手机上读完，网页看到的也是已读
        assertEquals(0, list(alice).get(0).getUnread());
        assertEquals(1, list(bob).get(0).getUnread(), "同一会话里每个人的已读是独立的");
    }

    @Test
    @DisplayName("群聊：一条消息写进全体成员的会话列表")
    void groupFanoutToAllMembers() {
        String alice = newUser();
        String bob = newUser();
        String carol = newUser();
        String groupId = "g-" + UUID.randomUUID();
        String conversation = newConversation();
        when(groupMembers.membersOf(groupId)).thenReturn(List.of(alice, bob, carol));

        submit(conversation, "GROUP", alice, groupId, "开会了");

        for (String member : List.of(alice, bob, carol)) {
            List<ConversationSummary> conversations = list(member);
            assertEquals(1, conversations.size(), member + " 的会话列表里应该有这个群");
            assertEquals("GROUP", conversations.get(0).getType());
            assertEquals(1, conversations.get(0).getUnread());
        }
    }

    @Test
    @DisplayName("最后一条消息只留摘要，不把整个 payload 拖进会话列表")
    void digestIsTruncated() {
        String alice = newUser();
        String bob = newUser();
        String conversation = newConversation();

        submit(conversation, "SINGLE", alice, bob, "x".repeat(500));

        String digest = list(alice).get(0).getLastDigest();
        assertEquals(64, digest.length(), "会话列表每页几十条，摘要不截断会把响应体撑爆");
        assertTrue(digest.chars().allMatch(c -> c == 'x'));
    }

    @Test
    @DisplayName("会话列表按最后消息时间倒序，最新说话的排最前")
    void listOrderedByRecency() {
        String alice = newUser();
        String bob = newUser();
        String older = newConversation();
        String newer = newConversation();

        submit(older, "SINGLE", bob, alice, "first");
        submit(newer, "SINGLE", bob, alice, "second");

        List<ConversationSummary> conversations = list(alice);
        assertEquals(2, conversations.size());
        assertEquals(newer, conversations.get(0).getConversationId());
        assertEquals(older, conversations.get(1).getConversationId());
    }

    @Test
    @DisplayName("置顶会话排在最前，且不影响未读数")
    void pinnedConversationComesFirst() {
        String alice = newUser();
        String bob = newUser();
        String pinned = newConversation();
        String newer = newConversation();

        submit(pinned, "SINGLE", bob, alice, "旧消息");
        submit(newer, "SINGLE", bob, alice, "新消息");
        service().setConversationFlags(alice, pinned, true, null);

        List<ConversationSummary> conversations = list(alice);
        assertEquals(pinned, conversations.get(0).getConversationId(), "置顶必须压过「更近」的时间序");
        assertTrue(conversations.get(0).getPinned());
        assertEquals(1, conversations.get(0).getUnread(), "置顶只影响排序，未读数照旧");
    }

    @Test
    @DisplayName("置顶与免打扰是两个独立开关：改一个不覆盖另一个")
    void flagsAreIndependent() {
        String alice = newUser();
        String bob = newUser();
        String conversation = newConversation();

        submit(conversation, "SINGLE", bob, alice, "hi");
        service().setConversationFlags(alice, conversation, true, true);
        // 只改免打扰：置顶必须留着，否则用户会莫名其妙丢掉自己的置顶
        service().setConversationFlags(alice, conversation, null, false);

        ConversationSummary summary = list(alice).get(0);
        assertTrue(summary.getPinned(), "只改免打扰不该把置顶清掉");
        assertFalse(summary.getMuted());
        assertEquals(1, summary.getUnread(), "免打扰不影响未读：只是客户端不响铃");
    }

    @Test
    @DisplayName("图片消息的会话摘要显示 [图片]，不会留下空摘要")
    void mediaMessageDigestIsPlaceholder() {
        String alice = newUser();
        String bob = newUser();
        String conversation = newConversation();

        service().submit(SubmitMessageRequest.newBuilder()
                .setClientMsgId("c-" + UUID.randomUUID())
                .setConversationId(conversation)
                .setConvType("SINGLE")
                .setFromUserId(bob)
                .setReceiverId(alice)
                .setPayload("")
                .setContentType("IMAGE")
                .setAttrs("{\"url\":\"https://cdn/a.png\"}")
                .setClientTs(System.currentTimeMillis())
                .build());

        assertEquals("[图片]", list(alice).get(0).getLastDigest(),
                "媒体消息的 body 往往是空的，列表里显示占位比显示空串有用");
    }

    // ==== 辅助 ====

    private SubmitMessageResponse submit(String conversation, String type, String from, String to, String body) {
        // 时间戳必须递增，否则同一毫秒内的两条消息 ZSet score 相同，排序断言会不稳定
        return service().submit(SubmitMessageRequest.newBuilder()
                .setClientMsgId("c-" + UUID.randomUUID())
                .setConversationId(conversation)
                .setConvType(type)
                .setFromUserId(from)
                .setReceiverId(to)
                .setPayload(body)
                .setClientTs(System.currentTimeMillis())
                .build());
    }

    private List<ConversationSummary> list(String userId) {
        ListConversationsResponse response = service().listConversations(
                ListConversationsRequest.newBuilder().setUserId(userId).setLimit(20).build());
        return response.getConversationsList();
    }

    private MessageService service() {
        return new MessageService(TestRedis.template(), repository, store(),
                new ClientFrames(), delivery, seqAllocator, watermarkStore,
                new ConversationStore(TestRedis.template(), TTL, 1000), groupMembers, null, null,
                null, null, Duration.ofMinutes(2), new MessageMetrics(null), null);
    }

    private OfflineMessageStore store() {
        return new OfflineMessageStore(TestRedis.template(), TTL, 500, TTL);
    }

    private String newConversation() {
        String conversationId = TestRedis.conversationId();
        conversationIds.add(conversationId);
        return conversationId;
    }

    private String newUser() {
        String userId = "conv-flow-" + UUID.randomUUID();
        userIds.add(userId);
        return userId;
    }
}
