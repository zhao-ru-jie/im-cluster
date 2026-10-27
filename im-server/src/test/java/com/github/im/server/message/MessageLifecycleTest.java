package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.PushPort;
import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 消息生命周期（撤回 / 本地删除）的端到端约定。
 *
 * <p>锁住四条最容易做错的语义：
 * <ol>
 *   <li><b>撤回后所有读路径都不能带出原文</b>——包括历史、以及「离线队列里那份发送时刻的快照」。
 *       只广播事件而不脱敏读路径，断线重连就会把原文拉回来；</li>
 *   <li><b>撤回是会话级，删除是用户级</b>：撤回后双方都看不到原文；删除后只有自己看不到；</li>
 *   <li><b>只有发送方能撤回，且有时限</b>；重复撤回幂等（成功但不再广播）；</li>
 *   <li><b>变更事件的接收方不同</b>：撤回广播给会话内所有人，删除只推给操作者自己的其它端。</li>
 * </ol>
 *
 * <p>需要本机 Redis，不可用时自动跳过。
 */
class MessageLifecycleTest {

    private static final Duration TTL = Duration.ofHours(1);
    private static final int MAX_RECENT = 1000;

    private final ObjectMapper mapper = new ObjectMapper();
    private final DeliveryDispatcher delivery = mock(DeliveryDispatcher.class);
    private final SeqAllocator seqAllocator = mock(SeqAllocator.class);
    private final GroupMemberRepository groupMembers = mock(GroupMemberRepository.class);
    /** 捕获下行推送（变更事件是异步的，用队列等而不是 sleep） */
    private final BlockingQueue<PushTask> pushed = new LinkedBlockingQueue<>();
    private final AtomicLong seq = new AtomicLong(0);
    private final List<String> userIds = new ArrayList<>();
    private final List<String> conversationIds = new ArrayList<>();
    private MessageUpdateNotifier notifier;
    private OfflineMessageStore offline;

    @BeforeEach
    void stubSeq() {
        when(seqAllocator.allocate(anyString(), anyString()))
                .thenAnswer(inv -> new SeqAllocator.Allocation(false, null, seq.incrementAndGet()));
    }

    @AfterEach
    void cleanup() {
        if (notifier != null) {
            notifier.shutdown();
        }
        StringRedisTemplate redis = TestRedis.template();
        for (String conversationId : conversationIds) {
            redis.delete(RedisKeys.convMessages(conversationId));
            redis.delete(RedisKeys.convMeta(conversationId));
            redis.delete(RedisKeys.convRecalled(conversationId));
            for (String userId : userIds) {
                redis.delete(RedisKeys.userDeleted(userId, conversationId));
            }
        }
        for (String userId : userIds) {
            redis.delete(ConversationStore.convsKeyOf(userId));
            redis.delete(ConversationStore.readKeyOf(userId));
            redis.delete(RedisKeys.userOfflinePtr(userId));
        }
    }

    @Test
    @DisplayName("撤回后历史里只剩占位：body 为空、recalled=true、不带 attrs")
    void recallHidesOriginalContent() {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "这段话不该被第二个人看到");

        assertEquals(0, service().recall(conversation, messageSeq, alice).code());

        List<Map<String, Object>> history = service().history(bob, conversation, 50);
        assertEquals(1, history.size(), "撤回不是删除：seq 仍然占位，否则客户端会当成空洞去反复拉齐");
        Map<String, Object> frame = history.get(0);
        assertEquals("", frame.get("body"));
        assertEquals(Boolean.TRUE, frame.get("recalled"));
        assertNull(frame.get("attrs"), "attrs 里可能是图片 URL，撤回后仍然可访问就等于没撤回");
    }

    @Test
    @DisplayName("只有发送方能撤回；超出时限也不能撤回")
    void onlySenderWithinWindowCanRecall() {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "hi");

        assertEquals(403, service().recall(conversation, messageSeq, bob).code(),
                "接收方不能撤回别人的消息");
        // 时限设成 0：任何一条已落库的消息都已经超过它
        assertEquals(403, service(Duration.ZERO).recall(conversation, messageSeq, alice).code(),
                "没有时限的撤回等于允许事后修改共同历史，那是编辑而不是撤回");
    }

    @Test
    @DisplayName("重复撤回幂等：返回成功但不再广播第二次")
    void duplicateRecallDoesNotBroadcastAgain() throws Exception {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "hi");

        assertEquals(0, service().recall(conversation, messageSeq, alice).code());
        assertNotNull(pushed.poll(2, TimeUnit.SECONDS), "首次撤回要广播变更事件");
        assertEquals(0, service().recall(conversation, messageSeq, alice).code(), "重复撤回仍返回成功");

        assertNull(pushed.poll(300, TimeUnit.MILLISECONDS), "重复撤回不该再推一条相同的变更帧");
    }

    @Test
    @DisplayName("撤回广播给会话内所有人（含发送方自己的其他端）")
    void recallBroadcastsToAllMembers() throws Exception {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "hi");

        service().recall(conversation, messageSeq, alice);

        PushTask task = pushed.poll(2, TimeUnit.SECONDS);
        assertNotNull(task);
        assertEquals(Set.of(alice, bob), Set.copyOf(task.getTargetUserIdsList()),
                "发送方的其他端也还显示着原文，必须一起通知");
        assertTrue(task.getFrame().contains("\"change\":\"RECALLED\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"seq\":" + messageSeq), task.getFrame());
    }

    @Test
    @DisplayName("本地删除只影响自己：对方的视图不受影响")
    void localDeleteHidesOnlyForOperator() {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "oops");
        pushed.clear();

        assertEquals(0, service().deleteForUser(conversation, messageSeq, alice).code());

        assertEquals(0, service().history(alice, conversation, 50).size(),
                "删了就不该在重装 App 后又冒出来");
        assertEquals(1, service().history(bob, conversation, 50).size(),
                "删除是用户级动作，不该让对方也失去这条消息");
    }

    @Test
    @DisplayName("删除事件只推给操作者自己（其它端同步这个删除）")
    void deleteEventGoesToOperatorOnly() throws Exception {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "oops");
        pushed.clear();

        service().deleteForUser(conversation, messageSeq, alice);

        PushTask task = pushed.poll(2, TimeUnit.SECONDS);
        assertNotNull(task);
        assertEquals(List.of(alice), task.getTargetUserIdsList(),
                "推给对端就等于告诉对方「我删了这条」，那不是删除的语义");
        assertTrue(task.getFrame().contains("\"change\":\"DELETED\""), task.getFrame());
    }

    @Test
    @DisplayName("补发已撤回的消息时不能带出原文（离线队列里那份快照也要脱敏）")
    void offlineRepushRedactsRecalled() {
        String alice = newUser();
        String bob = newUser();
        String conversation = single(alice, bob);
        long messageSeq = submit(alice, bob, "secret-content");

        // 模拟「当时推给 bob 的那一帧」：离线队列存的是发送时刻的快照，它不会自己变
        StoredMessage stored = messages().find(conversation, messageSeq).orElseThrow();
        offline().storeAll(List.of(bob), frames().frameOf(stored));
        assertEquals(0, service().recall(conversation, messageSeq, alice).code());

        PullPendingResponse pulled = service().pullPending(PullPendingRequest.newBuilder()
                .setUserId(bob).setDeviceId("web").setMax(50).build());

        List<String> frames = pulled.getFramesList();
        assertFalse(frames.isEmpty(), "离线帧应该还在（撤回不是删除，占位仍要下发）");
        assertTrue(frames.stream().anyMatch(frame -> frame.contains("\"recalled\":true")),
                "补发必须改写为已撤回形态: " + frames);
        assertTrue(frames.stream().noneMatch(frame -> frame.contains("secret-content")),
                "撤回后补发不能出现原文: " + frames);
    }

    // ==== 辅助 ====

    private MessageRepository messages() {
        return new RedisMessageRepository(TestRedis.template(), mapper);
    }

    private ClientFrames frames() {
        return new ClientFrames(mapper);
    }

    private OfflineMessageStore offline() {
        if (offline == null) {
            offline = new OfflineMessageStore(TestRedis.template(), TTL, 500, TTL);
        }
        return offline;
    }

    private MessageService service() {
        return service(Duration.ofMinutes(2));
    }

    private MessageService service(Duration recallWindow) {
        return new MessageService(TestRedis.template(), messages(), offline(), frames(),
                delivery, seqAllocator,
                new ClientWatermarkStore(TestRedis.template(), TTL),
                new ConversationStore(TestRedis.template(), TTL, MAX_RECENT),
                groupMembers, null, null, notifier(), recallWindow);
    }

    /** 变更事件通知器：真正走一遍实现，推送落到 {@link #pushed} 队列 */
    @SuppressWarnings("unchecked")
    private MessageUpdateNotifier notifier() {
        if (notifier == null) {
            ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
            PushPort pushPort = task -> {
                pushed.add(task);
                return PushResult.newBuilder()
                        .addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
            };
            when(provider.getIfAvailable()).thenReturn(pushPort);
            notifier = new MessageUpdateNotifier(frames(), provider, true);
        }
        return notifier;
    }

    private long submit(String from, String to, String body) {
        SubmitMessageResponse response = service().submit(SubmitMessageRequest.newBuilder()
                .setClientMsgId("c-" + UUID.randomUUID())
                .setConversationId(MessageEnvelope.singleConversationId(from, to))
                .setConvType(MessageEnvelope.CONV_SINGLE)
                .setFromUserId(from)
                .setReceiverId(to)
                .setPayload(body)
                .setClientTs(System.currentTimeMillis())
                .build());
        assertEquals(0, response.getCode(), "准备数据失败: " + response.getMessage());
        return response.getSeq();
    }

    private String single(String a, String b) {
        String conversationId = MessageEnvelope.singleConversationId(a, b);
        conversationIds.add(conversationId);
        return conversationId;
    }

    private String newUser() {
        String userId = "life-test-" + UUID.randomUUID();
        userIds.add(userId);
        return userId;
    }
}
