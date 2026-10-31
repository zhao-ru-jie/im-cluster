package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.PushPort;
import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.DeliveredAck;
import com.github.im.proto.ingress.v1.DeliveredAckRequest;
import com.github.im.proto.ingress.v1.DeliveredAckResponse;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.metrics.MessageMetrics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 端到端 ACK（{@code msg.recv} → {@code AckDelivered}）。
 *
 * <p>这是「delivered」唯一真实的来源。在此之前只有两个近似：
 * <ul>
 *   <li>{@code AckPending}：网关 {@code writeAndFlush} 成功 —— 只代表进了 socket 缓冲区，
 *       客户端可能早就断网；</li>
 *   <li>{@code sync} 的 watermarks：客户端主动上报，是懒的、会话级的对账。</li>
 * </ul>
 *
 * <p>这里锁住四件事：
 * <ol>
 *   <li>ACK 会推进该设备的 delivered 水位，并落 Redis（进程重启也在）；</li>
 *   <li>同一批里同一会话多条会被 max 压成一次写（否则就是「消息数」次 HSET）；</li>
 *   <li>水位按设备隔离，且只前进；</li>
 *   <li><b>最关键</b>：确认过的消息不会再被补发一遍——ACK 不是个指标，它会抬高补发起点。</li>
 * </ol>
 *
 * <p>需要本机 Redis，不可用时自动跳过。
 */
class DeliveredAckTest {

    private static final Duration TTL = Duration.ofHours(1);
    private static final String WEB = "web";
    private static final String PHONE = "phone";

    private final MessageRepository repository = mock(MessageRepository.class);
    private final DeliveryDispatcher delivery = mock(DeliveryDispatcher.class);
    private final SeqAllocator seqAllocator = mock(SeqAllocator.class);
    private final GroupMemberRepository groupMembers = mock(GroupMemberRepository.class);
    private final List<String> userIds = new ArrayList<>();
    private final List<String> conversationIds = new ArrayList<>();
    /** 捕获 msg.state 回执：推送是异步的，用队列等而不是 sleep */
    private final BlockingQueue<PushTask> pushed = new LinkedBlockingQueue<>();
    private MessageStateNotifier notifier;

    @AfterEach
    void cleanup() {
        if (notifier != null) {
            notifier.shutdown();
        }
        StringRedisTemplate redis = TestRedis.template();
        for (String userId : userIds) {
            redis.delete(ClientWatermarkStore.keyOf(userId, WEB));
            redis.delete(ClientWatermarkStore.keyOf(userId, PHONE));
            redis.delete(RedisKeys.userOfflinePtr(userId));
            redis.delete(OfflineMessageStore.groupCursorKeyOf(userId, WEB));
            redis.delete(OfflineMessageStore.groupCursorKeyOf(userId, PHONE));
            redis.delete(OfflineMessageStore.cursorKeyOf(userId, WEB));
        }
        for (String conversationId : conversationIds) {
            redis.delete(RedisKeys.convMeta(conversationId));
        }
    }

    @Test
    @DisplayName("ACK 推进该设备的 delivered 水位并落 Redis")
    void ackAdvancesWatermark() {
        String userId = newUser();
        String conversation = newConversation();

        DeliveredAckResponse response = service().ackDelivered(request(userId, WEB,
                ack(conversation, 5)));

        assertEquals(0, response.getCode());
        assertEquals(1, response.getAccepted());
        assertEquals(5, watermarkStore().deliveredSeq(userId, WEB, conversation));
    }

    @Test
    @DisplayName("同一批里同一会话多条，按 max 压成一次写")
    void batchCompressedToMaxPerConversation() {
        String userId = newUser();
        String a = newConversation();
        String b = newConversation();

        DeliveredAckResponse response = service().ackDelivered(request(userId, WEB,
                ack(a, 3), ack(a, 9), ack(a, 7), ack(b, 4)));

        assertEquals(2, response.getAccepted(), "4 条 ACK 压在 2 个会话上，写 2 次而不是 4 次");
        assertEquals(9, watermarkStore().deliveredSeq(userId, WEB, a), "同会话取最大，不是最后一条");
        assertEquals(4, watermarkStore().deliveredSeq(userId, WEB, b));
    }

    @Test
    @DisplayName("水位按设备隔离：手机确认不影响网页")
    void ackIsPerDevice() {
        String userId = newUser();
        String conversation = newConversation();

        service().ackDelivered(request(userId, WEB, ack(conversation, 20)));

        assertEquals(20, watermarkStore().deliveredSeq(userId, WEB, conversation));
        assertEquals(0, watermarkStore().deliveredSeq(userId, PHONE, conversation),
                "网页还没确认过，它的补发起点不能因为手机确认了就被抬高");
    }

    @Test
    @DisplayName("水位只前进：乱序到达的旧 ACK 不会把它拉回去")
    void ackNeverGoesBackwards() {
        String userId = newUser();
        String conversation = newConversation();

        service().ackDelivered(request(userId, WEB, ack(conversation, 20)));
        service().ackDelivered(request(userId, WEB, ack(conversation, 12)));

        assertEquals(20, watermarkStore().deliveredSeq(userId, WEB, conversation),
                "回退会让补发起点倒退，把已经确认过的消息重新发一遍");
    }

    @Test
    @DisplayName("客户端确认过的消息不会再被补发一遍（ACK 抬高补发起点）")
    void ackRaisesPullStartSoConfirmedMessagesAreNotResent() {
        String userId = newUser();
        String conversation = newConversation();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7), message(conversation, 8)));

        // 第一次补发：从锚点 7 开始，发出 7、8
        PullPendingResponse first = service().pullPending(pullRequest(userId, WEB));
        assertEquals(2, first.getFramesCount());

        // 客户端回端到端 ACK：收到了 8
        service().ackDelivered(request(userId, WEB, ack(conversation, 8)));

        // 第二次补发：起点应该被 ACK 抬到 9，而不是又回到锚点 7
        when(repository.since(eq(conversation), eq(8L), anyInt())).thenReturn(List.of());
        PullPendingResponse second = service().pullPending(pullRequest(userId, WEB));

        assertEquals(0, second.getFramesCount(),
                "ACK 若只写水位而不参与起点计算，客户端的确认就白做了——消息会被无限重发");
        verify(repository).since(eq(conversation), eq(8L), anyInt());
    }

    @Test
    @DisplayName("空 ACK / 无效条目直接忽略，不打 Redis")
    void emptyOrInvalidAcksAreIgnored() {
        String userId = newUser();
        String conversation = newConversation();

        DeliveredAckResponse empty = service().ackDelivered(DeliveredAckRequest.newBuilder()
                .setUserId(userId).setDeviceId(WEB).build());
        assertEquals(0, empty.getAccepted());

        DeliveredAckResponse invalid = service().ackDelivered(request(userId, WEB,
                ack(conversation, 0), ack("", 5)));
        assertEquals(0, invalid.getAccepted(), "seq<=0 和空会话 ID 都该被丢掉");
        assertEquals(0, watermarkStore().deliveredSeq(userId, WEB, conversation));
    }

    @Test
    @DisplayName("缺 user_id 返回 400，而不是静默当成匿名")
    void missingUserIdIsRejected() {
        DeliveredAckResponse response = service().ackDelivered(DeliveredAckRequest.newBuilder()
                .setDeviceId(WEB)
                .addAcks(ack("s:a:b", 1))
                .build());

        assertEquals(400, response.getCode());
        assertTrue(response.getMessage().contains("user_id"));
    }

    @Test
    @DisplayName("ACK 之后向对端推一条 msg.state=DELIVERED（发送方终于能看到「已送达」）")
    void ackPushesDeliveredStateToPeer() throws Exception {
        String reader = newUser();
        String sender = newUser();
        String conversation = singleConversation(reader, sender);

        service().ackDelivered(request(reader, WEB, ack(conversation, 5)));

        PushTask task = pushed.poll(3, TimeUnit.SECONDS);
        assertNotNull(task, "ACK 之后应该给会话对端推一条状态回执");
        assertEquals(List.of(sender), task.getTargetUserIdsList(), "回执推给对端，而不是自己");
        assertTrue(task.getFrame().contains("\"type\":\"msg.state\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"state\":\"DELIVERED\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"seq\":5"), task.getFrame());
        assertTrue(task.getFrame().contains("\"userId\":\"" + reader + "\""), task.getFrame());
    }

    @Test
    @DisplayName("重复 ACK / 水位没前进，不再重推同一条回执")
    void repeatedAckDoesNotPushAgain() throws Exception {
        String reader = newUser();
        String sender = newUser();
        String conversation = singleConversation(reader, sender);

        service().ackDelivered(request(reader, WEB, ack(conversation, 5)));
        assertNotNull(pushed.poll(3, TimeUnit.SECONDS), "第一次确认要推");
        service().ackDelivered(request(reader, WEB, ack(conversation, 5)));

        assertNull(pushed.poll(300, TimeUnit.MILLISECONDS),
                "水位没前进就不该再推——多端各报一次会把回执放大成 N 条");
    }

    @Test
    @DisplayName("群聊不发单点回执：群里「谁读到哪」是另一个模型")
    void groupAckPushesNoState() throws Exception {
        String reader = newUser();
        String conversation = "g:g-" + UUID.randomUUID();
        conversationIds.add(conversation);

        service().ackDelivered(request(reader, WEB, ack(conversation, 9)));

        assertNull(pushed.poll(300, TimeUnit.MILLISECONDS), "群聊不该推单点状态回执");
    }

    // ==== 辅助 ====

    private MessageService service() {
        return new MessageService(TestRedis.template(), repository, store(),
                new ClientFrames(), delivery, seqAllocator, watermarkStore(),
                new ConversationStore(TestRedis.template(), TTL, 1000), groupMembers, stateNotifier(),
                null, null, null, Duration.ofMinutes(2), new MessageMetrics(null), null);
    }

    /** 状态回执器：真正走一遍 {@link MessageStateNotifier}，推送落到 {@link #pushed} 队列里 */
    @SuppressWarnings("unchecked")
    private MessageStateNotifier stateNotifier() {
        if (notifier == null) {
            ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
            PushPort pushPort = task -> {
                pushed.add(task);
                return PushResult.newBuilder().addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
            };
            when(provider.getIfAvailable()).thenReturn(pushPort);
            notifier = new MessageStateNotifier(new ClientFrames(), provider, true);
        }
        return notifier;
    }

    /** 单聊会话 ID（{@code s:a:b}）：状态回执要靠它解析对端，不能用随机会话名 */
    private String singleConversation(String a, String b) {
        String conversationId = MessageEnvelope.singleConversationId(a, b);
        conversationIds.add(conversationId);
        return conversationId;
    }

    private OfflineMessageStore store() {
        return new OfflineMessageStore(TestRedis.template(), TTL, 500, TTL);
    }

    private ClientWatermarkStore watermarkStore() {
        return new ClientWatermarkStore(TestRedis.template(), TTL);
    }

    private static PullPendingRequest pullRequest(String userId, String deviceId) {
        return PullPendingRequest.newBuilder().setUserId(userId).setDeviceId(deviceId).setMax(200).build();
    }

    private static DeliveredAckRequest request(String userId, String deviceId, DeliveredAck... acks) {
        return DeliveredAckRequest.newBuilder()
                .setUserId(userId)
                .setDeviceId(deviceId)
                .addAllAcks(List.of(acks))
                .build();
    }

    private static DeliveredAck ack(String conversationId, long seq) {
        return DeliveredAck.newBuilder().setConversationId(conversationId).setSeq(seq).build();
    }

    private static StoredMessage message(String conversation, long seq) {
        return new StoredMessage(conversation, seq, "srv-" + seq, "alice", "bob", "GROUP",
                "c-" + seq, "hi", 1_700_000_000_000L + seq);
    }

    private String newConversation() {
        String conversationId = TestRedis.conversationId();
        conversationIds.add(conversationId);
        return conversationId;
    }

    private String newUser() {
        String userId = "ack-test-" + UUID.randomUUID();
        userIds.add(userId);
        return userId;
    }
}
