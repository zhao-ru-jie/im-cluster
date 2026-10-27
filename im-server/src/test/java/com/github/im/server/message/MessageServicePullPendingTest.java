package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.AckPendingRequest;
import com.github.im.proto.ingress.v1.AckPendingResponse;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 离线补发的「至少一次」约定。
 *
 * <p>改造前这里是 at-most-once：单聊用 {@code LRANGE + LTRIM} 把帧原子移走，网关拿到之后才写 socket，
 * 一帧写失败就 {@code break}，剩下的帧永远消失；群聊指针更是<b>读的时候就推进</b>了。
 * 现在锁住的是相反的一组约定：
 *
 * <ol>
 *   <li><b>读不消费</b>：{@code pullPending} 之后队列和指针都不变，所以「读到但没发出去」不会丢；</li>
 *   <li><b>确认才推进</b>：只有 {@code ackPending}（网关全部写成功后调用）才移动游标；</li>
 *   <li><b>确认只前进</b>：重复确认、旧连接的迟到的确认都是空操作，不会导致回退死循环；</li>
 *   <li><b>游标按设备隔离</b>：手机确认掉的那批，网页重连时仍然补得到；</li>
 *   <li>客户端水位、历史查询失败不动指针这两条老约定保持不变。</li>
 * </ol>
 *
 * <p>需要本机 Redis（队列、游标、指针都在 Redis 里），不可用时自动跳过。
 */
class MessageServicePullPendingTest {

    private static final Duration TTL = Duration.ofDays(1);
    private static final String WEB = "web";
    private static final String PHONE = "phone";

    private final MessageRepository repository = mock(MessageRepository.class);
    private final DeliveryDispatcher delivery = mock(DeliveryDispatcher.class);
    private final SeqAllocator seqAllocator = mock(SeqAllocator.class);
    private final ClientWatermarkStore watermarkStore = mock(ClientWatermarkStore.class);
    private final List<String> userIds = new ArrayList<>();

    @AfterEach
    void cleanup() {
        StringRedisTemplate redis = TestRedis.template();
        for (String id : userIds) {
            redis.delete(OfflineMessageStore.queueKeyOf(id));
            redis.delete(OfflineMessageStore.queueKeyOf(id) + ":pos");
            redis.delete(OfflineMessageStore.cursorKeyOf(id, WEB));
            redis.delete(OfflineMessageStore.cursorKeyOf(id, PHONE));
            redis.delete(OfflineMessageStore.cursorKeyOf(id, "default"));
            redis.delete(OfflineMessageStore.groupCursorKeyOf(id, WEB));
            redis.delete(OfflineMessageStore.groupCursorKeyOf(id, PHONE));
            redis.delete(RedisKeys.userDevices(id));
            redis.delete(RedisKeys.userOfflinePtr(id));
            redis.delete(RedisKeys.userLegacyOffline(id));
        }
    }

    // ==== 单聊帧队列 ====

    @Test
    @DisplayName("单聊：重复拉取拿到同一批帧，直到明确确认才前进")
    void singleChatFramesSurviveUntilAcked() {
        String userId = newUser();
        store().store(userId, frame(1));
        store().store(userId, frame(2));

        PullPendingResponse first = service().pullPending(request(userId, WEB, 200));
        assertEquals(2, first.getFramesCount(), "第一次应拿到两条");
        assertTrue(first.getAckSingleSeq() > 0, "必须带上待确认位置，否则服务端永远不知道该推进到哪");

        // 关键：网关写失败（没有 ack）→ 再拉一次必须还能拿到同样两条
        PullPendingResponse second = service().pullPending(request(userId, WEB, 200));
        assertEquals(2, second.getFramesCount(), "没确认就不能消失，这是 at-least-once 的全部意义");
        assertEquals(first.getAckSingleSeq(), second.getAckSingleSeq(), "两次读到的是同一批位置");
    }

    @Test
    @DisplayName("单聊：确认之后不再重复返回已确认的帧")
    void singleChatCursorAdvancesAfterAck() {
        String userId = newUser();
        store().store(userId, frame(1));
        store().store(userId, frame(2));

        PullPendingResponse pulled = service().pullPending(request(userId, WEB, 200));
        assertEquals(2, pulled.getFramesCount());
        service().ackPending(ack(userId, WEB, pulled));

        PullPendingResponse afterAck = service().pullPending(request(userId, WEB, 200));
        assertEquals(0, afterAck.getFramesCount(), "已确认的帧不该再补一遍");
        assertEquals(0, afterAck.getRemaining(), "remaining 要按「未确认条数」算，否则客户端会无限重拉");

        // 新来的消息仍然能补
        store().store(userId, frame(3));
        PullPendingResponse third = service().pullPending(request(userId, WEB, 200));
        assertEquals(1, third.getFramesCount());
        assertEquals(frame(3), third.getFrames(0));
    }

    @Test
    @DisplayName("单聊：游标按设备隔离，一端确认不影响另一端")
    void singleChatCursorIsPerDevice() {
        String userId = newUser();
        store().store(userId, frame(1));

        PullPendingResponse forWeb = service().pullPending(request(userId, WEB, 200));
        assertEquals(1, forWeb.getFramesCount());
        service().ackPending(ack(userId, WEB, forWeb));

        assertEquals(0, service().pullPending(request(userId, WEB, 200)).getFramesCount(),
                "网页已确认，不该再收到");
        assertEquals(1, service().pullPending(request(userId, PHONE, 200)).getFramesCount(),
                "手机还没确认过，必须还能补到——用户级单一游标会让这台设备永久丢消息");
    }

    @Test
    @DisplayName("单聊：确认只前进，迟到的旧确认不会把游标拉回去")
    void ackNeverMovesCursorBackwards() {
        String userId = newUser();
        store().store(userId, frame(1));
        store().store(userId, frame(2));

        PullPendingResponse pulled = service().pullPending(request(userId, WEB, 200));
        long high = pulled.getAckSingleSeq();
        service().ackPending(AckPendingRequest.newBuilder()
                .setUserId(userId).setDeviceId(WEB).setAckSingleSeq(high).build());

        // 模拟旧连接的确认晚到（位置更小）
        AckPendingResponse stale = service().ackPending(AckPendingRequest.newBuilder()
                .setUserId(userId).setDeviceId(WEB).setAckSingleSeq(1L).build());

        assertEquals(high, stale.getSingleCursor(), "游标只能前进，回退会造成重复补发的死循环");
        assertEquals(0, service().pullPending(request(userId, WEB, 200)).getFramesCount());
    }

    // ==== 群聊补发指针 ====

    @Test
    @DisplayName("群聊：拉取不推进任何进度，确认后只推进本设备的游标")
    void groupDeviceCursorMovesOnlyAfterAck() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7), message(conversation, 8)));

        PullPendingResponse pulled = service().pullPending(request(userId, WEB, 200));

        assertEquals(2, pulled.getFramesCount());
        assertTrue(pulled.getFrames(0).contains("\"seq\":7"), pulled.getFrames(0));
        assertTrue(pulled.getFrames(0).contains("\"type\":\"msg.push\""), "补发的帧必须与实时推送同构");
        assertEquals(0L, store().groupCursor(userId, WEB, conversation),
                "拉取阶段绝不能推进进度：网关这时还没把帧写出去");
        assertEquals(9L, pulled.getAckGroupSeqsMap().get(conversation), "要把「补到哪」告诉网关，等它写成功再确认");

        service().ackPending(ack(userId, WEB, pulled));

        assertEquals(9L, store().groupCursor(userId, WEB, conversation),
                "确认之后才前进到「已补发最后一条 +1」");
        assertEquals(7L, store().pendingConversations(userId, 10).get(conversation),
                "锚点是共享下界，一台设备补完不能把它前移");
    }

    @Test
    @DisplayName("群聊：一端补完后另一端仍能从自己的起点补齐（多端同步的核心）")
    void groupMultiDeviceEachKeepsOwnProgress() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7), message(conversation, 8)));

        // 网页先上线补完
        PullPendingResponse forWeb = service().pullPending(request(userId, WEB, 200));
        assertEquals(2, forWeb.getFramesCount());
        service().ackPending(ack(userId, WEB, forWeb));

        // 手机后上线：起点必须还是自己的锚点 7，而不是网页补到的 9
        PullPendingResponse forPhone = service().pullPending(request(userId, PHONE, 200));
        assertEquals(2, forPhone.getFramesCount(), "手机不能因为网页补过就永久缺这段消息");
        // 两次都以 from=6（即 seq > 6）查历史 = 两台设备各自从 7 开始补
        verify(repository, times(2)).since(eq(conversation), eq(6L), anyInt());

        // 网页再次上线：已经补到 9，不该重复
        when(repository.since(eq(conversation), eq(8L), anyInt())).thenReturn(List.of());
        assertEquals(0, service().pullPending(request(userId, WEB, 200)).getFramesCount());
    }

    @Test
    @DisplayName("群聊：所有登记设备都补完，锚点才退休")
    void groupAnchorRetiresOnlyAfterAllDevices() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7)));
        // 网关登记了两台设备
        TestRedis.template().opsForSet().add(RedisKeys.userDevices(userId), WEB, PHONE);

        PullPendingResponse forWeb = service().pullPending(request(userId, WEB, 200));
        service().ackPending(ack(userId, WEB, forWeb));
        assertEquals(7L, store().pendingConversations(userId, 10).get(conversation),
                "手机还没补，锚点不能删——删了手机就会从 seq 1 重拉整个历史");

        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7)));
        PullPendingResponse forPhone = service().pullPending(request(userId, PHONE, 200));
        service().ackPending(ack(userId, PHONE, forPhone));

        assertTrue(store().pendingConversations(userId, 10).isEmpty(),
                "所有端都补过了，锚点就该退休，否则每轮 pull 都为它白查一次历史");
    }

    @Test
    @DisplayName("群聊：历史已经空了才清指针（唯一可以安全删除的时机）")
    void groupPointerClearedOnlyWhenNothingLeft() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), anyLong(), anyInt())).thenReturn(List.of());

        PullPendingResponse response = service().pullPending(request(userId, WEB, 200));

        assertEquals(0, response.getFramesCount());
        assertTrue(store().pendingConversations(userId, 10).isEmpty(), "查不到数据说明已经补完，可以安全清理");
        assertTrue(response.getAckGroupSeqsMap().isEmpty(), "没有帧就没有要确认的东西");
    }

    @Test
    @DisplayName("群聊：历史查询失败时指针原地不动，也没有确认项")
    void keepsPointerWhenHistoryQueryFails() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), anyLong(), anyInt()))
                .thenThrow(new IllegalStateException("db down"));

        PullPendingResponse response = service().pullPending(request(userId, WEB, 200));

        assertEquals(0, response.getFramesCount());
        assertEquals(7L, store().pendingConversations(userId, 10).get(conversation),
                "指针被推进去就等于永久丢一段消息");
        assertTrue(response.getAckGroupSeqsMap().isEmpty(), "查询失败不能产生确认项");
        assertTrue(response.getRemaining() > 0, "没补成功必须让客户端知道还有东西没拉完");
    }

    @Test
    @DisplayName("群聊：预算被吃满时 remaining 提示继续拉，且不提前清指针")
    void reportsRemainingWhenBudgetExhausted() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7), message(conversation, 8)));

        PullPendingResponse response = service().pullPending(request(userId, WEB, 2));

        assertEquals(2, response.getFramesCount());
        assertEquals(1, response.getRemaining());
        assertEquals(7L, store().pendingConversations(userId, 10).get(conversation));
    }

    @Test
    @DisplayName("客户端上报过水位时从水位之后开始，不重复补已收到的消息")
    void honoursClientWatermark() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), anyLong(), anyInt())).thenReturn(List.of());

        service().pullPending(PullPendingRequest.newBuilder()
                .setUserId(userId)
                .setDeviceId(WEB)
                .setMax(200)
                .putWatermarks(conversation, 8L)
                .build());

        verify(repository).since(eq(conversation), eq(8L), anyInt());
    }

    // ==== 辅助 ====

    private MessageService service() {
        return new MessageService(TestRedis.template(), repository, store(),
                new ClientFrames(new ObjectMapper()), delivery, seqAllocator, watermarkStore,
                new ConversationStore(TestRedis.template(), TTL, 1000),
                mock(GroupMemberRepository.class), null, null, null, Duration.ofMinutes(2));
    }

    private OfflineMessageStore store() {
        return new OfflineMessageStore(TestRedis.template(), TTL, 500, TTL);
    }

    private static PullPendingRequest request(String userId, String deviceId, int max) {
        return PullPendingRequest.newBuilder().setUserId(userId).setDeviceId(deviceId).setMax(max).build();
    }

    private static AckPendingRequest ack(String userId, String deviceId, PullPendingResponse pulled) {
        return AckPendingRequest.newBuilder()
                .setUserId(userId)
                .setDeviceId(deviceId)
                .setAckSingleSeq(pulled.getAckSingleSeq())
                .putAllAckGroupSeqs(pulled.getAckGroupSeqsMap())
                .build();
    }

    private static String frame(long seq) {
        return "{\"type\":\"msg.push\",\"seq\":" + seq + "}";
    }

    private static StoredMessage message(String conversation, long seq) {
        return new StoredMessage(conversation, seq, "srv-" + seq, "alice", "bob", "GROUP",
                "c-" + seq, "hi", 1_700_000_000_000L + seq);
    }

    private String newUser() {
        String userId = "pull-test-" + UUID.randomUUID();
        userIds.add(userId);
        return userId;
    }
}
