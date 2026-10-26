package com.github.im.server.message;

import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link MessageService#pullPending} 的补发约定（群聊指针方案）。
 *
 * <p>要锁住的四件事，任何一条破了都会丢消息或放大查询：
 * <ol>
 *   <li>补发出来的帧与实时推送<b>同构</b>（都由 {@link ClientFrames#frameOf} 从 {@link StoredMessage} 渲染）；</li>
 *   <li>历史查询失败时<b>指针原地不动</b>——宁可下次重复补，也不能把指针推过去；</li>
 *   <li>客户端上报过水位就从水位之后开始，避免重复补发已经实时收到的消息；</li>
 *   <li>单聊帧队列（含升级前遗留的群聊整帧）仍然照旧 drain。</li>
 * </ol>
 *
 * <p>需要本机 Redis（指针在 Redis 里），不可用时自动跳过。
 */
class MessageServicePullPendingTest {

    private static final Duration TTL = Duration.ofDays(1);

    private final MessageRepository repository = mock(MessageRepository.class);
    private final DeliveryDispatcher delivery = mock(DeliveryDispatcher.class);
    private final SeqAllocator seqAllocator = mock(SeqAllocator.class);
    private final ClientWatermarkStore watermarkStore = mock(ClientWatermarkStore.class);
    private final List<String> userIds = new ArrayList<>();

    @AfterEach
    void cleanup() {
        StringRedisTemplate redis = TestRedis.template();
        for (String id : userIds) {
            redis.delete(RedisKeys.OFFLINE_PREFIX + id);
            redis.delete(RedisKeys.OFFLINE_PTR_PREFIX + id);
        }
    }

    @Test
    @DisplayName("群聊离线：按指针从历史重建帧，追平后清掉指针")
    void rebuildsGroupFramesFromHistory() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7), message(conversation, 8), message(conversation, 9)));

        PullPendingResponse response = service().pullPending(request(userId, 200));

        assertEquals(3, response.getFramesCount());
        assertTrue(response.getFrames(0).contains("\"seq\":7"), response.getFrames(0));
        assertTrue(response.getFrames(0).contains("\"type\":\"msg.push\""), "补发的帧必须与实时推送同构");
        assertTrue(store().pendingConversations(userId, 10).isEmpty(), "追平后应清掉指针");
        assertEquals(0, response.getRemaining());
    }

    @Test
    @DisplayName("没补完时指针前进到最后一条之后，remaining 提示还要继续拉")
    void advancesPointerWhenBudgetExhausted() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), eq(6L), anyInt()))
                .thenReturn(List.of(message(conversation, 7), message(conversation, 8)));

        PullPendingResponse response = service().pullPending(request(userId, 2));

        assertEquals(2, response.getFramesCount());
        assertEquals(9L, store().pendingConversations(userId, 10).get(conversation),
                "指针应停在「已补发最后一条 +1」，下次从这里继续");
        assertEquals(1, response.getRemaining());
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
                .setMax(200)
                .putWatermarks(conversation, 8L)
                .build());

        verify(repository).since(eq(conversation), eq(8L), anyInt());
    }

    @Test
    @DisplayName("历史查询失败时指针不动，宁可下轮重复补也不能丢")
    void keepsPointerWhenHistoryQueryFails() {
        String userId = newUser();
        String conversation = TestRedis.conversationId();
        store().recordGroupPending(List.of(userId), conversation, 7);
        when(repository.since(eq(conversation), anyLong(), anyInt()))
                .thenThrow(new IllegalStateException("db down"));

        PullPendingResponse response = service().pullPending(request(userId, 200));

        assertEquals(0, response.getFramesCount());
        assertEquals(7L, store().pendingConversations(userId, 10).get(conversation), "指针被推进去就等于永久丢一段消息");
        assertTrue(response.getRemaining() > 0, "没补成功必须让客户端知道还有东西没拉完");
    }

    @Test
    @DisplayName("单聊帧队列仍然照旧 drain（也覆盖升级前遗留的群聊整帧）")
    void stillDrainsSingleChatFrames() {
        String userId = newUser();
        store().store(userId, "{\"type\":\"msg.push\",\"seq\":1}");

        PullPendingResponse response = service().pullPending(request(userId, 200));

        assertEquals(1, response.getFramesCount());
        assertEquals("{\"type\":\"msg.push\",\"seq\":1}", response.getFrames(0));
    }

    // ==== 辅助 ====

    private MessageService service() {
        return new MessageService(TestRedis.template(), repository, store(),
                new ClientFrames(new ObjectMapper()), delivery, seqAllocator, watermarkStore);
    }

    private OfflineMessageStore store() {
        return new OfflineMessageStore(TestRedis.template(), TTL, 500, TTL);
    }

    private static PullPendingRequest request(String userId, int max) {
        return PullPendingRequest.newBuilder().setUserId(userId).setMax(max).build();
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
