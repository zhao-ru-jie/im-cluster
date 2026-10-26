package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link DeliveryDispatcher} 的三条硬约定：
 * <b>不阻塞调用方</b>、<b>永不吃掉消息</b>、<b>并发满时退化而不是排队</b>。
 *
 * <p>这三条任何一条破了后果都很重：阻塞调用方等于 ack 又等回推送；
 * 吃掉消息等于客户端拿到 seq 但永远收不到内容；无限排队则会把堆内存堆穿。
 */
class DeliveryDispatcherTest {

    private final MessageRouter router = mock(MessageRouter.class);
    private final OfflineMessageStore offlineStore = mock(OfflineMessageStore.class);
    private final MessageEnvelope envelope = mock(MessageEnvelope.class);

    @Test
    @DisplayName("dispatch 立即返回，推送在后台线程执行")
    void dispatchReturnsBeforeDeliveryFinishes() throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(envelope.conversationId()).thenReturn("c1");
        when(router.deliver(any(), anyString(), anyString())).thenAnswer(invocation -> {
            started.countDown();
            release.await(3, TimeUnit.SECONDS);
            return List.<String>of();
        });

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(router, offlineStore, 8);
        try {
            dispatcher.dispatch(envelope, "frame", "trace", 1L);
            assertTrue(started.await(2, TimeUnit.SECONDS), "推送应该在后台开始");
            // 关键断言：上面 dispatch 早就返回了，而推送此刻还卡在 release 上
            assertEquals(1, dispatcher.inFlight(), "推送未完成时应占用一个并发许可");
        } finally {
            release.countDown();
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("路由抛异常时不向外抛，接收方仍然落离线")
    void routerFailureFallsBackToOffline() throws Exception {
        when(envelope.conversationId()).thenReturn("c2");
        when(envelope.to()).thenReturn("bob");
        when(router.deliver(any(), anyString(), anyString()))
                .thenThrow(new IllegalStateException("router boom"));
        CountDownLatch stored = new CountDownLatch(1);
        doAnswer(invocation -> {
            stored.countDown();
            return 1;
        }).when(offlineStore).storeAll(anyCollection(), anyString());

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(router, offlineStore, 8);
        try {
            dispatcher.dispatch(envelope, "frame", "trace", 2L);
            assertTrue(stored.await(2, TimeUnit.SECONDS), "路由异常时必须落离线，否则消息真的丢了");
        } finally {
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("并发许可打满时就地同步执行，绝不无界排队")
    void degradesToSynchronousWhenInflightSaturated() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        when(envelope.conversationId()).thenReturn("c3");
        when(router.deliver(any(), anyString(), anyString())).thenAnswer(invocation -> {
            if (calls.incrementAndGet() == 1) {
                firstStarted.countDown();
                release.await(3, TimeUnit.SECONDS);
            }
            return List.<String>of();
        });

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(router, offlineStore, 1);
        try {
            dispatcher.dispatch(envelope, "frame", "trace", 3L);
            assertTrue(firstStarted.await(2, TimeUnit.SECONDS), "第一个推送应占满唯一的许可");

            // 许可已被占满：这次 dispatch 必须就地同步做完再返回
            dispatcher.dispatch(envelope, "frame", "trace", 4L);
            verify(router, times(2)).deliver(any(), anyString(), anyString());
        } finally {
            release.countDown();
            dispatcher.shutdown();
        }
    }

    @Test
    @DisplayName("关闭后仍能投递（退化为同步），不会静默丢消息")
    void dispatchStillDeliversAfterShutdown() {
        when(envelope.conversationId()).thenReturn("c4");
        when(router.deliver(any(), anyString(), anyString())).thenReturn(List.of());

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(router, offlineStore, 8);
        dispatcher.shutdown();

        dispatcher.dispatch(envelope, "frame", "trace", 5L);

        verify(router, times(1)).deliver(any(), anyString(), anyString());
    }

    @Test
    @DisplayName("群聊落离线只记补发指针，不按人存整帧")
    void groupOfflineRecordsPointerInsteadOfFrames() throws Exception {
        when(envelope.conversationId()).thenReturn("g:1");
        when(envelope.isGroup()).thenReturn(true);
        when(router.deliver(any(), anyString(), anyString())).thenReturn(List.of("bob", "carol"));
        CountDownLatch recorded = new CountDownLatch(1);
        doAnswer(invocation -> {
            recorded.countDown();
            return 2;
        }).when(offlineStore).recordGroupPending(anyCollection(), anyString(), anyLong());

        DeliveryDispatcher dispatcher = new DeliveryDispatcher(router, offlineStore, 8);
        try {
            dispatcher.dispatch(envelope, "frame", "trace", 9L);
            assertTrue(recorded.await(2, TimeUnit.SECONDS), "群聊失败成员必须记补发指针");

            verify(offlineStore).recordGroupPending(List.of("bob", "carol"), "g:1", 9L);
            // 关键断言：群聊一旦退化成按人存整帧，一条消息就是「不在线人数份」相同内容
            verify(offlineStore, never()).storeAll(anyCollection(), anyString());
        } finally {
            dispatcher.shutdown();
        }
    }
}
