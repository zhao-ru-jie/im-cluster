package com.github.im.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * {@code onGap} / 空洞判定的回归：<b>本端自己发出的 seq 不算丢消息</b>。
 *
 * <p>会话的 seq 是会话级共享的发号器，双方都发消息时收到的 seq 天然是隔一个的
 * （2、4、6……），中间那些号是本端自己刚发出去、已经 ack 过的。修复前 SDC 只拿
 * 「已收到」的水位判空洞，于是每收一条就回调一次 {@code onGap}，宿主照着提示反复拉历史。
 *
 * <p>两个用例分别盯住两件事：
 * <ul>
 *   <li>缺口里全是自己发的号 → <b>不许</b>回调；</li>
 *   <li>缺口里有谁都没发的号 → <b>必须</b>回调，且 {@code expectedFrom} 指向真正缺的那个。</li>
 * </ul>
 *
 * <p>桩网关直接复用 {@link ImClientIntegrationTest.StubGateway}（真 TCP、真长度前缀），
 * 这里只负责编排「谁在什么时候发了 seq 几」。
 */
class ImClientGapDetectionTest {

    private static final String SINGLE = "s:alice:bob";
    private static final String GROUP = "g:g1";

    private ImClientIntegrationTest.StubGateway gateway;
    private ImClient client;

    @AfterEach
    void tearDown() throws Exception {
        if (client != null) {
            client.close();
        }
        if (gateway != null) {
            gateway.close();
        }
    }

    @Test
    @DisplayName("单聊双方互发：中间缺的号是自己发的 → 不报空洞；真缺口（谁都没发）→ 必须报")
    void gapIgnoresOwnSeqInSingleChat() throws Exception {
        Recorder recorder = new Recorder();
        AtomicLong serverSeq = new AtomicLong();
        AtomicReference<ImClientIntegrationTest.StubGateway.Conn> conn = new AtomicReference<>();

        gateway = startGateway(conn, serverSeq);
        client = newClient("alice", recorder);
        client.connect();
        awaitTrue("connect + connected 帧", () -> client.state() == ImClient.State.CONNECTED);
        awaitTrue("桩网关记录连接", () -> conn.get() != null);

        // 1) 我发第 1 条 → 服务端给会话 seq=1（这个号是我占的）
        client.send(OutgoingMessage.text("bob", "alice-1"));
        awaitTrue("ack#1", () -> recorder.acked.get() >= 1);

        // 2) 对端发来 seq=2（对端占号）
        serverSeq.set(2);
        conn.get().send(push(SINGLE, OutgoingMessage.CONV_SINGLE, 2, "bob", "bob-1"));
        awaitTrue("收到 seq=2", () -> lastInbound(SINGLE) >= 2);

        // 3) 我再发第 2 条 → 服务端给 seq=3（又是我占的）
        client.send(OutgoingMessage.text("bob", "alice-2"));
        awaitTrue("ack#2", () -> recorder.acked.get() >= 2);

        // 4) 对端发来 seq=4：水位 2→4 之间只缺 3，而 3 是我自己发的 → 不该报空洞
        serverSeq.set(4);
        conn.get().send(push(SINGLE, OutgoingMessage.CONV_SINGLE, 4, "bob", "bob-2"));
        awaitTrue("收到 seq=4", () -> lastInbound(SINGLE) >= 4);
        Thread.sleep(150); // 给读线程把（万一有的）onGap 执行完的时间窗
        assertEquals(0, recorder.gapCount(), "中间缺的 3 是本端自己发出的，不是丢消息");

        // 5) 对端发来 seq=6：水位 4→6 之间缺 5，而 5 谁都没发 → 必须报，且起点是 5
        serverSeq.set(6);
        conn.get().send(push(SINGLE, OutgoingMessage.CONV_SINGLE, 6, "bob", "bob-3"));
        awaitTrue("收到 seq=6", () -> lastInbound(SINGLE) >= 6);
        awaitTrue("onGap(5,6)", () -> recorder.gapCount() >= 1);

        assertEquals(1, recorder.gapCount(), "5 是真正的空洞，应回调一次 onGap");
        assertEquals(5, recorder.gaps.get(0)[0], "expectedFrom 应跳过自己发的号");
        assertEquals(6, recorder.gaps.get(0)[1]);
    }

    @Test
    @DisplayName("群聊：会话 ID 由请求里带，同样按「自己发的号不算空洞」判定")
    void gapIgnoresOwnSeqInGroupChat() throws Exception {
        Recorder recorder = new Recorder();
        AtomicLong serverSeq = new AtomicLong();
        AtomicReference<ImClientIntegrationTest.StubGateway.Conn> conn = new AtomicReference<>();

        gateway = startGateway(conn, serverSeq);
        client = newClient("alice", recorder);
        client.connect();
        awaitTrue("connect + connected 帧", () -> client.state() == ImClient.State.CONNECTED);
        awaitTrue("桩网关记录连接", () -> conn.get() != null);

        // 群聊：客户端自己带 conversationId / convType，服务端按会话发号
        client.send(OutgoingMessage.builder()
                .conversationId(GROUP).convType(OutgoingMessage.CONV_GROUP).body("alice-g1").build());
        awaitTrue("群消息 ack", () -> recorder.acked.get() >= 1);

        serverSeq.set(2);
        conn.get().send(push(GROUP, OutgoingMessage.CONV_GROUP, 2, "carol", "carol-1"));
        awaitTrue("收到 seq=2", () -> lastInbound(GROUP) >= 2);

        client.send(OutgoingMessage.builder()
                .conversationId(GROUP).convType(OutgoingMessage.CONV_GROUP).body("alice-g2").build());
        awaitTrue("群消息 ack#2", () -> recorder.acked.get() >= 2);

        // 水位 2 → 4，缺 3，而 3 是自己发的 → 不报
        serverSeq.set(4);
        conn.get().send(push(GROUP, OutgoingMessage.CONV_GROUP, 4, "carol", "carol-2"));
        awaitTrue("收到 seq=4", () -> lastInbound(GROUP) >= 4);
        Thread.sleep(150);

        assertEquals(0, recorder.gapCount(), "群聊同理：自己发的 seq 不算空洞");
    }

    // ==== 桩网关与工具 ====

    /**
     * 桩网关：{@code connect} 回 {@code connected}，{@code msg.send} 用共享的 {@code serverSeq}
     * 发号并回 {@code msg.ack}（这样测试可以精确编排「谁的号」）。
     */
    private ImClientIntegrationTest.StubGateway startGateway(
            AtomicReference<ImClientIntegrationTest.StubGateway.Conn> conn, AtomicLong serverSeq) throws Exception {
        return new ImClientIntegrationTest.StubGateway((c, frame) -> {
            switch (JsonUtils.toJsonNode(frame).path("type").asString("")) {
                case Frames.T_CONNECT -> {
                    conn.set(c);
                    c.send(json(Frames.T_CONNECTED, "userId", "alice", "transport", "tcp", "ts", 1L));
                }
                case Frames.T_SEND -> {
                    long seq = serverSeq.incrementAndGet();
                    c.send(json(Frames.T_ACK,
                            "clientMsgId", JsonUtils.toJsonNode(frame).path("clientMsgId").asString(null),
                            "serverMsgId", "srv-" + seq, "seq", seq, "code", 0));
                }
                default -> {
                    // sync / recv / ping 之类与本用例无关
                }
            }
        });
    }

    private ImClient newClient(String userId, ImClientListener listener) {
        ImClientConfig config = ImClientConfig.builder()
                .host("127.0.0.1")
                .port(gateway.port())
                .userId(userId)
                .deviceId("gap-test")
                .connectTimeoutMs(2_000)
                .authTimeoutMs(2_000)
                .recvFlushIntervalMs(50)
                .keepAliveIntervalMs(0)
                .outboxScanIntervalMs(50)
                .sendRetryBaseMs(10)
                .sendRetryMaxMs(40)
                .reconnectPolicy(new ReconnectPolicy(10, 40, 5_000))
                .build();
        return new ImClient(config, listener);
    }

    /** 会话内已收到的最大 seq（就是 SDK 判空洞时用的水位） */
    private long lastInbound(String conversationId) {
        return client.watermarks().getOrDefault(conversationId, 0L);
    }

    private static void awaitTrue(String what, BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(10);
        }
        fail("等待超时：" + what);
    }

    private static String push(String conversationId, String convType, long seq, String from, String body) {
        return json(Frames.T_PUSH, "conversationId", conversationId, "convType", convType, "seq", seq,
                "serverMsgId", "srv-" + seq, "from", from, "to", "alice", "body", body,
                "contentType", OutgoingMessage.CT_TEXT, "ts", 1_700_000_000_000L);
    }

    private static String json(String type, Object... keyValues) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", Frames.VER);
        frame.put("type", type);
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            frame.put((String) keyValues[i], keyValues[i + 1]);
        }
        return JsonUtils.toJson(frame);
    }

    /** 只记 onAck / onGap，其余回调不关心 */
    static final class Recorder implements ImClientListener {

        final AtomicInteger acked = new AtomicInteger();
        final List<long[]> gaps = Collections.synchronizedList(new ArrayList<>());
        final CountDownLatch connected = new CountDownLatch(1);

        @Override
        public void onConnected() {
            connected.countDown();
        }

        @Override
        public void onAck(DownlinkFrame.Ack ack, LocalMessage local) {
            if (ack.ok()) {
                acked.incrementAndGet();
            }
        }

        @Override
        public void onGap(String conversationId, long expectedFrom, long actualSeq) {
            gaps.add(new long[]{expectedFrom, actualSeq});
        }

        int gapCount() {
            synchronized (gaps) {
                return gaps.size();
            }
        }
    }
}
