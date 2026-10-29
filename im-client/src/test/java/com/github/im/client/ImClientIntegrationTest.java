package com.github.im.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * SDK 的端到端行为：对着一个<b>桩网关</b>（真 TCP、真长度前缀、真 JSON）跑完整链路。
 *
 * <p>为什么不用 mock 掉 {@code GatewayConnection}：这一层最容易错的地方恰恰是
 * 「长度前缀 + 首帧认证 + 断开时机」，mock 掉它等于把最需要验证的东西假设成对的。
 * 桩网关只用 JDK 的 {@link ServerSocket}，因此测试没有额外依赖。
 *
 * <p>三条断言对应三个线上事故：
 * <ul>
 *   <li>ack 丢失 → 必须用同一个 {@code clientMsgId} 重投（否则一条消息变两条）；</li>
 *   <li>补发重复 → 必须按 seq 去重（否则用户看到两遍同样的消息）；</li>
 *   <li>401 → 必须停止重连（否则 token 失效时客户端把自己打进风控）。</li>
 * </ul>
 */
class ImClientIntegrationTest {
    
    private static final String CONV = "s:alice:bob";

    @Test
    @DisplayName("连上后先报水位(sync)，发消息收到 ack 后本地表置 SENT")
    void sendsMessageAndRecordsAck() throws Exception {
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        try (StubGateway gateway = new StubGateway((conn, frame) -> {
            received.add(frame);
            switch (typeOf(frame)) {
                case Frames.T_CONNECT -> conn.send(connected("alice"));
                case Frames.T_SEND -> conn.send(ack(str(frame, "clientMsgId"), "srv-1", 1));
                default -> {
                }
            }
        })) {
            RecordingListener listener = new RecordingListener();
            try (ImClient client = new ImClient(fastConfig(gateway.port()), listener)) {
                client.connect();
                assertTrue(listener.connected.await(5, TimeUnit.SECONDS), "应在 5s 内收到 connected 帧");
                assertTrue(listener.connectedTimes.get() == 1, "只应该连上一次");

                String clientMsgId = client.sendText("bob", "hi");
                assertTrue(listener.acked.await(5, TimeUnit.SECONDS), "应在 5s 内收到 msg.ack");

                LocalMessage row = client.table().find(clientMsgId).orElseThrow();
                assertEquals(LocalMessage.Status.SENT, row.status());
                assertEquals("srv-1", row.serverMsgId());
                assertEquals(1, row.seq());
                assertEquals(1, row.attempts(), "成功发出一次就只应记一次尝试");

                assertTrue(types(received).contains(Frames.T_SYNC),
                        "连上后必须上报水位：它是服务端抬高 seq 下界、补发缺口的唯一依据");
            }
        }
    }

    @Test
    @DisplayName("ack 丢失后重连重投：同一个 clientMsgId 会重复上行，但最终状态仍是一条 SENT")
    void resendsUnackedMessageAfterReconnect() throws Exception {
        List<String> sentIds = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger connections = new AtomicInteger();

        try (StubGateway gateway = new StubGateway((conn, frame) -> {
            switch (typeOf(frame)) {
                case Frames.T_CONNECT -> {
                    connections.incrementAndGet();
                    conn.send(connected("alice"));
                }
                case Frames.T_SEND -> {
                    String clientMsgId = str(frame, "clientMsgId");
                    sentIds.add(clientMsgId);
                    if (sentIds.size() == 1) {
                        // 服务端收到了，但 ack 丢在路上（或服务端在处理完 ack 前崩溃）
                        conn.close();
                    } else {
                        conn.send(ack(clientMsgId, "srv-1", 1));
                    }
                }
                default -> {
                }
            }
        })) {
            RecordingListener listener = new RecordingListener();
            try (ImClient client = new ImClient(fastConfig(gateway.port()), listener)) {
                client.connect();
                assertTrue(listener.connected.await(5, TimeUnit.SECONDS));

                String clientMsgId = client.sendText("bob", "hi");
                assertTrue(listener.acked.await(10, TimeUnit.SECONDS), "重连后应重投并拿到 ack");

                assertTrue(connections.get() >= 2, "断开后必须自动重连，实际连接次数 " + connections.get());
                assertTrue(sentIds.size() >= 2, "未收到 ack 的消息必须重投");
                assertEquals(List.copyOf(Collections.nCopies(sentIds.size(), clientMsgId)), sentIds,
                        "重投必须复用同一个幂等键，否则服务端会存成两条消息");

                LocalMessage row = client.table().find(clientMsgId).orElseThrow();
                assertEquals(LocalMessage.Status.SENT, row.status());
                assertTrue(row.attempts() >= 2, "重投过一次，attempts 应该 >= 2，实际 " + row.attempts());
            }
        }
    }

    @Test
    @DisplayName("补发重复按 seq 去重、跳号触发 onGap、msg.recv 攒批上报最大水位")
    void dedupesPushesDetectsGapAndReportsRecv() throws Exception {
        CountDownLatch recvFrame = new CountDownLatch(1);
        List<String> recvFrames = Collections.synchronizedList(new ArrayList<>());

        try (StubGateway gateway = new StubGateway((conn, frame) -> {
            switch (typeOf(frame)) {
                case Frames.T_CONNECT -> conn.send(connected("alice"));
                case Frames.T_SYNC -> {
                    conn.send(push(CONV, 1, "第一条"));
                    conn.send(push(CONV, 1, "第一条-补发重复"));
                    conn.send(push(CONV, 3, "第三条（2 丢了）"));
                }
                case Frames.T_RECV -> {
                    recvFrames.add(frame);
                    recvFrame.countDown();
                }
                default -> {
                }
            }
        })) {
            RecordingListener listener = new RecordingListener();
            try (ImClient client = new ImClient(fastConfig(gateway.port()), listener)) {
                client.connect();
                assertTrue(listener.connected.await(5, TimeUnit.SECONDS));
                assertTrue(recvFrame.await(5, TimeUnit.SECONDS), "应上报一次 msg.recv");

                assertEquals(List.of(1L, 3L), listener.pushedSeqs,
                        "重复的 seq=1 必须只回调一次；seq=3 是新的");
                assertEquals(1, listener.gaps.size(), "seq 从 1 跳到 3，应触发一次空洞");
                assertEquals(2, listener.gaps.get(0)[0]);
                assertEquals(3, listener.gaps.get(0)[1]);

                String recv = recvFrames.get(0);
                assertEquals(1, JsonUtils.toJsonNode(recv).path("acks").size(),
                        "recv 按会话聚合：同一个会话多条只报最大 seq");
                assertEquals(CONV, JsonUtils.toJsonNode(recv).path("acks").get(0).path("conversationId").asString());
                assertEquals(3, JsonUtils.toJsonNode(recv).path("acks").get(0).path("seq").asLong());
                assertEquals(0, client.pendingRecvCount(), "上报完应清空攒批缓冲");

                assertEquals(Map.of(CONV, 3L), client.watermarks(),
                        "水位 = 本地表里该会话已收到的最大 seq");
                assertEquals(2, client.table().inbound(CONV, 10).size());
            }
        }
    }

    @Test
    @DisplayName("认证失败(401)不重连：token 无效时反复重连只会把自己打进风控")
    void doesNotReconnectAfterAuthRejection() throws Exception {
        AtomicInteger connections = new AtomicInteger();
        try (StubGateway gateway = new StubGateway((conn, frame) -> {
            if (Frames.T_CONNECT.equals(typeOf(frame))) {
                connections.incrementAndGet();
                conn.send(error(401, "invalid token"));
            }
        })) {
            RecordingListener listener = new RecordingListener();
            try (ImClient client = new ImClient(fastConfig(gateway.port()), listener)) {
                client.connect();
                assertTrue(listener.authRejected.await(5, TimeUnit.SECONDS), "应回调 onAuthRejected");

                // 给「错误地重连」留出足够的时间窗：退避上限 40ms，300ms 里能重连几十次
                Thread.sleep(300);
                assertEquals(1, connections.get(), "401 之后不该再发起连接");
                assertEquals(ImClient.State.CLOSED, client.state());
            }
        }
    }

    // ==== 配置与工具 ====

    private static ImClientConfig fastConfig(int port) {
        return ImClientConfig.builder()
                .host("127.0.0.1")
                .port(port)
                .userId("alice")
                .deviceId("test-device")
                .connectTimeoutMs(2_000)
                .authTimeoutMs(2_000)
                .recvFlushIntervalMs(20)
                .keepAliveIntervalMs(0)
                .outboxScanIntervalMs(20)
                .sendRetryBaseMs(10)
                .sendRetryMaxMs(40)
                // 退避压到毫秒级：测试要的是「会不会重连」，不是「退避曲线好不好看」
                .reconnectPolicy(new ReconnectPolicy(10, 40, 5_000))
                .build();
    }

    private static String typeOf(String frame) {
        return JsonUtils.toJsonNode(frame).path("type").asString("");
    }

    private static String str(String frame, String field) {
        return JsonUtils.toJsonNode(frame).path(field).asString(null);
    }

    private static List<String> types(List<String> frames) {
        List<String> types = new ArrayList<>();
        synchronized (frames) {
            frames.forEach(frame -> types.add(typeOf(frame)));
        }
        return types;
    }

    private static String connected(String userId) {
        return json("ver", 1, "type", Frames.T_CONNECTED, "userId", userId,
                "transport", "tcp", "ts", 1L);
    }

    private static String ack(String clientMsgId, String serverMsgId, long seq) {
        return json("ver", 1, "type", Frames.T_ACK, "clientMsgId", clientMsgId,
                "serverMsgId", serverMsgId, "seq", seq, "code", 0);
    }

    private static String error(int code, String message) {
        return json("ver", 1, "type", Frames.T_ERROR, "code", code, "message", message);
    }

    private static String push(String conversationId, long seq, String body) {
        return json("ver", 1, "type", Frames.T_PUSH, "conversationId", conversationId,
                "convType", "SINGLE", "seq", seq, "serverMsgId", "srv-" + seq,
                "clientMsgId", "c-" + seq, "from", "bob", "to", "alice", "body", body,
                "contentType", "TEXT", "ts", 1_700_000_000_000L);
    }

    /** {@code Map.of} 最多 10 组键值，而下行帧字段更多，所以用 varargs 拼 */
    private static String json(Object... keyValues) {
        Map<String, Object> frame = new java.util.LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            frame.put((String) keyValues[i], keyValues[i + 1]);
        }
        return JsonUtils.toJson(frame);
    }

    /** 记录回调结果的监听器：测试断言的就是「宿主能看到什么」 */
    static final class RecordingListener implements ImClientListener {

        final CountDownLatch connected = new CountDownLatch(1);
        final CountDownLatch acked = new CountDownLatch(1);
        final CountDownLatch authRejected = new CountDownLatch(1);
        final AtomicInteger connectedTimes = new AtomicInteger();
        final List<Long> pushedSeqs = Collections.synchronizedList(new ArrayList<>());
        final List<long[]> gaps = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onConnected() {
            connectedTimes.incrementAndGet();
            connected.countDown();
        }

        @Override
        public void onAck(DownlinkFrame.Ack ack, LocalMessage local) {
            acked.countDown();
        }

        @Override
        public void onMessage(DownlinkFrame.Push push) {
            pushedSeqs.add(push.seq());
        }

        @Override
        public void onGap(String conversationId, long expectedFrom, long actualSeq) {
            gaps.add(new long[]{expectedFrom, actualSeq});
        }

        @Override
        public void onAuthRejected(String message) {
            authRejected.countDown();
        }
    }

    /**
     * 桩网关：真 TCP + 真长度前缀。行为完全由测试给的 {@link Handler} 决定，
     * 这样每个用例要验证的「服务端行为」都写在用例里，不用去翻桩代码。
     */
    static final class StubGateway implements AutoCloseable {

        interface Handler {
            void onFrame(Conn conn, String frame) throws Exception;
        }

        private final ServerSocket serverSocket;
        private final Handler handler;
        private final List<Conn> connections = Collections.synchronizedList(new ArrayList<>());
        private volatile boolean closed;

        StubGateway(Handler handler) throws IOException {
            this.serverSocket = new ServerSocket(0);
            this.handler = handler;
            Thread.ofVirtual().name("stub-gateway-accept").start(this::acceptLoop);
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    Conn conn = new Conn(socket);
                    connections.add(conn);
                    Thread.ofVirtual().name("stub-gateway-conn").start(() -> readLoop(conn));
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void readLoop(Conn conn) {
            try {
                while (!closed) {
                    String frame = LengthPrefixedCodec.read(conn.input(), 1024 * 1024);
                    if (frame == null) {
                        break;
                    }
                    handler.onFrame(conn, frame);
                }
            } catch (Exception ignored) {
                // 桩网关里任何读异常都等价于「这条连接断了」，与真实网关行为一致
            } finally {
                conn.close();
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            serverSocket.close();
            synchronized (connections) {
                connections.forEach(Conn::close);
            }
        }

        static final class Conn {

            private final Socket socket;

            Conn(Socket socket) {
                this.socket = socket;
            }

            java.io.InputStream input() throws IOException {
                return socket.getInputStream();
            }

            synchronized void send(String frame) throws IOException {
                LengthPrefixedCodec.write(socket.getOutputStream(), frame);
            }

            void close() {
                try {
                    socket.close();
                } catch (IOException ignored) {
                    // 关闭失败无需处理
                }
            }
        }
    }
}
