package com.github.im.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * WebSocket 承载的端到端验证：对着一个<b>手写的 RFC 6455 桩网关</b>跑完整链路。
 *
 * <p>为什么值得手写一个 WS 服务端而不是跳过这块测试：
 * 承载层最容易错的地方是「握手 Accept 计算」「客户端帧必须掩码」「升级 query 里的身份」，
 * 这三处任一写错都表现为「偶发连不上」，而它对上层是完全透明的——
 * 没有这层测试，只能靠人工冒烟去撞。
 */
class ImClientWebSocketTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Test
    @DisplayName("WebSocket 建连即认证：不发 connect 帧，能发消息并收到 ack")
    void sendsMessageOverWebSocket() throws Exception {
        List<String> received = Collections.synchronizedList(new ArrayList<>());
        try (StubWebSocketGateway gateway = new StubWebSocketGateway((conn, message) -> {
            received.add(message);
            if (Frames.T_SEND.equals(typeOf(message))) {
                conn.sendText(json("ver", 1, "type", Frames.T_ACK,
                        "clientMsgId", str(message, "clientMsgId"),
                        "serverMsgId", "srv-ws-1", "seq", 1, "code", 0));
            }
        })) {
            Recorder recorder = new Recorder();
            // 必须用桩服务端实际绑定的端口（new ServerSocket(0) 是随机端口）：
            // 硬编码 7003 会连上本机常驻网关，断言拿到的是真实 serverMsgId，测试变成看运气
            try (ImClient client = new ImClient(wsConfig(gateway.port()), recorder)) {
                client.connect();
                assertTrue(recorder.connected.await(5, TimeUnit.SECONDS), "握手成功后应回调 onConnected");

                String clientMsgId = client.sendText("bob", "hi via ws");
                assertTrue(recorder.acked.await(5, TimeUnit.SECONDS), "应收到 msg.ack");

                LocalMessage row = client.table().find(clientMsgId).orElseThrow();
                assertEquals(LocalMessage.Status.SENT, row.status());
                assertEquals("srv-ws-1", row.serverMsgId());

                String requestLine = gateway.requestLine();
                assertTrue(requestLine.contains("userId=alice"), "身份必须在握手 query 里：" + requestLine);
                assertTrue(requestLine.contains("deviceId=test-device"), requestLine);
                assertFalse(received.stream().map(ImClientWebSocketTest::typeOf).toList()
                                .contains(Frames.T_CONNECT),
                        "WS 不该发 connect 帧：身份已在握手声明，多发的 connect 会被当未知 type 回 400");
            }
        }
    }

    @Test
    @DisplayName("服务端 ping 必须立刻回 pong（否则 ~90s 后被判心跳超时断开）")
    void repliesPongToServerPing() throws Exception {
        CountDownLatch pong = new CountDownLatch(1);
        try (StubWebSocketGateway gateway = new StubWebSocketGateway((conn, message) -> {
            if (Frames.T_SYNC.equals(typeOf(message))) {
                conn.sendText(json("ver", 1, "type", Frames.T_PING, "ts", 1L));
            } else if (Frames.T_PONG.equals(typeOf(message))) {
                pong.countDown();
            }
        })) {
            Recorder recorder = new Recorder();
            try (ImClient client = new ImClient(wsConfig(gateway.port()), recorder)) {
                client.connect();
                assertTrue(recorder.connected.await(5, TimeUnit.SECONDS));
                assertTrue(pong.await(5, TimeUnit.SECONDS), "收到 ping 后必须回 pong");
            }
        }
    }

    @Test
    @DisplayName("握手 401 不再重连，并回调 onAuthRejected（重连只会把自己打进风控）")
    void stopsRetryingAfterUnauthorizedHandshake() throws Exception {
        try (StubWebSocketGateway gateway = new StubWebSocketGateway((conn, message) -> {
            // 不会走到这里：握手阶段就被拒
        }, 401)) {
            Recorder recorder = new Recorder();
            try (ImClient client = new ImClient(wsConfig(gateway.port()), recorder)) {
                client.connect();
                assertTrue(recorder.authRejected.await(5, TimeUnit.SECONDS), "应回调 onAuthRejected");

                Thread.sleep(300);
                assertEquals(1, gateway.connections(), "401 之后不该再发起连接");
                assertEquals(ImClient.State.CLOSED, client.state());
            }
        }
    }

    @Test
    @DisplayName("session.kick：回调带 reason，且客户端停机不再重连（否则被踢的设备 0.5s 后自己又回来）")
    void stopsReconnectingAfterKick() throws Exception {
        CountDownLatch kicked = new CountDownLatch(1);
        AtomicReference<DownlinkFrame.SessionKick> captured = new AtomicReference<>();
        try (StubWebSocketGateway gateway = new StubWebSocketGateway((conn, message) -> {
            if (Frames.T_SYNC.equals(typeOf(message))) {
                // 与真实网关同序：先发 session.kick，再关连接
                conn.sendText(json("ver", 1, "type", Frames.T_SESSION_KICK,
                        "deviceId", "test-device", "reason", "另一台设备登录了", "ts", 1L));
                throw new IOException("kicked");
            }
        })) {
            ImClientListener listener = new ImClientListener() {
                @Override
                public void onSessionKick(DownlinkFrame.SessionKick kick) {
                    captured.set(kick);
                    kicked.countDown();
                }
            };
            try (ImClient client = new ImClient(wsConfig(gateway.port()), listener)) {
                client.connect();
                assertTrue(kicked.await(5, TimeUnit.SECONDS), "应回调 onSessionKick");

                Thread.sleep(300);
                assertEquals("另一台设备登录了", captured.get().reason(), "reason 要原样交给宿主展示");
                assertEquals(1, gateway.connections(), "被踢之后不该再发起连接");
                assertEquals(ImClient.State.CLOSED, client.state());
            }
        }
    }

    // ==== 配置与工具 ====

    private static ImClientConfig wsConfig(int port) {
        return ImClientConfig.builder()
                .host("127.0.0.1")
                .port(port)
                .transport(ImClientConfig.Transport.WEBSOCKET)
                // 端口是桩网关随机分配的，路径用网关默认值
                .wsPath("/ws")
                .userId("alice")
                .deviceId("test-device")
                .connectTimeoutMs(2_000)
                .authTimeoutMs(2_000)
                .recvFlushIntervalMs(20)
                .keepAliveIntervalMs(0)
                .outboxScanIntervalMs(20)
                .sendRetryBaseMs(10)
                .sendRetryMaxMs(40)
                .reconnectPolicy(new ReconnectPolicy(10, 40, 5_000))
                .build();
    }

    private static String typeOf(String frame) {
        return MAPPER.readTree(frame).path("type").asString("");
    }

    private static String str(String frame, String field) {
        return MAPPER.readTree(frame).path(field).asString(null);
    }

    private static String json(Object... keyValues) {
        Map<String, Object> frame = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            frame.put((String) keyValues[i], keyValues[i + 1]);
        }
        return MAPPER.writeValueAsString(frame);
    }

    static final class Recorder implements ImClientListener {

        final CountDownLatch connected = new CountDownLatch(1);
        final CountDownLatch acked = new CountDownLatch(1);
        final CountDownLatch authRejected = new CountDownLatch(1);

        @Override
        public void onConnected() {
            connected.countDown();
        }

        @Override
        public void onAck(DownlinkFrame.Ack ack, LocalMessage local) {
            acked.countDown();
        }

        @Override
        public void onAuthRejected(String message) {
            authRejected.countDown();
        }
    }

    /**
     * 最小可用的 RFC 6455 服务端：握手 + 文本帧收发。
     *
     * <p>只实现测试需要的那部分：升级握手、读取带掩码的客户端帧、写出不带掩码的服务端帧、
     * 应答 close/ping 控制帧。真实网关用的是 Netty，这里刻意不复用它的任何代码。
     */
    static final class StubWebSocketGateway implements AutoCloseable {

        private static final String WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        interface Handler {
            void onText(Conn conn, String message) throws Exception;
        }

        private final ServerSocket serverSocket;
        private final Handler handler;
        private final int rejectStatus;
        private final AtomicInteger connections = new AtomicInteger();
        private final List<String> requestLines = Collections.synchronizedList(new ArrayList<>());
        private volatile boolean closed;

        StubWebSocketGateway(Handler handler) throws IOException {
            this(handler, 0);
        }

        StubWebSocketGateway(Handler handler, int rejectStatus) throws IOException {
            this.handler = handler;
            this.rejectStatus = rejectStatus;
            this.serverSocket = new ServerSocket(0);
            Thread.ofVirtual().name("stub-ws-accept").start(this::acceptLoop);
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        int connections() {
            return connections.get();
        }

        String requestLine() {
            return requestLines.isEmpty() ? "" : requestLines.get(0);
        }

        private void acceptLoop() {
            while (!closed) {
                try {
                    Socket socket = serverSocket.accept();
                    connections.incrementAndGet();
                    Thread.ofVirtual().name("stub-ws-conn").start(() -> serve(socket));
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void serve(Socket socket) {
            try (socket) {
                InputStream in = new BufferedInputStream(socket.getInputStream());
                OutputStream out = socket.getOutputStream();
                if (!handshake(in, out)) {
                    return;
                }
                Conn conn = new Conn(out);
                while (true) {
                    String message = readFrame(in, out);
                    if (message == null) {
                        return;
                    }
                    handler.onText(conn, message);
                }
            } catch (Exception ignored) {
                // 桩网关：任何异常都等价于「这条连接断了」
            }
        }

        /** @return true 表示已升级成功；false 表示已按 rejectStatus 拒绝并关闭 */
        private boolean handshake(InputStream in, OutputStream out) throws Exception {
            StringBuilder head = new StringBuilder();
            while (true) {
                int b = in.read();
                if (b < 0) {
                    return false;
                }
                head.append((char) b);
                if (head.length() >= 4 && head.toString().endsWith("\r\n\r\n")) {
                    break;
                }
            }
            String request = head.toString();
            requestLines.add(request.lines().findFirst().orElse(""));
            String key = request.lines()
                    .filter(line -> line.toLowerCase().startsWith("sec-websocket-key:"))
                    .map(line -> line.substring(line.indexOf(':') + 1).trim())
                    .findFirst()
                    .orElse(null);
            if (rejectStatus > 0) {
                byte[] body = "rejected".getBytes(StandardCharsets.UTF_8);
                out.write(("HTTP/1.1 " + rejectStatus + " Rejected\r\nContent-Length: " + body.length
                        + "\r\nConnection: close\r\n\r\n").getBytes(StandardCharsets.UTF_8));
                out.write(body);
                out.flush();
                return false;
            }
            String accept = Base64.getEncoder().encodeToString(
                    MessageDigest.getInstance("SHA-1")
                            .digest((key + WS_GUID).getBytes(StandardCharsets.UTF_8)));
            out.write(("HTTP/1.1 101 Switching Protocols\r\n"
                    + "Upgrade: websocket\r\n"
                    + "Connection: Upgrade\r\n"
                    + "Sec-WebSocket-Accept: " + accept + "\r\n\r\n").getBytes(StandardCharsets.UTF_8));
            out.flush();
            return true;
        }

        /** @return 文本消息；{@code null} 表示连接关闭 */
        private static String readFrame(InputStream in, OutputStream out) throws IOException {
            int b0 = in.read();
            if (b0 < 0) {
                return null;
            }
            int b1 = readByte(in);
            int opcode = b0 & 0x0F;
            boolean masked = (b1 & 0x80) != 0;
            long length = b1 & 0x7F;
            if (length == 126) {
                length = ((long) readByte(in) << 8) | readByte(in);
            } else if (length == 127) {
                length = 0;
                for (int i = 0; i < 8; i++) {
                    length = (length << 8) | readByte(in);
                }
            }
            byte[] mask = new byte[4];
            if (masked) {
                readFully(in, mask);
            }
            byte[] payload = new byte[(int) length];
            readFully(in, payload);
            if (masked) {
                for (int i = 0; i < payload.length; i++) {
                    payload[i] ^= mask[i % 4];
                }
            }
            if (opcode == 0x8) {
                return null;
            }
            if (opcode == 0x9) {
                writeFrame(out, 0xA, payload);
                return "";
            }
            return new String(payload, StandardCharsets.UTF_8);
        }

        private static void writeFrame(OutputStream out, int opcode, byte[] payload) throws IOException {
            synchronized (out) {
                out.write(0x80 | opcode);
                int length = payload.length;
                if (length < 126) {
                    out.write(length);
                } else if (length < 65_536) {
                    out.write(126);
                    out.write(length >>> 8);
                    out.write(length & 0xFF);
                } else {
                    out.write(127);
                    for (int i = 7; i >= 0; i--) {
                        out.write((int) (((long) length) >>> (8 * i)) & 0xFF);
                    }
                }
                out.write(payload);
                out.flush();
            }
        }

        private static int readByte(InputStream in) throws IOException {
            int b = in.read();
            if (b < 0) {
                throw new IOException("连接已关闭");
            }
            return b;
        }

        private static void readFully(InputStream in, byte[] buffer) throws IOException {
            int read = 0;
            while (read < buffer.length) {
                int n = in.read(buffer, read, buffer.length - read);
                if (n < 0) {
                    throw new IOException("连接已关闭");
                }
                read += n;
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            serverSocket.close();
        }

        static final class Conn {

            private final OutputStream out;

            Conn(OutputStream out) {
                this.out = out;
            }

            void sendText(String message) throws IOException {
                writeFrame(out, 0x1, message.getBytes(StandardCharsets.UTF_8));
            }
        }
    }
}
