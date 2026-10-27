package com.github.im.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * <b>TCP 调用 SDK 的完整示例</b>：从建连认证到关闭，一条消息走完「上行 → 受理 → 下发 → 已读回执」。
 *
 * <h2>这个示例演示了什么</h2>
 *
 * <ol>
 *   <li>怎么配 {@link ImClientConfig}（TCP 承载、身份、超时、心跳、攒批间隔）；</li>
 *   <li>怎么接 {@link ImClientListener}（宿主只关心 {@code onConnected/onMessage/onAck/onState}）；</li>
 *   <li>怎么发消息并拿幂等键去本地表查状态（{@link ImClient#sendText} → {@link LocalMessage.Status#SENT}）；</li>
 *   <li>收到的消息长什么样（{@link DownlinkFrame.Push}），以及它如何自动进入本地表与水位；</li>
 *   <li>已读回执（{@link ImClient#markRead} → 发送方 {@link DownlinkFrame.State}）；</li>
 *   <li>SDK 自己做的两件后台事：{@code msg.recv} 攒批上报、心跳 {@code ping/pong}。</li>
 * </ol>
 *
 * <h2>为什么自带一个「最小 IM 服务端」</h2>
 *
 * <p>示例要能在<b>没有任何外部依赖</b>的情况下跑起来（本机、CI、别人刚 clone 下来都行），
 * 所以用一个只认 5 种帧的 {@link MiniImServer} 顶替网关 + IM 服务：真 TCP、真 4 字节长度前缀、
 * 真 JSON。它不实现群聊/撤回/离线补发——那是服务端的事，不是「怎么用 SDK」的一部分。
 *
 * <h2>两种跑法</h2>
 *
 * <table border="1">
 *   <caption>场景与开关</caption>
 *   <tr><th>场景</th><th>怎么跑</th><th>断言范围</th></tr>
 *   <tr>
 *     <td>{@link #fullTcpConversation()}</td>
 *     <td>默认（什么都不传）</td>
 *     <td>全链路：含「服务端确实收到了 msg.recv / ping」，因为服务端是自带的，能观察它</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #realServerRoundTrip()}</td>
 *     <td>{@code -Dim.tcp.port=7004}（可选 {@code im.tcp.host/im.tcp.to/im.tcp.user/im.tcp.token}）</td>
 *     <td>只断言客户端可见的东西：认证成功、服务端受理、本地表 SENT。服务端内部状态看不到了，
 *         而且 seq 由真实服务端持久发号（不等于 1），所以不能照搬场景一的断言</td>
 *   </tr>
 * </table>
 *
 * <p><b>不要把场景一的端口改成真实服务端口</b>：它连的是 {@code new ServerSocket(0)} 随机分配的桩服务端，
 * 改了之后 {@code seq == 1}、{@code server.awaitRecv()} 这些断言会以「和连接毫无关系」的方式失败。
 * 想连真实服务，用场景二的系统属性。
 *
 * <p><b>注意 TCP 端口不是 HTTP 端口</b>：单体 {@code im-app} 是 {@code 9092}，
 * 拆分形态是网关的 {@code im.gateway.transport.tcp-port}（本机 IntelliJ 里起的 gateway-boot 常用 {@code 7004}）。
 * 打到 {@code 8080/8088}（HTTP）只会连上就被断开。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class TcpSdkExampleTest {

    /** 等事件的兜底超时：本地回环上的握手/推送都在毫秒级，5s 只用于兜住慢机器 */
    private static final long WAIT_SECONDS = 5;

    private static final String CONV = "s:alice:bob";

    private MiniImServer server;
    private final List<ImClient> clients = new ArrayList<>();

    @AfterEach
    void tearDown() throws IOException {
        // 顺序无关：client.close() 会断 socket，server.close() 会关监听与残留连接
        clients.forEach(ImClient::close);
        clients.clear();
        if (server != null) {
            server.close();
        }
    }

    @Test
    @DisplayName("场景一（自带桩服务端）：认证 → 发消息 → 对端收到 → 本地表/水位 → 已读回执 → recv/心跳 → 关闭")
    void fullTcpConversation() throws Exception {
        server = new MiniImServer();
        Recorder alice = new Recorder();
        Recorder bob = new Recorder();

        // 1. 两个真实客户端：不同 userId + 不同 deviceId（同一 deviceId 重连会顶掉旧连接）
        ImClient aliceClient = stubClient("alice", "alice-laptop", alice);
        ImClient bobClient = stubClient("bob", "bob-phone", bob);

        // 防呆：本场景的断言只对自带桩服务端成立。连真实服务请用场景二
        // （-Dim.tcp.port=7004），否则会在 seq==1 或 server.awaitRecv() 上莫名其妙地红
        assertEquals(server.port(), aliceClient.config().port(),
                "场景一必须连自带桩服务端；要连真实服务请跑 realServerRoundTrip（-Dim.tcp.port=...）");

        // 2. 建连：connect() 是异步的，认证成功才算连上（TCP 要等 connected 帧）
        assertEquals(ImClient.State.IDLE, aliceClient.state(), "connect() 之前是 IDLE");
        aliceClient.connect();
        bobClient.connect();
        assertTrue(alice.connected.await(WAIT_SECONDS, TimeUnit.SECONDS), "alice 应在 5s 内认证成功");
        assertTrue(bob.connected.await(WAIT_SECONDS, TimeUnit.SECONDS), "bob 应在 5s 内认证成功");
        assertEquals(ImClient.State.CONNECTED, aliceClient.state());

        // 3. 上行：先写本地消息表再上网（所以断网也不会丢），返回值就是幂等键 clientMsgId
        String clientMsgId = aliceClient.sendText("bob", "在吗");
        assertFalse(clientMsgId.isBlank(), "sendText 返回的是幂等键，重投时要用同一个");

        // 4. 下发：bob 收到 Push（实时推送、离线补发、历史拉取都是这个结构）
        assertTrue(bob.message.await(WAIT_SECONDS, TimeUnit.SECONDS), "bob 应在 5s 内收到 msg.push");
        DownlinkFrame.Push received = bob.messages.get(0);
        assertEquals(CONV, received.conversationId(), "单聊会话 ID 由服务端按 (from,to) 推导");
        assertEquals("alice", received.from());
        assertEquals("bob", received.to());
        assertEquals("在吗", received.body());
        assertEquals(1, received.seq(), "会话内 seq 从 1 开始递增");

        // 5. 受理回执：ack 到达后本地表这一行从 PENDING 前进到 SENT
        assertTrue(alice.acked.await(WAIT_SECONDS, TimeUnit.SECONDS), "alice 应收到 msg.ack");
        LocalMessage row = aliceClient.table().find(clientMsgId).orElseThrow();
        assertEquals(LocalMessage.Status.SENT, row.status());
        assertEquals(received.seq(), row.seq(), "本地表和服务端下发的是同一个 seq");
        assertTrue(row.attempts() >= 1, "至少记过一次尝试（ack 慢于重投间隔时会多记，属正常）");

        // 6. 收端同样落表：水位 = 该会话已收到的最大 seq，重连时会被 sync 上报
        assertEquals(Map.of(CONV, 1L), bobClient.watermarks());
        assertEquals(1, bobClient.table().inbound(CONV, 10).size());

        // 7. 已读：只上报「我见到 seq=N」，服务端转成 msg.state{READ} 推给发送方
        assertTrue(bobClient.markRead(CONV, received.seq()), "已读要在连接状态下才发得出去");
        assertTrue(alice.state.await(WAIT_SECONDS, TimeUnit.SECONDS), "alice 应收到 msg.state");
        DownlinkFrame.State state = alice.states.get(0);
        assertEquals("READ", state.state());
        assertEquals("bob", state.userId(), "回执带的是「谁读了」");
        assertEquals(received.seq(), state.seq());

        // 8. 后台行为之一：收到消息后 SDK 自动攒批上报 msg.recv（批量，不是每条一帧）
        assertTrue(server.awaitRecv(WAIT_SECONDS), "SDK 应自动上报 msg.recv");
        assertTrue(server.recvFrames().get(0).contains(CONV), "msg.recv 里带的是「会话 → 已见 seq」");
        assertEquals(0, bobClient.pendingRecvCount(), "上报完攒批缓冲应清空");

        // 9. 后台行为之二：keepAlive 到点自动 ping（本例 200ms，线上默认 30s）
        assertTrue(server.awaitPing(WAIT_SECONDS), "SDK 应自动发心跳");
        assertTrue(server.pingCount() > 0);

        // 10. 关闭：幂等，且不会触发重连
        aliceClient.close();
        assertEquals(ImClient.State.CLOSED, aliceClient.state());
        assertTrue(alice.disconnects.isEmpty(), "正常关闭不算断线，不该回调 onDisconnected");
    }

    // ==== 场景二：对着真实服务打一发 ====

    /**
     * 真实服务的冒烟：只验证「能连上、能发出去、服务端受理了」。
     *
     * <p>默认<b>不跑</b>（没有 {@code -Dim.tcp.port} 就跳过），因为它依赖一个外部服务；
     * 也因此不能断言「对端收到」「服务端收到了 msg.recv」——那需要一个在线的第二个客户端，
     * 而且服务端内部统计对客户端不可见。
     *
     * <p>本机常用：
     * <pre>{@code
     * ./mvnw.cmd -pl im-client test -Dtest=TcpSdkExampleTest#realServerRoundTrip -Dim.tcp.port=7004
     * }</pre>
     * 注意 seq 由真实服务端持久发号，<b>不会</b>从 1 开始，所以这里只断言 {@code seq > 0}。
     */
    @Test
    @DisplayName("场景二（真实服务，默认跳过）：连上 → 发一条 → 服务端受理")
    @EnabledIfSystemProperty(named = "im.tcp.port", matches = "\\d+")
    void realServerRoundTrip() throws Exception {
        int port = Integer.getInteger("im.tcp.port");
        String host = System.getProperty("im.tcp.host", "127.0.0.1");
        String to = System.getProperty("im.tcp.to", "bob");
        // 用随机 userId 避免和你正在调试的实例撞槽位（同 userId 再连会顶掉旧连接）
        String userId = System.getProperty("im.tcp.user", "sdk-smoke-" + System.nanoTime());
        String token = System.getProperty("im.tcp.token", "demo-token");

        Recorder recorder = new Recorder();
        ImClient client = newClient(host, port, userId, "sdk-smoke", token,
                100, 30_000, recorder);
        client.connect();
        assertTrue(recorder.connected.await(WAIT_SECONDS, TimeUnit.SECONDS),
                "应在 5s 内认证成功（" + host + ":" + port + "），错误：" + recorder.describe());

        String clientMsgId = client.send(OutgoingMessage.text(to, "SDK TCP 冒烟 " + System.currentTimeMillis()));
        assertTrue(recorder.acked.await(WAIT_SECONDS, TimeUnit.SECONDS),
                "服务端应受理并回 msg.ack，实际：" + recorder.describe());

        LocalMessage row = client.table().find(clientMsgId).orElseThrow();
        assertEquals(LocalMessage.Status.SENT, row.status(), "受理后本地表应前进到 SENT");
        assertTrue(row.seq() > 0, "真实服务端返回的 seq：" + row.seq());

        client.close();
        assertEquals(ImClient.State.CLOSED, client.state());
    }

    // ==== 怎么创建客户端 ====

    /**
     * 场景一用：连<b>自带桩服务端</b>（端口由 {@code new ServerSocket(0)} 随机分配，不能改成真实服务端口）。
     *
     * <p>想连真实服务请走场景二（{@code -Dim.tcp.port=7004}）——场景一里
     * {@code seq == 1}、{@code server.awaitRecv()}、{@code server.awaitPing()} 这些断言
     * 只对桩服务端成立：真实服务端的 seq 是持久发号的，它的内部状态客户端也看不到。
     *
     * <p>间隔全部压到几十毫秒，是为了让「攒批上报 / 心跳」在测试里也能观察到；
     * 真实环境不要这么配（用默认的 100ms / 30s）。
     */
    private ImClient stubClient(String userId, String deviceId, ImClientListener listener) {
        return newClient("127.0.0.1", server.port(), userId, deviceId, "demo-token",
                50, 200, listener);
    }

    /**
     * 生产代码里这一段通常只写一次并复用（一个进程一个用户连接）。
     *
     * <p>三个容易忽略的点：{@code deviceId} 要稳定（否则服务端当成新设备，占满槽位）、
     * {@code keepAliveIntervalMs} 不能比服务端的空闲探测更短（否则白耗电）、
     * {@code token} 在 {@code im.auth.mode=enforce} 下必须由业务后端下发。
     *
     * @param recvFlushIntervalMs {@code msg.recv} 攒批窗口
     * @param keepAliveIntervalMs 主动心跳间隔，{@code 0} = 只被动应答服务端 ping
     */
    private ImClient newClient(String host, int port, String userId, String deviceId, String token,
                               int recvFlushIntervalMs, long keepAliveIntervalMs,
                               ImClientListener listener) {
        ImClientConfig config = ImClientConfig.builder()
                .host(host)
                .port(port)
                .transport(ImClientConfig.Transport.TCP)
                .userId(userId)
                .deviceId(deviceId)
                .token(token)                 // im.auth.mode=off 时不校验；enforce 时换成真 token
                .connectTimeoutMs(2_000)
                .authTimeoutMs(2_000)
                .recvFlushIntervalMs(recvFlushIntervalMs)
                .keepAliveIntervalMs(keepAliveIntervalMs)
                .outboxScanIntervalMs(50)     // 断线重投扫描间隔
                .build();
        ImClient client = new ImClient(config, listener);
        clients.add(client);
        return client;
    }

    /** 宿主侧回调：只记事件，不做重活（回调跑在 SDK 的读循环/定时器线程上） */
    static final class Recorder implements ImClientListener {

        final CountDownLatch connected = new CountDownLatch(1);
        final CountDownLatch acked = new CountDownLatch(1);
        final CountDownLatch message = new CountDownLatch(1);
        final CountDownLatch state = new CountDownLatch(1);
        final List<DownlinkFrame.Push> messages = Collections.synchronizedList(new ArrayList<>());
        final List<DownlinkFrame.State> states = Collections.synchronizedList(new ArrayList<>());
        final List<DownlinkFrame.Error> errors = Collections.synchronizedList(new ArrayList<>());
        final List<Throwable> disconnects = Collections.synchronizedList(new ArrayList<>());
        final List<String> authRejections = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void onConnected() {
            connected.countDown();
        }

        @Override
        public void onDisconnected(Throwable cause, long retryInMs) {
            // retryInMs < 0 表示不会再重连（例如 401），这里只在被异常断开时才有内容
            disconnects.add(cause);
        }

        @Override
        public void onError(DownlinkFrame.Error error) {
            // 上行被拒（4xx）、服务暂不可用（503）都会先走这里；SDK 已按 code 处理过本地表
            errors.add(error);
        }

        @Override
        public void onAuthRejected(String message) {
            authRejections.add(message);
        }

        /** 断言失败时把「宿主能看到的异常信号」一次打全，省得再猜是真服务没起、token 错还是被拒 */
        String describe() {
            return "errors=" + errors + ", authRejected=" + authRejections + ", disconnects=" + disconnects;
        }

        @Override
        public void onMessage(DownlinkFrame.Push push) {
            // SDK 已按 (会话, seq) 去重，这里每条只会来一次
            messages.add(push);
            message.countDown();
        }

        @Override
        public void onAck(DownlinkFrame.Ack ack, LocalMessage local) {
            // local 是本地表里那一行（已置 SENT）；找不到时为 null
            acked.countDown();
        }

        @Override
        public void onState(DownlinkFrame.State stateFrame) {
            // 参数与字段同名会互相遮蔽，所以参数叫 stateFrame、字段叫 state
            states.add(stateFrame);
            this.state.countDown();
        }
    }

    // ==== 最小 IM 服务端（仅示例用途）====

    /**
     * 只实现这个示例需要的 5 种上行帧：{@code connect}、{@code msg.send}、{@code msg.read}、
     * {@code msg.recv}、{@code ping}。
     *
     * <p>帧格式与真网关一致：{@code 4 字节大端长度 + UTF-8 JSON}。真服务端还会做鉴权、
     * 群聊扇出、离线补发、撤回广播——那些与「SDK 怎么用」无关，示例里不实现。
     */
    static final class MiniImServer implements AutoCloseable {

        private static final int MAX_FRAME_BYTES = 1024 * 1024;

        private final ServerSocket serverSocket;
        private final Map<String, Session> sessions = new ConcurrentHashMap<>();
        private final Map<String, AtomicLong> seqs = new ConcurrentHashMap<>();
        private final List<String> recvFrames = Collections.synchronizedList(new ArrayList<>());
        private final AtomicLong pings = new AtomicLong();
        private final CountDownLatch recvLatch = new CountDownLatch(1);
        private final CountDownLatch pingLatch = new CountDownLatch(1);

        MiniImServer() throws IOException {
            this.serverSocket = new ServerSocket(0);
            Thread.ofVirtual().name("mini-im-accept").start(this::acceptLoop);
        }

        int port() {
            return serverSocket.getLocalPort();
        }

        boolean awaitRecv(long seconds) throws InterruptedException {
            return recvLatch.await(seconds, TimeUnit.SECONDS);
        }

        boolean awaitPing(long seconds) throws InterruptedException {
            return pingLatch.await(seconds, TimeUnit.SECONDS);
        }

        long pingCount() {
            return pings.get();
        }

        List<String> recvFrames() {
            return List.copyOf(recvFrames);
        }

        private void acceptLoop() {
            while (!serverSocket.isClosed()) {
                try {
                    Session session = new Session(serverSocket.accept());
                    Thread.ofVirtual().name("mini-im-conn").start(() -> readLoop(session));
                } catch (IOException e) {
                    return;
                }
            }
        }

        private void readLoop(Session session) {
            try {
                while (!session.socket.isClosed()) {
                    String payload = LengthPrefixedCodec.read(session.in, MAX_FRAME_BYTES);
                    if (payload == null) {
                        break;
                    }
                    handle(session, JsonUtils.toJsonNode(payload));
                }
            } catch (IOException ignored) {
                // 任意读异常都等价于「这条连接断了」，与真网关行为一致
            } finally {
                session.close();
                if (session.userId != null) {
                    sessions.remove(session.userId, session);
                }
            }
        }

        private void handle(Session session, JsonNode frame) throws IOException {
            switch (frame.path("type").asString("")) {
                case Frames.T_CONNECT -> onConnect(session, frame);
                case Frames.T_SEND -> onSend(session, frame);
                case Frames.T_READ -> onRead(session, frame);
                case Frames.T_RECV -> {
                    recvFrames.add(JsonUtils.toJson(frame));
                    recvLatch.countDown();
                }
                case Frames.T_PING -> {
                    pings.incrementAndGet();
                    pingLatch.countDown();
                    session.send(json(Frames.T_PONG, "ts", System.currentTimeMillis()));
                }
                // sync：真服务端会按 watermarks 补发缺口；这里没有离线消息，忽略即可
                case Frames.T_SYNC -> {
                }
                default -> {
                }
            }
        }

        private void onConnect(Session session, JsonNode frame) throws IOException {
            // 真服务端还会读 deviceId 来分槽位（同 userId 最多 5 个设备），示例按 userId 一线程一槽
            String userId = frame.path("userId").asString(null);
            if (userId == null || userId.isBlank()) {
                // 真网关缺 userId 直接回 400 并断开：认证失败不该让连接继续挂着
                session.send(json(Frames.T_ERROR, "code", 400, "message", "userId required"));
                session.close();
                return;
            }
            session.userId = userId;
            Session previous = sessions.put(session.userId, session);
            if (previous != null && previous != session) {
                // 同一 userId 再连一次：踢掉旧连接（真服务端按 userId+deviceId 分槽位，最多 5 个）
                previous.close();
            }
            session.send(json(Frames.T_CONNECTED, "userId", session.userId,
                    "transport", "tcp", "ts", System.currentTimeMillis()));
        }

        private void onSend(Session sender, JsonNode frame) throws IOException {
            String clientMsgId = frame.path("clientMsgId").asString(null);
            String to = frame.path("to").asString(null);
            String conversationId = frame.path("conversationId").asString(null);
            if (conversationId == null || conversationId.isBlank()) {
                conversationId = single(sender.userId, to);
            }
            long seq = seqs.computeIfAbsent(conversationId, key -> new AtomicLong()).incrementAndGet();
            String serverMsgId = conversationId + '#' + seq;

            // 先回 ack（受理 ≠ 送达），再把消息推给在线对端
            sender.send(json(Frames.T_ACK, "clientMsgId", clientMsgId,
                    "serverMsgId", serverMsgId, "seq", seq, "code", 0));
            Session target = to == null ? null : sessions.get(to);
            if (target != null) {
                target.send(json(Frames.T_PUSH, "conversationId", conversationId,
                        "convType", OutgoingMessage.CONV_SINGLE, "seq", seq, "serverMsgId", serverMsgId,
                        "clientMsgId", clientMsgId, "from", sender.userId, "to", to,
                        "body", frame.path("body").asString(""),
                        "contentType", frame.path("contentType").asString(OutgoingMessage.CT_TEXT),
                        "recalled", false, "ts", System.currentTimeMillis()));
            }
        }

        private void onRead(Session reader, JsonNode frame) throws IOException {
            String conversationId = frame.path("conversationId").asString("");
            long seq = frame.path("seq").asLong(0);
            String peer = peerOf(conversationId, reader.userId);
            Session sender = peer == null ? null : sessions.get(peer);
            if (sender != null) {
                sender.send(json(Frames.T_STATE, "conversationId", conversationId,
                        "convType", OutgoingMessage.CONV_SINGLE, "state", "READ", "seq", seq,
                        "userId", reader.userId, "ts", System.currentTimeMillis()));
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
            sessions.values().forEach(Session::close);
            sessions.clear();
        }

        /** 单聊会话 ID：{@code s:{小}:{大}}，两端算出来必须是同一个 */
        private static String single(String a, String b) {
            return a.compareTo(b) <= 0 ? "s:" + a + ':' + b : "s:" + b + ':' + a;
        }

        private static String peerOf(String conversationId, String self) {
            String[] parts = conversationId.split(":");
            if (parts.length != 3) {
                return null;
            }
            return parts[1].equals(self) ? parts[2] : parts[1];
        }

        /** {@code Map.of} 最多 10 组，而下行帧字段更多，所以用 varargs 拼 */
        private static String json(String type, Object... keyValues) {
            Map<String, Object> frame = new LinkedHashMap<>();
            frame.put("ver", Frames.VER);
            frame.put("type", type);
            for (int i = 0; i + 1 < keyValues.length; i += 2) {
                frame.put((String) keyValues[i], keyValues[i + 1]);
            }
            return JsonUtils.toJson(frame);
        }

        /** 一条连接：读写都加锁，因为「别的连接」的读线程也会往我这写帧 */
        static final class Session {

            private final Socket socket;
            private final InputStream in;
            private final OutputStream out;
            private volatile String userId;

            Session(Socket socket) throws IOException {
                this.socket = socket;
                this.in = socket.getInputStream();
                this.out = socket.getOutputStream();
            }

            synchronized void send(String payload) throws IOException {
                LengthPrefixedCodec.write(out, payload);
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
