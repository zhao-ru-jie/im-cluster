package com.github.im.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * IM 客户端 SDK 门面：连接维护 + 断线退避重连 + 本地消息表驱动的可靠上下行。
 *
 * <h2>一条消息的完整生命周期</h2>
 *
 * <pre>
 * send() ──► 写本地消息表(PENDING) ──► 写 socket ──► 服务端受理
 *                                            │             │
 *                                      ack 未到/写失败      └─► msg.ack ─► 表行置 SENT ─► onAck
 *                                            │
 *                                    记一次 attempt + 推后 nextAttemptAt
 *                                            │
 *                    断线 → 重连成功 → flushOutbox() 用<b>同一个 clientMsgId</b> 重投
 *                                            │
 *                                   服务端幂等命中 → 返回同一 serverMsgId/seq（不会变成两条）
 * </pre>
 *
 * <h2>为什么「先落表、后发送」不能反过来</h2>
 *
 * <p>反过来的话，端到端只有一段窗口（写 socket 成功、写表之前进程被杀）会丢消息，
 * 而这段窗口在移动端非常常见——用户点了发送就切后台、系统立刻冻结进程。
 * 落表在前，最坏情况是「表里有、但还没发出去」，那是可恢复的（重投）。
 *
 * <h2>重连的三种触发源，三种处理</h2>
 *
 * <ul>
 *   <li><b>读写失败 / 对端关闭</b>：按 {@link ReconnectPolicy} 指数退避；</li>
 *   <li><b>{@code session.migrate}</b>（摘流、缩容、发版）：按服务端 {@code retryAfterMs}
 *       ＋ 客户端抖动，<b>不计入</b>失败次数——这不是故障；</li>
 *   <li><b>{@code error 401}</b>：不重连。token 无效时重连只会把自己打进风控，
 *       交给宿主重新取 token（{@link ImClientListener#onAuthRejected}）。</li>
 * </ul>
 *
 * <p>{@code session.kick} 也走「不重连」：连接是被服务端主动关掉的，退避重连只会被再踢一次
 * （见 {@link ImClientListener#onSessionKick}）。
 *
 * <h2>线程模型</h2>
 *
 * <p>一条虚拟线程读 socket、一个 2 线程的调度器跑定时任务（保活、重投扫描、recv 攒批）＋
 * 自己重连。所有状态都在 {@code lock} 下改，回调一律在<b>锁外</b>触发——
 * 否则宿主在回调里调 {@code send()} 会自己把自己锁死。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class ImClient implements AutoCloseable {

    private static final System.Logger log = System.getLogger(ImClient.class.getName());

    /** 每个会话保留多少个「本端发出的 seq」：只用于空洞判定，不需要全量 */
    private static final int OWN_SEQ_KEEP = 1_024;
    /** 空洞跨度超过这个数就不再逐个排除（seq 被服务端重置过时区间可能非常大） */
    private static final long GAP_SCAN_LIMIT = 1_024;

    /** 客户端状态：{@code CONNECTING} 覆盖「正在建连」和「已连上但还没收到 connected 帧」 */
    public enum State {
        IDLE, CONNECTING, CONNECTED, CLOSED
    }

    private final ImClientConfig config;
    private final Frames frames;
    private final LocalMessageTable table;
    private final ImClientListener listener;
    private final ReconnectPolicy reconnectPolicy;
    private final Backoff sendBackoff;
    private final ScheduledExecutorService scheduler;

    private final Object lock = new Object();
    /**
     * 待上报的 {@code msg.recv}：会话 → 已见最大 seq。
     *
     * <p>键是会话而不是消息：协议的语义就是「我见到 seq=N」，
     * 同一个会话多条只留最大值，天然把批量压到「会话数」量级。
     */
    private final Map<String, Long> pendingRecv = new LinkedHashMap<>();

    /**
     * 会话 → 本端发出、且服务端已受理的 seq（数据来源是 {@code msg.ack}）。
     *
     * <p>为什么需要它：会话的 seq 是<b>会话级共享</b>的发号器，本端发出的消息同样占用序号。
     * 如果只拿「已收到」的水位判空洞，那么双方都说话时，「中间缺的那几个号」其实都是自己发的，
     * 却会被判成丢消息——于是每收一条就回调一次 {@link ImClientListener#onGap}，
     * 宿主照着提示反复拉历史，纯属白干。
     *
     * <p>只保留最近的 {@link #OWN_SEQ_KEEP} 个：空洞判定只关心水位之后的区间，老的不必留着。
     */
    private final Map<String, NavigableSet<Long>> ownSeqs = new HashMap<>();

    private State state = State.IDLE;
    private FrameChannel connection;
    /** 连续失败次数（稳定连接后归零），只影响退避档位 */
    private int attempt;
    private long connectedAt;
    /** >=0 表示下一次重连要用服务端给的 retryAfterMs（摘流） */
    private long retryAfterHintMs = -1;
    private boolean closed;

    private ScheduledFuture<?> reconnectTask;
    private ScheduledFuture<?> authTask;
    private ScheduledFuture<?> outboxTask;
    private ScheduledFuture<?> recvTask;
    private ScheduledFuture<?> pingTask;

    /** 内存表版本：适合测试与「不需要跨进程重启保留」的场景 */
    public ImClient(ImClientConfig config, ImClientListener listener) {
        this(config, new InMemoryLocalMessageTable(), listener);
    }

    public ImClient(ImClientConfig config, LocalMessageTable table, ImClientListener listener) {
        this.config = Objects.requireNonNull(config, "config");
        this.table = table == null ? new InMemoryLocalMessageTable() : table;
        this.listener = listener == null ? new ImClientListener() {
        } : listener;
        this.frames = new Frames();
        this.reconnectPolicy = config.reconnectPolicy();
        this.sendBackoff = new Backoff(config.sendRetryBaseMs(), config.sendRetryMaxMs());
        // 虚拟线程做定时任务的执行体：任务里会调用宿主回调，可能阻塞在宿主的锁上，
        // 用平台线程池会把「宿主卡住」变成「SDK 的心跳也停了」
        this.scheduler = Executors.newScheduledThreadPool(2,
                Thread.ofVirtual().name("im-client-timer-", 0).factory());
    }

    // ==== 连接管理 ====

    /**
     * 发起连接（异步）。已连接/正在连接时是空操作，因此「网络恢复」时重复调用是安全的。
     *
     * @throws IllegalStateException 客户端已 {@link #close()}
     */
    public void connect() {
        synchronized (lock) {
            ensureOpen();
            if (state == State.CONNECTED || state == State.CONNECTING) {
                return;
            }
            state = State.CONNECTING;
        }
        scheduler.execute(this::connectNow);
    }

    public State state() {
        synchronized (lock) {
            return state;
        }
    }

    /** 关闭客户端：停止重连、断开连接、关闭本地消息表。幂等。 */
    @Override
    public void close() {
        shutdown("客户端关闭");
    }

    private void connectNow() {
        if (isClosed()) {
            return;
        }
        FrameChannel channel;
        try {
            channel = openChannel();
        } catch (IOException | RuntimeException e) {
            // 建连/握手失败（拒绝连接、DNS、超时、WS 握手被拒）：走同一条路径，
            // 由 handleDisconnect 区分「该退避」与「重连没用」
            handleDisconnect(null, e);
            return;
        }
        synchronized (lock) {
            if (closed) {
                channel.close();
                return;
            }
            connection = channel;
        }
        if (config.transport() == ImClientConfig.Transport.TCP) {
            try {
                // 首帧必须是 connect：TCP 没有 HTTP 握手，身份只能在这一帧里声明。
                // 网关的认证窗口默认 10s，超时或首帧不是 connect 会回 error 401 并断开。
                channel.send(frames.connect(config.userId(), config.deviceId(), config.token()));
            } catch (IOException e) {
                // 读循环会感知到关闭并回调 handleDisconnect，这里不重复处理（否则会重连两次）
                channel.close();
                return;
            }
            startAuthTimeout(channel);
        }
        // WebSocket：身份在握手 query 里，握手成功后由 onReady → handleAuthenticated 接手
    }

    private FrameChannel openChannel() throws IOException {
        FrameChannel.Listener channelListener = new FrameChannel.Listener() {
            @Override
            public void onReady(FrameChannel channel) {
                // 只有 WebSocket 会走这里：握手成功即认证成功
                handleAuthenticated(channel);
            }

            @Override
            public void onFrame(FrameChannel channel, String payload) {
                handleFrame(channel, payload);
            }

            @Override
            public void onClosed(FrameChannel channel, Throwable cause) {
                handleDisconnect(channel, cause);
            }
        };
        if (config.transport() == ImClientConfig.Transport.WEBSOCKET) {
            return WebSocketFrameChannel.open(WebSocketFrameChannel.uri(
                            config.host(), config.port(), config.wsTls(), config.wsPath(),
                            config.userId(), config.deviceId(), config.token()),
                    config.connectTimeoutMs(), channelListener);
        }
        return TcpFrameChannel.open(config.host(), config.port(), config.connectTimeoutMs(),
                config.maxFrameBytes(), channelListener);
    }

    private void startAuthTimeout(FrameChannel conn) {
        synchronized (lock) {
            cancel(authTask);
            authTask = scheduler.schedule(() -> {
                if (state != State.CONNECTED) {
                    log.log(System.Logger.Level.DEBUG,
                            "[client] {0}ms 内未收到 connected 帧，断开重连", config.authTimeoutMs());
                    conn.close();
                }
            }, config.authTimeoutMs(), TimeUnit.MILLISECONDS);
        }
    }

    private void handleConnected(FrameChannel conn, DownlinkFrame.Connected frame) {
        log.log(System.Logger.Level.DEBUG, "[client] 收到 connected 帧 transport={0} remote={1}",
                frame.transport(), conn.remote());
        handleAuthenticated(conn);
    }

    /**
     * 通道已认证：TCP 是收到 {@code connected} 帧，WebSocket 是握手成功。
     *
     * <p>两种承载在这里汇成一条路：置 CONNECTED → 起定时器 → 报水位 → 重投积压。
     * 之后的业务逻辑（去重、水位、重投、退避）完全不知道承载是哪种。
     */
    private void handleAuthenticated(FrameChannel conn) {
        synchronized (lock) {
            if (connection != conn) {
                return;
            }
            if (state == State.CONNECTED) {
                // WS 的 onReady 只在握手成功时来一次；这里仍然做幂等保护：
                // 重复置位会重复起定时器、重复上报水位
                return;
            }
            state = State.CONNECTED;
            connectedAt = System.currentTimeMillis();
            cancel(authTask);
            authTask = null;
        }
        log.log(System.Logger.Level.INFO, "[client] 已连接 user={0} device={1} transport={2} remote={3}",
                config.userId(), config.deviceId(), config.transport(), conn.remote());
        startTimers();
        notifyListener(listener::onConnected);
        // 重连对账：先报水位——服务端据此把 seq 下界抬到水位之上，并补发缺口
        sendSync();
        // 断线期间积压的待发消息立刻重投（clientMsgId 不变 → 服务端幂等，不会变成两条）
        flushOutbox();
    }

    private void handleDisconnect(FrameChannel conn, Throwable cause) {
        if (cause instanceof WebSocketHandshakeException handshake && !retryableHandshake(handshake)) {
            handleRejectedHandshake(conn, handshake);
            return;
        }
        long delayMs;
        int nextAttempt;
        long durationMs;
        synchronized (lock) {
            if (closed) {
                return;
            }
            // 旧连接的回调（例如已经被替换掉的连接这才收到 EOF）必须忽略，
            // 否则一次「重连成功后旧连接关闭」会把新连接一起判死，形成重连循环
            if (conn != null && connection != conn) {
                return;
            }
            cancel(authTask);
            authTask = null;
            cancel(outboxTask);
            outboxTask = null;
            cancel(recvTask);
            recvTask = null;
            cancel(pingTask);
            pingTask = null;
            connection = null;

            durationMs = connectedAt == 0 ? 0 : System.currentTimeMillis() - connectedAt;
            connectedAt = 0;
            if (retryAfterHintMs >= 0) {
                // 摘流/缩容：按服务端给的时间 + 自己的抖动，不计入失败次数
                delayMs = reconnectPolicy.delayAfterMigrate(retryAfterHintMs);
                retryAfterHintMs = -1;
            } else {
                // 活够久的连接说明链路是通的（不是被反复秒踢），计数归零重新从 0.5s 起退
                attempt = reconnectPolicy.isStable(durationMs) ? 0 : attempt + 1;
                delayMs = reconnectPolicy.delayMs(attempt);
            }
            nextAttempt = attempt;
            state = State.CONNECTING;
            cancel(reconnectTask);
            reconnectTask = scheduler.schedule(this::connectNow, delayMs, TimeUnit.MILLISECONDS);
        }
        log.log(System.Logger.Level.INFO,
                "[client] 连接断开（{0}），{1}ms 后重连（第 {2} 次）",
                cause == null ? "未知原因" : cause.getMessage(), delayMs, nextAttempt);
        long retryInMs = delayMs;
        notifyListener(() -> listener.onDisconnected(cause, retryInMs));
    }

    /** 503 是「实例在摘流，等会再来」，其余握手拒绝（400/401/404）重连多少次都一样 */
    private static boolean retryableHandshake(WebSocketHandshakeException handshake) {
        return handshake.status() >= 500;
    }

    /**
     * 握手被永久拒绝：不停在这里重试，而是收摊并如实告诉宿主。
     *
     * <p>{@code 401} 走 {@link ImClientListener#onAuthRejected}（宿主该去换 token），
     * 其余走 {@link ImClientListener#onDisconnected} 且 {@code retryInMs = -1}（明确表示不再重连）。
     * 对配置错误（缺 userId、路径写错）无限重连，只会让日志里全是同一行 400。
     */
    private void handleRejectedHandshake(FrameChannel conn, WebSocketHandshakeException handshake) {
        synchronized (lock) {
            if (closed) {
                return;
            }
            if (conn != null && connection != conn) {
                return;
            }
            connection = null;
            cancel(authTask);
            authTask = null;
            cancel(outboxTask);
            outboxTask = null;
            cancel(recvTask);
            recvTask = null;
            cancel(pingTask);
            pingTask = null;
            cancel(reconnectTask);
        }
        log.log(System.Logger.Level.WARNING, "[client] 握手被拒（HTTP {0}），不再重连：{1}",
                handshake.status(), handshake.getMessage());
        if (handshake.status() == 401) {
            String message = handshake.getMessage();
            notifyListener(() -> listener.onAuthRejected(message));
        }
        notifyListener(() -> listener.onDisconnected(handshake, -1L));
        shutdown("握手被拒：HTTP " + handshake.status());
    }

    /** 永久停机：不再重连（认证失败、宿主主动关闭） */
    private void shutdown(String reason) {
        FrameChannel conn;
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            state = State.CLOSED;
            conn = connection;
            connection = null;
            cancel(authTask);
            cancel(outboxTask);
            cancel(recvTask);
            cancel(pingTask);
            cancel(reconnectTask);
        }
        log.log(System.Logger.Level.INFO, "[client] 关闭：{0}", reason);
        if (conn != null) {
            conn.close();
        }
        scheduler.shutdownNow();
        table.close();
    }

    // ==== 上行 API ====

    /**
     * 发送一条消息：<b>先写本地消息表，再尝试上网</b>。
     *
     * <p>方法本身不阻塞、不等待 ack，返回的 {@code clientMsgId} 是这条消息的幂等键——
     * 宿主可以拿它去表里查状态（{@link LocalMessageTable#find}）做「发送中/已送达」气泡。
     *
     * @return 本次消息的幂等键（调用方没给时自动生成）
     */
    public String send(OutgoingMessage message) {
        ensureOpen();
        OutgoingMessage normalized = Objects.requireNonNull(message, "message").normalized();
        long now = System.currentTimeMillis();
        LocalMessage row = LocalMessage.outbound(normalized, now);
        // 先落表：反过来的话「写 socket 成功、落表之前进程被杀」就会丢消息
        table.upsert(row);
        if (connectedConnection() != null) {
            trySend(row);
        }
        return normalized.clientMsgId();
    }

    /** 便捷：单聊纯文本 */
    public String sendText(String to, String body) {
        return send(OutgoingMessage.text(to, body));
    }

    /**
     * 上报已读位置。
     *
     * @return {@code false} 表示当前没连上、这一帧没发出去——已读是「只前进」的语义，
     *         宿主下次进会话时重报一次即可，不需要排队
     */
    public boolean markRead(String conversationId, long seq) {
        ensureOpen();
        if (conversationId == null || conversationId.isBlank() || seq <= 0) {
            return false;
        }
        return sendControl(frames.read(conversationId, seq));
    }

    /**
     * 上报「我在输入 / 停止输入」。
     *
     * <p><b>不落本地表、不重投</b>：输入状态只在「此刻」有意义，断线期间攒下的
     * 「他在输入」重连后补发出去只会误导对方。所以这里没连上就直接返回 false。
     *
     * <p>宿主不需要自己做节流：服务端对 {@code typing=true} 有 3 秒去抖。
     * 但 <b>{@code typing=false} 必须发</b>——它是对方那三个点熄灭的唯一信号。
     *
     * @param conversationId 会话 ID（单聊 {@code s:a:b} / 群聊 {@code g:xxx}）
     */
    public boolean sendTyping(String conversationId, boolean typing) {
        ensureOpen();
        if (conversationId == null || conversationId.isBlank()) {
            return false;
        }
        return sendControl(frames.typing(conversationId, null, null, typing));
    }

    /** 单聊简写：只给对端，会话 ID 由服务端按消息路径同一套规则生成 */
    public boolean sendTypingTo(String to, boolean typing) {
        ensureOpen();
        if (to == null || to.isBlank()) {
            return false;
        }
        return sendControl(frames.typing(null, to, null, typing));
    }

    /** 群聊简写：把 groupId 拼成 {@code g:} 前缀的会话 ID（这层约定不该让宿主自己记） */
    public boolean sendGroupTyping(String groupId, boolean typing) {
        ensureOpen();
        if (groupId == null || groupId.isBlank()) {
            return false;
        }
        return sendControl(frames.typing("g:" + groupId, groupId, "GROUP", typing));
    }

    /**
     * 查询一批用户的在线状态；应答经 {@link ImClientListener#onPresenceSnapshot} 回调。
     *
     * @return 本帧是否发出（没连上为 false——状态查询不值得排队）
     */
    public boolean queryPresence(Collection<String> userIds) {
        ensureOpen();
        if (userIds == null || userIds.isEmpty()) {
            return false;
        }
        return sendControl(frames.presenceGet(userIds));
    }

    /** 立即上报一次会话水位（正常由重连自动触发；宿主也可以在切后台前手动调） */
    public void sendSync() {
        sendControl(frames.sync(table.inboundWatermarks()));
    }

    // ==== 本地消息表访问 ====

    /**
     * 待发/发送中的消息（{@code PENDING}，按创建时间升序）。
     *
     * <p>已受理（{@code SENT}）和永久失败（{@code FAILED}）不算「待发」——
     * 这个列表就是宿主画「发送中」气泡和重试按钮的依据，混进已完成的条目会让 UI 一直转圈。
     */
    public List<LocalMessage> pendingOutbound(int limit) {
        return table.outbound(limit).stream()
                .filter(row -> row.status() == LocalMessage.Status.PENDING)
                .toList();
    }

    /** 会话 → 已收到的最大 seq（就是 {@code sync} 上报的内容） */
    public Map<String, Long> watermarks() {
        return table.inboundWatermarks();
    }

    /** 暴露本地表：宿主通常用它做聊天记录分页（{@link LocalMessageTable#inbound}） */
    public LocalMessageTable table() {
        return table;
    }

    public ImClientConfig config() {
        return config;
    }

    /** 尚未上报的 {@code msg.recv} 条数（观测/测试用） */
    public int pendingRecvCount() {
        synchronized (pendingRecv) {
            return pendingRecv.size();
        }
    }

    // ==== 下行分发 ====

    private void handleFrame(FrameChannel conn, String payload) {
        DownlinkFrame frame;
        try {
            frame = frames.parse(payload);
        } catch (RuntimeException e) {
            // 长度前缀对得上、内容却不是 JSON：说明字节流已经错位，只能断开重连
            log.log(System.Logger.Level.WARNING,
                    "[client] 下行帧无法解析，断开重连：" + e.getMessage());
            conn.close();
            return;
        }
        switch (frame) {
            case DownlinkFrame.Connected connected -> handleConnected(conn, connected);
            case DownlinkFrame.Ack ack -> handleAck(ack);
            case DownlinkFrame.Push push -> handlePush(push);
            case DownlinkFrame.State stateFrame -> notifyListener(() -> listener.onState(stateFrame));
            case DownlinkFrame.Update update -> notifyListener(() -> listener.onUpdate(update));
            case DownlinkFrame.GroupUpdate groupUpdate ->
                    notifyListener(() -> listener.onGroupUpdate(groupUpdate));
            // 状态类帧（输入中 / 在线状态）：不写本地表、不参与去重，直接交给宿主渲染
            case DownlinkFrame.Typing typingFrame -> notifyListener(() -> listener.onTyping(typingFrame));
            case DownlinkFrame.Presence presence -> notifyListener(() -> listener.onPresence(presence));
            case DownlinkFrame.PresenceSnapshot snapshot ->
                    notifyListener(() -> listener.onPresenceSnapshot(snapshot));
            // 多端登录 / 会话标记同步：纯通知，不改本地表，也不影响本连接
            case DownlinkFrame.SessionLogin login -> notifyListener(() -> listener.onSessionLogin(login));
            case DownlinkFrame.ConvUpdate convUpdate ->
                    notifyListener(() -> listener.onConvUpdate(convUpdate));
            // 好友申请：纯通知，不改本地表（申请列表由宿主按 REST 拉取，避免两套数据源打架）
            case DownlinkFrame.RelationRequest request ->
                    notifyListener(() -> listener.onRelationRequest(request));
            case DownlinkFrame.RelationUpdate relationUpdate ->
                    notifyListener(() -> listener.onRelationUpdate(relationUpdate));
            case DownlinkFrame.SessionKick kick -> handleKick(kick);
            case DownlinkFrame.Error error -> handleError(error);
            // 服务端探测必须立刻回：网关 90s 收不到任何上行就断开，
            // 而「回 pong」本身也证明连接是可写的（半开连接靠这个暴露）
            case DownlinkFrame.Ping ignored -> sendControl(frames.pong());
            case DownlinkFrame.Pong ignored -> {
                // 保活应答，无需处理
            }
            case DownlinkFrame.Migrate migrate -> handleMigrate(migrate);
            case DownlinkFrame.Unknown unknown ->
                    log.log(System.Logger.Level.DEBUG, "[client] 忽略未知帧 type={0}", unknown.type());
        }
    }

    private void handleAck(DownlinkFrame.Ack ack) {
        long now = System.currentTimeMillis();
        LocalMessage row = table.find(ack.clientMsgId()).orElse(null);
        LocalMessage updated = null;
        if (row != null) {
            updated = row.withSent(ack.serverMsgId(), ack.seq(), now);
            table.upsert(updated);
            if (ack.ok() && ack.seq() > 0) {
                // 这个 seq 是本端自己占掉的号：后面判空洞时要把它当成「已知」，不能当丢消息
                rememberOwnSeq(conversationOf(updated), ack.seq());
            }
        }
        LocalMessage finalRow = updated;
        notifyListener(() -> listener.onAck(ack, finalRow));
    }

    /**
     * 这条消息属于哪个会话。
     *
     * <p>群聊的行里带 {@code conversationId}；单聊客户端发的时候只给 {@code to}（会话 ID 由服务端推导），
     * 所以这里按服务端的规则 {@code s:{小}:{大}} 补出来。推导不出来（没有 to）就返回 null，
     * 该次 ack 不参与空洞判定——退化成本次修复前的行为，不会误伤别的会话。
     */
    private String conversationOf(LocalMessage row) {
        if (row.conversationId() != null && !row.conversationId().isBlank()) {
            return row.conversationId();
        }
        String to = row.to();
        if (to == null || to.isBlank()) {
            return null;
        }
        String self = config.userId();
        return self.compareTo(to) <= 0 ? "s:" + self + ':' + to : "s:" + to + ':' + self;
    }

    private void rememberOwnSeq(String conversationId, long seq) {
        if (conversationId == null) {
            return;
        }
        synchronized (ownSeqs) {
            NavigableSet<Long> seqs = ownSeqs.computeIfAbsent(conversationId, key -> new TreeSet<>());
            seqs.add(seq);
            while (seqs.size() > OWN_SEQ_KEEP) {
                seqs.pollFirst();
            }
        }
    }

    private void handlePush(DownlinkFrame.Push push) {
        String conversationId = push.conversationId();
        long previous = table.lastInboundSeq(conversationId).orElse(0);
        if (!table.recordInbound(LocalMessage.inbound(push, System.currentTimeMillis()))) {
            // 离线补发是「至少一次」，同一 seq 被推多次是协议的正常组成部分，不是错误
            log.log(System.Logger.Level.DEBUG, "[client] 重复消息已丢弃 conv={0} seq={1}",
                    conversationId, push.seq());
            return;
        }
        boolean flushNow;
        synchronized (pendingRecv) {
            pendingRecv.merge(conversationId, push.seq(), Math::max);
            flushNow = pendingRecv.size() >= config.recvBatchMax();
        }
        notifyListener(() -> listener.onMessage(push));
        if (previous > 0 && push.seq() > previous + 1) {
            // 水位到这里之间缺的号，可能只是「我自己发的」（会话 seq 双方共用），
            // 只有真的存在既没收到、又不是本端发出的号，才算空洞
            Long firstUnknown = firstUnknownSeq(conversationId, previous + 1, push.seq());
            if (firstUnknown != null) {
                long expectedFrom = firstUnknown;
                notifyListener(() -> listener.onGap(conversationId, expectedFrom, push.seq()));
            }
        }
        if (flushNow) {
            flushRecv();
        }
    }

    /**
     * 在 {@code [fromInclusive, toExclusive)} 里找第一个「既没收到、也不是本端发出」的 seq。
     *
     * @return 全区间都是本端自己发的号时返回 {@code null}（此时不是空洞）
     */
    private Long firstUnknownSeq(String conversationId, long fromInclusive, long toExclusive) {
        if (toExclusive <= fromInclusive) {
            return null;
        }
        if (toExclusive - fromInclusive > GAP_SCAN_LIMIT) {
            // 跨度异常大（服务端重置过 seq，或断线很久）：不逐个排除，直接按最保守的起点报，
            // 让宿主去拉一次历史——比漏报安全
            return fromInclusive;
        }
        NavigableSet<Long> own;
        synchronized (ownSeqs) {
            NavigableSet<Long> known = ownSeqs.get(conversationId);
            // 拷一份再出锁：后面要回调宿主，不能持锁跑宿主的代码
            own = known == null ? null : new TreeSet<>(known);
        }
        for (long seq = fromInclusive; seq < toExclusive; seq++) {
            if (own == null || !own.contains(seq)) {
                return seq;
            }
        }
        return null;
    }

    private void handleError(DownlinkFrame.Error error) {
        if (error.code() == 401) {
            // 认证问题：重连多少次都是 401，而且会把账号/IP 打进风控。交给宿主换 token。
            notifyListener(() -> listener.onAuthRejected(error.message()));
            shutdown("认证被拒：" + error.message());
            return;
        }
        String clientMsgId = error.clientMsgId();
        if (clientMsgId != null && !clientMsgId.isBlank()) {
            long now = System.currentTimeMillis();
            table.find(clientMsgId).ifPresent(row -> table.upsert(retryable(error.code())
                    // 服务端临时不可用：保持 PENDING，只把重投时间往后推
                    ? row.withAttempt(now + sendBackoff.delayMs(row.attempts()), now)
                    // 4xx：这条消息本身有问题，重投只是重复消耗流量与电量
                    : row.withFailed("code=" + error.code() + " " + error.message(), now)));
        }
        notifyListener(() -> listener.onError(error));
    }

    /**
     * 被服务端踢下线：通知宿主，然后<b>彻底停机</b>。
     *
     * <p>网关的顺序是「先发 {@code session.kick} 再 close」，收到这一帧说明连接马上就要断。
     * 如果照常按网络故障重连，被踢的设备会在 0.5s 后自己回来，踢下线就退化成「闪断一次」。
     * 所以这里直接走 {@link #shutdown}（与 401 同一套语义：本实例不再重连），
     * 宿主想重新登录时用新的 {@link ImClientConfig} 新建一个实例即可。
     *
     * <p>先通知再停机：宿主在回调里读到的 {@link ImClient#state()} 仍是连接态，
     * 展示 {@code reason}、清理本地凭据都不会撞上已经关掉的本地表。
     */
    private void handleKick(DownlinkFrame.SessionKick kick) {
        log.log(System.Logger.Level.WARNING, "[client] 被服务端踢下线：{0}", kick.reason());
        notifyListener(() -> listener.onSessionKick(kick));
        shutdown("被服务端踢下线：" + kick.reason());
    }

    private void handleMigrate(DownlinkFrame.Migrate migrate) {
        synchronized (lock) {
            retryAfterHintMs = Math.max(0, migrate.retryAfterMs());
        }
        FrameChannel conn = connectedConnection();
        if (conn != null) {
            // 断开后由 handleDisconnect 读 retryAfterHintMs 走「迁移」分支（含客户端抖动）
            conn.close();
        }
    }

    // ==== 定时任务 ====

    private void startTimers() {
        synchronized (lock) {
            if (closed || state != State.CONNECTED) {
                return;
            }
            cancel(recvTask);
            recvTask = scheduler.scheduleWithFixedDelay(this::flushRecv,
                    config.recvFlushIntervalMs(), config.recvFlushIntervalMs(), TimeUnit.MILLISECONDS);
            cancel(outboxTask);
            outboxTask = scheduler.scheduleWithFixedDelay(this::flushOutbox,
                    config.outboxScanIntervalMs(), config.outboxScanIntervalMs(), TimeUnit.MILLISECONDS);
            cancel(pingTask);
            pingTask = null;
            if (config.keepAliveIntervalMs() > 0) {
                pingTask = scheduler.scheduleAtFixedRate(this::sendPing,
                        config.keepAliveIntervalMs(), config.keepAliveIntervalMs(), TimeUnit.MILLISECONDS);
            }
        }
    }

    /** 重投到点的待发消息（重连成功时也会主动调一次，不必等定时器） */
    private void flushOutbox() {
        if (isClosed() || connectedConnection() == null) {
            return;
        }
        for (LocalMessage row : table.pendingDue(System.currentTimeMillis(), config.outboxBatch())) {
            if (isClosed() || connectedConnection() == null) {
                return;
            }
            trySend(row);
        }
    }

    /**
     * 发送一行本地消息并记录这次尝试。
     *
     * <p>发出去但 ack 未到时也要把 {@code nextAttemptAt} 往后推：
     * 「至少一次」不等于「每 500ms 重复一次」——那样一个慢 ack 就会触发一串重复上行。
     */
    private boolean trySend(LocalMessage row) {
        FrameChannel conn = connectedConnection();
        if (conn == null) {
            return false;
        }
        long now = System.currentTimeMillis();
        try {
            conn.send(frames.send(row));
        } catch (IOException e) {
            // 写失败说明连接已经坏了：标记重试（保持 PENDING），并让读循环去感知断开、触发重连
            String error = "write failed: " + e.getMessage();
            table.upsert(row.withAttempt(now + sendBackoff.delayMs(row.attempts()), error, now));
            conn.close();
            return false;
        }
        table.upsert(row.withAttempt(now + sendBackoff.delayMs(row.attempts()), now));
        return true;
    }

    /** 上报攒批的 {@code msg.recv}（协议要求批量：逐条 ACK 会把消息数放大成帧数） */
    private void flushRecv() {
        List<Frames.DeliveredAck> batch = new ArrayList<>();
        synchronized (pendingRecv) {
            if (pendingRecv.isEmpty()) {
                return;
            }
            pendingRecv.forEach((conversationId, seq) ->
                    batch.add(new Frames.DeliveredAck(conversationId, seq)));
            pendingRecv.clear();
        }
        if (!sendControl(frames.recv(batch))) {
            // 没发出去就放回：水位只前进，晚点重报没有任何副作用
            synchronized (pendingRecv) {
                batch.forEach(ack -> pendingRecv.merge(ack.conversationId(), ack.seq(), Math::max));
            }
        }
    }

    private void sendPing() {
        if (connectedConnection() == null) {
            return;
        }
        sendControl(frames.ping());
    }

    // ==== 工具 ====

    private boolean sendControl(String frame) {
        FrameChannel conn = connectedConnection();
        if (conn == null) {
            return false;
        }
        try {
            conn.send(frame);
            return true;
        } catch (IOException e) {
            log.log(System.Logger.Level.DEBUG, "[client] 控制帧发送失败：" + e.getMessage());
            conn.close();
            return false;
        }
    }

    private FrameChannel connectedConnection() {
        synchronized (lock) {
            return state == State.CONNECTED ? connection : null;
        }
    }

    private boolean isClosed() {
        synchronized (lock) {
            return closed;
        }
    }

    private void ensureOpen() {
        if (isClosed()) {
            throw new IllegalStateException("客户端已关闭");
        }
    }

    /** 401/403/404/400 重试无意义；5xx 与 429 是「过会再来」 */
    private static boolean retryable(int code) {
        return code >= 500 || code == 429;
    }

    private void notifyListener(Runnable callback) {
        try {
            callback.run();
        } catch (RuntimeException e) {
            // 宿主的异常不能反过来打死 SDK 的状态机：记一笔继续跑
            log.log(System.Logger.Level.WARNING, "[client] 宿主回调抛出异常：" + e.getMessage(), e);
        }
    }

    private static void cancel(ScheduledFuture<?> future) {
        if (future != null) {
            future.cancel(false);
        }
    }
}
