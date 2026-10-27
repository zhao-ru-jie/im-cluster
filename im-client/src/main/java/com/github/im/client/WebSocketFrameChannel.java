package com.github.im.client;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * WebSocket 承载：{@code ws(s)://host:wsPort/ws?userId=..&deviceId=..&token=..}。
 *
 * <h2>为什么用 JDK 的 {@link WebSocket} 而不是自己实现 RFC 6455</h2>
 *
 * <p>手写握手要处理 {@code Sec-WebSocket-Accept}、掩码、分片、控制帧、关闭握手，
 * 任何一处写错都表现为「偶发连不上」这种最难查的故障。JDK 自带客户端从 11 起就有，
 * 零依赖、零额外 jar，而且把 TLS、代理、超时都处理好了——对 SDK 来说这是最划算的选择。
 *
 * <h2>身份在 query 里，所以握手成功即认证成功</h2>
 *
 * <p>网关的 {@code WebSocketHandshakeGate} 在 HTTP 升级阶段就校验 {@code userId}/{@code token}：
 * 缺 userId → 400、token 无效 → 401、实例摘流 → 503、路径错 → 404。
 * 因此本通道在 {@link #onOpen} 里直接回调 {@link Listener#onReady}，
 * 而握手失败则以 {@link WebSocketHandshakeException} 带出状态码（决定要不要重连）。
 *
 * <h2>为什么 send 要 join</h2>
 *
 * <p>{@code sendText} 是异步的，且 JDK 明确禁止<b>未完成</b>时再次发送。
 * 不 join 就得自己维护一条待发队列；而单连接的写频率（一条消息一次）远达不到需要队列的程度，
 * 直接串行等完成反而把「发送顺序」和「背压」一次解决。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
final class WebSocketFrameChannel implements FrameChannel, WebSocket.Listener {

    private static final System.Logger log = System.getLogger(WebSocketFrameChannel.class.getName());
    private static final Duration SEND_WAIT = Duration.ofSeconds(10);

    private final URI uri;
    private final Listener listener;
    private final CompletableFuture<WebSocket> handshake = new CompletableFuture<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    /** 分片消息的累积缓冲：一个大帧可能被拆成多次 onText(last=false) */
    private final StringBuilder partial = new StringBuilder();

    private WebSocketFrameChannel(URI uri, Listener listener) {
        this.uri = uri;
        this.listener = listener;
    }

    /**
     * 发起握手（异步）。
     *
     * <p>返回的通道可能还没握手完：{@code send()} 会等握手完成，
     * 而上层本来就要等 {@link Listener#onReady} 才会开始发业务帧。
     */
    static WebSocketFrameChannel open(URI uri, int connectTimeoutMs, Listener listener) {
        WebSocketFrameChannel channel = new WebSocketFrameChannel(uri, listener);
        channel.start(connectTimeoutMs);
        return channel;
    }

    /**
     * 拼握手 URI。
     *
     * <p>query 一定要编码：userId 里出现 {@code &}、{@code #} 或空格时，
     * 不编码会让网关解析出「半个 userId」或直接 400，而现象是「这个用户永远连不上」。
     */
    static URI uri(String host, int port, boolean tls, String path, String userId, String deviceId,
                   String token) {
        StringBuilder sb = new StringBuilder();
        sb.append(tls ? "wss://" : "ws://").append(host).append(':').append(port);
        sb.append(path == null || path.isBlank() ? "/ws" : path);
        sb.append("?userId=").append(encode(userId));
        if (deviceId != null && !deviceId.isBlank()) {
            sb.append("&deviceId=").append(encode(deviceId));
        }
        if (token != null && !token.isBlank()) {
            sb.append("&token=").append(encode(token));
        }
        return URI.create(sb.toString());
    }

    private static String encode(String value) {
        return URLEncoder.encode(value == null ? "" : value, StandardCharsets.UTF_8);
    }

    private void start(int connectTimeoutMs) {
        Duration timeout = Duration.ofMillis(Math.max(1, connectTimeoutMs));
        HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build()
                .newWebSocketBuilder()
                .connectTimeout(timeout)
                .buildAsync(uri, this)
                .whenComplete((webSocket, error) -> {
                    if (error == null) {
                        // 成功路径已经由 onOpen 处理（它可能先于这里执行）
                        return;
                    }
                    Throwable cause = describe(error);
                    handshake.completeExceptionally(cause);
                    closed.set(true);
                    listener.onClosed(this, cause);
                });
    }

    /**
     * 把 JDK 的失败包装成带状态码的异常。
     *
     * <p>JDK 的 {@code WebSocketHandshakeException} 拿得到 HTTP 响应，
     * 但它是 checked 的 {@code IOException} 子类且藏在 {@code CompletionException} 里；
     * 这里统一成我们自己的类型，上层只认「状态码」这一件事。
     */
    private static Throwable describe(Throwable error) {
        Throwable cause = error instanceof CompletionException && error.getCause() != null
                ? error.getCause() : error;
        if (cause instanceof java.net.http.WebSocketHandshakeException handshake) {
            int status = handshake.getResponse().statusCode();
            return new WebSocketHandshakeException(status,
                    "WebSocket 握手被拒：HTTP " + status, handshake);
        }
        return cause;
    }

    // ==== WebSocket.Listener ====

    @Override
    public void onOpen(WebSocket webSocket) {
        log.log(System.Logger.Level.DEBUG, "[client] WebSocket 握手完成 {0}", uri);
        handshake.complete(webSocket);
        listener.onReady(this);
        // 必须主动请求下一个消息，否则收到第一条之后就不再接收
        webSocket.request(1);
    }

    @Override
    public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
        partial.append(data);
        if (last) {
            String payload = partial.toString();
            partial.setLength(0);
            try {
                listener.onFrame(this, payload);
            } catch (RuntimeException e) {
                log.log(System.Logger.Level.WARNING, "[client] 处理下行帧失败：" + e.getMessage(), e);
            }
        }
        webSocket.request(1);
        return null;
    }

    @Override
    public CompletionStage<?> onClose(WebSocket webSocket, int statusCode, String reason) {
        close();
        listener.onClosed(this, new IOException(
                "网关关闭了 WebSocket：code=" + statusCode + " reason=" + reason));
        return null;
    }

    @Override
    public void onError(WebSocket webSocket, Throwable error) {
        close();
        if (!handshake.isDone()) {
            handshake.completeExceptionally(error);
        }
        listener.onClosed(this, error);
    }

    // ==== FrameChannel ====

    @Override
    public void send(String frame) throws IOException {
        WebSocket webSocket = awaitHandshake();
        try {
            synchronized (this) {
                webSocket.sendText(frame, true).join();
            }
        } catch (CompletionException e) {
            throw new IOException("WebSocket 发送失败：" + e.getMessage(), describe(e));
        }
    }

    private WebSocket awaitHandshake() throws IOException {
        try {
            return handshake.get(SEND_WAIT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException io) {
                throw io;
            }
            throw new IOException("WebSocket 握手失败：" + cause, cause);
        } catch (TimeoutException e) {
            throw new IOException("WebSocket 握手超时", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("等待 WebSocket 握手时被中断", e);
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        if (!handshake.isDone()) {
            handshake.completeExceptionally(new IOException("WebSocket 握手未完成即关闭"));
            return;
        }
        try {
            handshake.getNow(null).sendClose(WebSocket.NORMAL_CLOSURE, "bye");
        } catch (RuntimeException e) {
            log.log(System.Logger.Level.DEBUG, "[client] 发送 close 帧失败：" + e.getMessage());
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public String remote() {
        return String.valueOf(uri);
    }
}
