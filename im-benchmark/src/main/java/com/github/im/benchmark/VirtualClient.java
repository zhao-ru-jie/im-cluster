package com.github.im.benchmark;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolConfig;
import io.netty.handler.codec.http.websocketx.WebSocketClientProtocolHandler;
import io.netty.handler.codec.http.websocketx.WebSocketCloseStatus;
import io.netty.handler.codec.http.websocketx.WebSocketVersion;
import io.netty.handler.ssl.SslContext;

import java.net.URI;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个虚拟连接（一个「用户 + 设备」），承载：握手 → 心跳应答 → 按速率发消息 → 收回 ack/push。
 *
 * <p>线程模型：发送定时器挂在 {@code channel.eventLoop()} 上，读（{@link #channelRead0}）也在同一个
 * event loop 线程里跑，所以 {@link #seq} 这类状态不需要加锁——这是压测端能扛住十万连接的前提，
 * 每个连接各自在自己的 event loop 上自转，没有共享锁热点。
 *
 * <p>两个「时间戳」技巧，避免为每条消息维护一张在途表：
 * <ul>
 *   <li><b>RTT</b>：{@code clientMsgId} 里直接编码发送时刻（base36 纳秒），
 *       网关的 {@code msg.ack} 会原样回显它，收到 ack 解出来就能算 RTT，零额外内存；</li>
 *   <li><b>端到端</b>：消息体以「十进制纳秒 + |」开头，接收方的 push 里带的正是这个 body，
 *       同一 JVM 内 {@code System.nanoTime()} 可比，于是直接得到发送到投递的延迟。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class VirtualClient extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private static final long HANDSHAKE_TIMEOUT_MILLIS = 10_000;
    private static final int MAX_FRAME_BYTES = 1024 * 1024;

    private final int index;
    private final BenchmarkOptions options;
    private final BenchMetrics metrics;
    private final Frames frames;
    private final AtomicBoolean trafficEnabled;

    private final String userId;
    private final String targetUserId;
    private final String bodyPadding;
    private final AtomicInteger inFlight = new AtomicInteger();

    private Channel channel;
    private ScheduledFuture<?> sender;
    private boolean handshakeDone;
    /** 握手是否成功过：决定断开时算「连接关闭」还是「建连失败」 */
    private boolean opened;
    /** 失败是否已记账：一条连接只允许记一次失败，避免异常 + 断开被算成两次 */
    private boolean failureNoted;
    private long seq;

    public VirtualClient(int index, BenchmarkOptions options, BenchMetrics metrics,
                         Frames frames, AtomicBoolean trafficEnabled) {
        this.index = index;
        this.options = options;
        this.metrics = metrics;
        this.frames = frames;
        this.trafficEnabled = trafficEnabled;
        this.userId = options.userId(index);
        this.targetUserId = options.targetUserId(index);
        this.bodyPadding = options.bodyBytes() == 0 ? "" : "x".repeat(options.bodyBytes());
    }

    public String userId() {
        return userId;
    }

    /** 每条连接一个独立的 initializer：握手 URI 里带着本连接的 userId/deviceId。 */
    public ChannelInitializer<Channel> initializer(SslContext sslContext) {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(Channel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                if (sslContext != null) {
                    pipeline.addLast(sslContext.newHandler(ch.alloc(), options.host(), options.port()));
                }
                pipeline.addLast(new HttpClientCodec());
                pipeline.addLast(new HttpObjectAggregator(64 * 1024));
                pipeline.addLast(new WebSocketClientProtocolHandler(protocolConfig()));
                pipeline.addLast(VirtualClient.this);
            }
        };
    }

    private WebSocketClientProtocolConfig protocolConfig() {
        return WebSocketClientProtocolConfig.newBuilder()
                .webSocketUri(URI.create(options.uri(index)))
                .version(WebSocketVersion.V13)
                .allowExtensions(false)
                .maxFramePayloadLength(MAX_FRAME_BYTES)
                .handshakeTimeoutMillis(HANDSHAKE_TIMEOUT_MILLIS)
                .handleCloseFrames(true)
                // 压测端不校验 UTF-8：帧内容是我们自己造的，省下这条连接上的每次校验开销
                .withUTF8Validator(false)
                .build();
    }

    // ==== 连接生命周期 ====

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        this.channel = ctx.channel();
        ctx.fireChannelActive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt == WebSocketClientProtocolHandler.ClientHandshakeStateEvent.HANDSHAKE_COMPLETE) {
            handshakeDone = true;
            opened = true;
            metrics.onConnectOpened();
            startSender(ctx);
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    /**
     * 发送定时器：每 {@code 1/send-rate} 秒发一条。
     *
     * <p>为什么是「定时器 + 在途上限」而不是「发一条等一条 ack」（纯闭环）：
     * 纯闭环测出来的是「单连接串行 RTT」，会系统性低估服务端吞吐；
     * 纯开环（定时发、不做任何约束）又会在服务端变慢时把连接撑爆成内存炸弹。
     * 定时发 + 单连接在途上限，是这两者之间那个能同时看出「吞吐」和「背压」的位置。
     */
    private void startSender(ChannelHandlerContext ctx) {
        long intervalNanos = options.sendIntervalNanos();
        if (intervalNanos <= 0) {
            return;
        }
        sender = ctx.channel().eventLoop().scheduleAtFixedRate(
                () -> sendOne(ctx), intervalNanos, intervalNanos, TimeUnit.NANOSECONDS);
    }

    private void sendOne(ChannelHandlerContext ctx) {
        if (!trafficEnabled.get() || !handshakeDone) {
            return;
        }
        Channel ch = ctx.channel();
        if (!ch.isActive()) {
            return;
        }
        if (inFlight.get() >= options.maxInFlight()) {
            metrics.onBackpressure();
            return;
        }
        long nanos = System.nanoTime();
        String clientMsgId = Long.toString(nanos, 36) + '-' + (++seq);
        String body = nanos + "|" + bodyPadding;
        inFlight.incrementAndGet();
        metrics.onSent();
        ch.writeAndFlush(new TextWebSocketFrame(frames.send(clientMsgId, targetUserId, body)));
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        Frames.Parsed parsed;
        try {
            parsed = frames.parse(frame.text());
        } catch (RuntimeException e) {
            metrics.onParseError();
            metrics.announceOnce("下行帧不是合法 JSON：" + e.getMessage());
            return;
        }
        if (parsed == null || parsed.type() == null) {
            metrics.onUnknownFrame();
            return;
        }
        switch (parsed.type()) {
            case Frames.TYPE_ACK -> {
                releaseInFlight();
                long sentAt = decodeSendNanos(parsed.clientMsgId());
                metrics.onAck(sentAt > 0 ? System.nanoTime() - sentAt : -1);
            }
            case Frames.TYPE_ERROR -> {
                releaseInFlight();
                metrics.onAckError(parsed.code());
                if (parsed.code() == 400) {
                    metrics.announceOnce("服务端以 400 拒绝了消息：帧内容不符合协议，先别调参数，先查协议");
                }
            }
            case Frames.TYPE_PING -> {
                // 网关 45s 无上行就会探测、90s 无上行直接断开，所以这里必须立刻回
                ctx.writeAndFlush(new TextWebSocketFrame(frames.pong()));
                metrics.onServerPing();
            }
            case Frames.TYPE_PONG -> metrics.onServerPong();
            case Frames.TYPE_PUSH -> {
                long sentAt = decodeSendNanosFromBody(parsed.body());
                metrics.onPush(sentAt > 0 ? System.nanoTime() - sentAt : -1);
            }
            case Frames.TYPE_MIGRATE -> metrics.onMigrate();
            default -> metrics.onUnknownFrame();
        }
    }

    private void releaseInFlight() {
        if (inFlight.get() > 0) {
            inFlight.decrementAndGet();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        String reason = describe(cause);
        if (!opened && !failureNoted) {
            failureNoted = true;
            // 握手阶段的失败通常是 400 缺 userId / 401 token 无效 / 503 摘流 / TCP 连接被拒
            metrics.onConnectFailure(reason);
        } else {
            metrics.announceOnce("连接出错（合并同类，只打一次）：" + reason);
        }
        ctx.close();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        handshakeDone = false;
        channel = null;
        if (sender != null) {
            sender.cancel(false);
            sender = null;
        }
        if (opened) {
            metrics.onConnectionClosed();
            opened = false;
        } else if (!failureNoted) {
            failureNoted = true;
            metrics.onConnectFailure("closed-before-handshake");
        }
        ctx.fireChannelInactive();
    }

    /**
     * 建连失败（TCP 被拒/超时）：这时连接还没进 pipeline，只能由 {@code Bootstrap} 的
     * connect 回调把失败交回来。用和异常路径同一个 {@code failureNoted} 标记，
     * 保证一条连接不会被记成两次失败。
     */
    public void onConnectFailed(Throwable cause) {
        if (opened || failureNoted) {
            return;
        }
        failureNoted = true;
        metrics.onConnectFailure("connect-failed: " + describe(cause));
    }

    /** 主动断开：发 close 帧再关，让网关走正常注销流程，避免「压测结束时连接数指标还挂着」。 */
    public void close() {
        Channel ch = this.channel;
        handshakeDone = false;
        if (sender != null) {
            sender.cancel(false);
            sender = null;
        }
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(new CloseWebSocketFrame(WebSocketCloseStatus.NORMAL_CLOSURE))
                    .addListener(ChannelFutureListener.CLOSE);
        }
    }

    // ==== 时间戳编解码 ====

    private static long decodeSendNanos(String clientMsgId) {
        if (clientMsgId == null) {
            return -1;
        }
        int dash = clientMsgId.indexOf('-');
        if (dash <= 0) {
            return -1;
        }
        try {
            return Long.parseLong(clientMsgId.substring(0, dash), 36);
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static long decodeSendNanosFromBody(String body) {
        if (body == null || body.isEmpty()) {
            return -1;
        }
        int bar = body.indexOf('|');
        if (bar <= 0) {
            return -1;
        }
        try {
            return Long.parseLong(body.substring(0, bar));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    private static String describe(Throwable cause) {
        String message = cause.getMessage();
        return cause.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }
}
