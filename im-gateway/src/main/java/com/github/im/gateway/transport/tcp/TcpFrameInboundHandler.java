package com.github.im.gateway.transport.tcp;

import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.NettyClientSession;
import com.github.im.gateway.session.SessionAttributes;
import com.github.im.gateway.transport.SessionLifecycle;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;

import java.util.concurrent.TimeUnit;

/**
 * 原生 TCP 传输的适配器：首帧 {@code connect} 认证 + 后续帧交给 {@link SessionLifecycle}。
 *
 * <p>为什么 TCP 需要额外的认证帧：WebSocket 有 HTTP 握手，可以在 query 里带
 * {@code userId}；裸 TCP 没有这个位置，只能约定「连上后第一帧必须是」：
 * <pre>{"ver":1,"type":"connect","userId":"alice","deviceId":"mobile"}</pre>
 * 认证成功后网关回 {@code {"type":"connected",...}}，客户端据此确认身份已被接受。
 *
 * <p><b>防挂连接</b>：认证窗口有超时（默认 10s）。否则一个只建连不发数据的客户端会一直占着
 * 文件句柄和内存，而又不在 {@code ConnectionManager} 里（空闲巡检看不到它）。
 *
 * <p>未认证前的任何非 connect 帧一律拒绝并断开，避免「匿名连接」进入业务通道。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class TcpFrameInboundHandler extends SimpleChannelInboundHandler<String> {

    private static final Logger log = LoggerFactory.getLogger(TcpFrameInboundHandler.class);

    private final SessionLifecycle lifecycle;
    private final ClientFrames frames;
    private final long authTimeoutMs;

    /** 认证成功后才非空；为 null 表示当前连接还没绑定身份 */
    private NettyClientSession session;

    public TcpFrameInboundHandler(SessionLifecycle lifecycle, ClientFrames frames, long authTimeoutMs) {
        this.lifecycle = lifecycle;
        this.frames = frames;
        this.authTimeoutMs = authTimeoutMs;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.executor().schedule(() -> {
            if (session == null && ctx.channel().isActive()) {
                log.debug("tcp connection {} not authenticated in {}ms, closing", ctx.channel().id().asShortText(), authTimeoutMs);
                ctx.writeAndFlush(frames.error(null, 401, "connect frame required"))
                        .addListener(ChannelFutureListener.CLOSE);
            }
        }, authTimeoutMs, TimeUnit.MILLISECONDS);
        ctx.fireChannelActive();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String payload) {
        if (session == null) {
            authenticate(ctx, payload);
            return;
        }
        lifecycle.onFrame(session, payload);
    }

    private void authenticate(ChannelHandlerContext ctx, String payload) {
        JsonNode node;
        try {
            node = frames.parse(payload);
        } catch (RuntimeException e) {
            reject(ctx, 400, "invalid frame: " + e.getMessage());
            return;
        }
        String type = text(node, "type");
        if (!ClientFrames.TYPE_CONNECT.equals(type)) {
            reject(ctx, 401, "first frame must be type=connect");
            return;
        }
        String userId = text(node, "userId");
        if (userId == null || userId.isBlank()) {
            reject(ctx, 401, "userId is required");
            return;
        }
        session = NettyClientSession.tcp(ctx.channel()).bind(userId, text(node, "deviceId"));
        // 心跳 handler 靠这个属性下发 ping / 判定超时
        ctx.channel().attr(SessionAttributes.SESSION).set(session);
        // 先回认证应答，再登记会话：客户端收到 connected 后才会开始发业务帧
        ctx.writeAndFlush(frames.connected(userId, ClientSession.TRANSPORT_TCP));
        lifecycle.onOpen(session);
    }

    private void reject(ChannelHandlerContext ctx, int code, String message) {
        ctx.writeAndFlush(frames.error(null, code, message)).addListener(ChannelFutureListener.CLOSE);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (session != null) {
            lifecycle.onClose(session);
        }
        ctx.fireChannelInactive();
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asString();
    }
}
