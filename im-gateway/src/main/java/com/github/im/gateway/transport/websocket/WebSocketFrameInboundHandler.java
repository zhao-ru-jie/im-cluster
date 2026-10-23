package com.github.im.gateway.transport.websocket;

import com.github.im.gateway.session.NettyClientSession;
import com.github.im.gateway.session.SessionAttributes;
import com.github.im.gateway.transport.SessionLifecycle;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;

/**
 * WebSocket 传输的适配器：只做「事件翻译」，所有业务语义都在 {@link SessionLifecycle}。
 *
 * <p>三个钩子：
 * <ul>
 *   <li>{@code HANDSHAKE_COMPLETE}：HTTP 升级成功，此时才能拿到 userId 并登记会话
 *       （如果在 {@code channelActive} 就登记，未通过握手校验的连接也会进连接表）；</li>
 *   <li>文本帧：交给生命周期处理（ping/pong/msg.send）；</li>
 *   <li>{@code channelInactive}：清理会话与路由。</li>
 * </ul>
 *
 * <p>协议级 ping/pong 帧（{@code PingWebSocketFrame}）由 {@code WebSocketServerProtocolHandler}
 * 自动应答，不会走到这里——业务层的 {@code {"type":"ping"}} 是另一套应用级心跳。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class WebSocketFrameInboundHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private final SessionLifecycle lifecycle;
    /** 握手完成后创建；channelInactive 时用它清理 */
    private NettyClientSession session;

    public WebSocketFrameInboundHandler(SessionLifecycle lifecycle) {
        this.lifecycle = lifecycle;
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        // 握手完成时登记会话
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            session = NettyClientSession.websocket(ctx.channel())
                    .bind(ctx.channel().attr(SessionAttributes.USER_ID).get(),
                            ctx.channel().attr(SessionAttributes.DEVICE_ID).get());
            // 心跳 handler 从 Channel 属性取会话；必须在 onOpen 前挂好，否则首次空闲事件读不到会话
            ctx.channel().attr(SessionAttributes.SESSION).set(session);
            lifecycle.onOpen(session);
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        if (session == null) {
            return;
        }
        lifecycle.onFrame(session, frame.text());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        if (session != null) {
            lifecycle.onClose(session);
        }
        ctx.fireChannelInactive();
    }
}
