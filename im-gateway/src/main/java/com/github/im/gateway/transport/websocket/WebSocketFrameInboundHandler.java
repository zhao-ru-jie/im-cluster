package com.github.im.gateway.transport.websocket;

import com.github.im.gateway.session.NettyClientSession;
import com.github.im.gateway.session.SessionAttributes;
import com.github.im.gateway.transport.SerialExecutor;
import com.github.im.gateway.transport.SessionLifecycle;
import io.netty.channel.Channel;
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
 * <p><b>线程模型</b>：三个钩子都只做「取数据 + 提交任务」，真正的业务跑在 {@link SerialExecutor}
 * 的虚拟线程上（同一连接严格串行）。{@code onOpen} 里有 Redis 写和一次 gRPC 补拉，
 * {@code onFrame} 里要等 IM 服务返回——都不能待在 event loop 上。
 * 事件与帧按提交顺序执行，所以「先握手、后收帧」的因果不会因为切线程而颠倒。
 *
 * <p><b>背压</b>：积压超过高水位时不开除连接，而是 {@code setAutoRead(false)} 暂停读，
 * 把压力交回客户端（TCP 窗口收缩）；降到低水位自动恢复。见 {@link SerialExecutor#onBackpressure}。
 *
 * <p>协议级 ping/pong 帧（{@code PingWebSocketFrame}）由 {@code WebSocketServerProtocolHandler}
 * 自动应答，不会走到这里——业务层的 {@code {"type":"ping"}} 是另一套应用级心跳。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class WebSocketFrameInboundHandler extends SimpleChannelInboundHandler<TextWebSocketFrame> {

    private final SessionLifecycle lifecycle;
    private final SerialExecutor serial;
    /** 握手完成后由虚拟线程赋值；channelInactive 也在这条串行队列里读，volatile 只为跨线程可见性 */
    private volatile NettyClientSession session;

    public WebSocketFrameInboundHandler(SessionLifecycle lifecycle, SerialExecutor serial) {
        this.lifecycle = lifecycle;
        this.serial = serial;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        // 背压钩子：暂停/恢复只作用在本连接上，两个方向都可能跨线程调用，Channel 内部已保证线程安全
        Channel channel = ctx.channel();
        serial.onBackpressure(
                () -> channel.config().setAutoRead(false),
                () -> {
                    if (channel.isActive()) {
                        channel.config().setAutoRead(true);
                    }
                });
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        // 握手完成时登记会话
        if (evt instanceof WebSocketServerProtocolHandler.HandshakeComplete) {
            // 用 submit 而不是 offer：会话登记不能被背压推迟到队列末尾
            serial.submit(() -> {
                session = NettyClientSession.websocket(ctx.channel())
                        .bind(ctx.channel().attr(SessionAttributes.USER_ID).get(),
                                ctx.channel().attr(SessionAttributes.DEVICE_ID).get());
                // 心跳 handler 从 Channel 属性取会话；必须在 onOpen 前挂好，否则首次空闲事件读不到会话
                ctx.channel().attr(SessionAttributes.SESSION).set(session);
                lifecycle.onOpen(session);
            });
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, TextWebSocketFrame frame) {
        // 必须在 event loop 上取文本：frame 在本方法返回后就会被 SimpleChannelInboundHandler 释放
        String payload = frame.text();
        // 总是入队、不丢帧：积压到高水位由 SerialExecutor 回调暂停读取，而不是在这里断连
        serial.offer(() -> {
            // session 的判断放进任务体内：握手任务可能还排在队列里没执行
            NettyClientSession current = session;
            if (current == null) {
                return;
            }
            lifecycle.onFrame(current, payload);
        });
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 排在已入队的帧之后执行：保证「最后一帧处理完」再清会话/路由
        serial.submit(() -> {
            NettyClientSession current = session;
            if (current != null) {
                lifecycle.onClose(current);
            }
        });
        ctx.fireChannelInactive();
    }
}
