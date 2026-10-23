package com.github.im.gateway.session;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;

/**
 * Netty Channel 上的会话实现。
 *
 * <p>两种传输的差异被收敛到 {@link #send} 与 {@link #close}：
 * <ul>
 *   <li>WebSocket：写 {@code TextWebSocketFrame}（由 pipeline 里的编码器把 String 包成帧），
 *       关闭时先发 1001（going away）再断，客户端能区分「服务端要走了」和「网络断了」；</li>
 *   <li>TCP：写 String，由 pipeline 里的 {@code StringEncoder + LengthFieldPrepender}
 *       变成「4 字节大端长度 + UTF-8 JSON」，关闭直接断链。</li>
 * </ul>
 *
 * <p>{@code userId} 之所以可变：TCP 不像 WebSocket 能在 HTTP 握手时带 query 参数，
 * 身份只能由首帧 {@code connect} 声明，认证成功后回填。
 */
public final class NettyClientSession implements ClientSession {

    private final Channel channel;
    private final String transport;
    private volatile String userId;
    private volatile String deviceId;

    private NettyClientSession(Channel channel, String transport) {
        this.channel = channel;
        this.transport = transport;
    }

    public static NettyClientSession websocket(Channel channel) {
        return new NettyClientSession(channel, TRANSPORT_WS);
    }

    public static NettyClientSession tcp(Channel channel) {
        return new NettyClientSession(channel, TRANSPORT_TCP);
    }

    /** 认证成功后绑定身份（TCP 首帧 connect；WebSocket 在握手阶段直接绑定） */
    public NettyClientSession bind(String userId, String deviceId) {
        this.userId = userId;
        this.deviceId = deviceId;
        return this;
    }

    @Override
    public String id() {
        return channel.id().asShortText();
    }

    @Override
    public String userId() {
        return userId;
    }

    @Override
    public String deviceId() {
        return deviceId;
    }

    @Override
    public String transport() {
        return transport;
    }

    @Override
    public boolean isOpen() {
        return channel.isActive();
    }

    @Override
    public boolean send(String payload) {
        if (!channel.isActive()) {
            return false;
        }
        // isWritable() 反映 Netty 出站缓冲水位：慢客户端积压超过高水位时，写会一直堆在堆外内存里。
        // 这里宁可按「投递失败」返回，让 IM 服务把帧落离线队列，也不让积压把网关内存吃穿。
        // 代价是该帧可能「推了又落离线」，客户端按 serverMsgId/seq 去重即可（幂等是协议层的约定）。
        if (!channel.isWritable()) {
            return false;
        }
        // 写是异步的，失败通过 listener 体现；这里不阻塞业务线程等结果，
        // 真正的「连接已死」由 isActive + 空闲巡检兜底。
        channel.writeAndFlush(payload);
        return true;
    }

    @Override
    public void close(String reason) {
        if (!channel.isActive()) {
            return;
        }
        if (TRANSPORT_WS.equals(transport)) {
            channel.writeAndFlush(new CloseWebSocketFrame(1001, reason))
                    .addListener(ChannelFutureListener.CLOSE);
        } else {
            channel.close();
        }
    }

    @Override
    public String toString() {
        return transport + "[" + id() + ", user=" + userId + "]";
    }
}
