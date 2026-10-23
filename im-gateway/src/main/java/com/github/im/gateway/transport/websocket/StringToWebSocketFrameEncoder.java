package com.github.im.gateway.transport.websocket;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.websocketx.TextWebSocketFrame;

import java.util.List;

/**
 * 把 {@code String} 包成 {@code TextWebSocketFrame}（出站）。
 *
 * <p>为什么需要它：{@code ClientSession.send} 只接受「已序列化好的 JSON 字符串」这一种形态，
 * 传输细节（WS 文本帧 / TCP 长度帧）下沉到各自的 pipeline。
 * 这样 {@code ConnectionManager} 不用写 {@code if (websocket) ... else ...}。
 */
public class StringToWebSocketFrameEncoder extends MessageToMessageEncoder<String> {

    @Override
    protected void encode(ChannelHandlerContext ctx, String payload, List<Object> out) {
        out.add(new TextWebSocketFrame(payload));
    }
}
