package com.github.im.gateway.transport.websocket;

import com.github.im.gateway.auth.AuthGuard;
import com.github.im.gateway.auth.AuthVerdict;
import com.github.im.gateway.drain.GatewayDrainService;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import com.github.im.gateway.session.SessionAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * WebSocket 握手前的准入检查（对应 Spring 版本里 {@code HandshakeInterceptor} 的位置）。
 *
 * <p>它必须排在 {@code WebSocketServerProtocolHandler} 之前，因为一旦交给协议处理器，
 * 101 Switching Protocols 就已经发出去了，再也不方便回一个 503/400 的 HTTP 状态码。
 *
 * <p>四件事：
 * <ol>
 *   <li><b>摘流拦截</b>：处于 drain 中的实例直接 503，避免「边摘边进」；</li>
 *   <li>路径校验：只接受 {@code im.gateway.transport.ws-path}（默认 {@code /ws}）；</li>
 *   <li>身份提取：{@code userId} 必填、{@code deviceId} 可选，写进 Channel 属性供后续使用；</li>
 *   <li><b>token 校验</b>：交给 {@link AuthGuard}（{@code im.auth.mode} 三档）。
 *       放在握手阶段而不是首帧，是因为此时还能回一个正常的 HTTP 401，
 *       客户端能直接看出「凭据不对」而不是「连上了但被断」。</li>
 * </ol>
 *
 * <p>原生 TCP 不需要这一步：它没有 HTTP 握手，身份由首帧 {@code connect} 声明。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class WebSocketHandshakeGate extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(WebSocketHandshakeGate.class);

    private final GatewayDrainService drainService;
    private final String wsPath;
    private final AuthGuard authGuard;

    public WebSocketHandshakeGate(GatewayDrainService drainService, String wsPath, AuthGuard authGuard) {
        this.drainService = drainService;
        this.wsPath = wsPath;
        this.authGuard = authGuard;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (!(msg instanceof FullHttpRequest request)) {
            ctx.fireChannelRead(msg);
            return;
        }
        if (drainService.isDraining()) {
            // 摘流中：让客户端立刻换实例重连，不要在这个实例上建连
            log.debug("reject handshake: gateway is draining");
            reject(ctx, request, HttpResponseStatus.SERVICE_UNAVAILABLE, "gateway is draining");
            return;
        }

        String uri = request.uri();
        int queryIdx = uri.indexOf('?');
        String path = queryIdx >= 0 ? uri.substring(0, queryIdx) : uri;
        if (!wsPath.equals(path)) {
            reject(ctx, request, HttpResponseStatus.NOT_FOUND, "unknown endpoint, use " + wsPath);
            return;
        }

        Map<String, String> query = parseQuery(queryIdx >= 0 ? uri.substring(queryIdx + 1) : null);
        String userId = query.get("userId");
        if (userId == null || userId.isBlank()) {
            reject(ctx, request, HttpResponseStatus.BAD_REQUEST, "userId is required");
            return;
        }
        // 鉴权：mode=off 时直接放行；warn 只记录；enforce 拒绝并回 401
        if (authGuard != null) {
            AuthVerdict verdict = authGuard.check(userId, query.get("token"));
            if (!verdict.accepted()) {
                log.debug("reject handshake: userId={} reason={}", userId, verdict.reason());
                reject(ctx, request, HttpResponseStatus.UNAUTHORIZED, verdict.reason());
                return;
            }
        }
        ctx.channel().attr(SessionAttributes.USER_ID).set(userId);
        String deviceId = query.get("deviceId");
        if (deviceId != null && !deviceId.isBlank()) {
            ctx.channel().attr(SessionAttributes.DEVICE_ID).set(deviceId);
        }
        // 关键：WebSocketServerProtocolHandler 默认用 uri().equals(websocketPath) 判断路径，
        // 不会剥离 query string。若把 `/ws?userId=alice` 原样交下去，它认为「不是本 handler 的路径」，
        // 直接把请求转发到 pipeline 尾部丢掉——既不回 101 也不报错（表现为客户端握手无声超时）。
        // 身份已经解析进 Channel 属性，因此这里安全地把 URI 还原成纯路径。
        request.setUri(wsPath);
        // 交还给 WebSocketServerProtocolHandler 完成握手
        ctx.fireChannelRead(request);
    }

    private void reject(ChannelHandlerContext ctx, FullHttpRequest request,
                        HttpResponseStatus status, String reason) {
        byte[] body = ("{\"error\":\"" + reason + "\"}").getBytes(StandardCharsets.UTF_8);
        FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(body));
        response.headers().set(HttpHeaderNames.CONTENT_TYPE, "application/json; charset=UTF-8");
        response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, body.length);
        ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
        request.release();
    }

    private static Map<String, String> parseQuery(String query) {
        Map<String, String> params = new HashMap<>();
        if (query == null || query.isBlank()) {
            return params;
        }
        for (String pair : query.split("&")) {
            int idx = pair.indexOf('=');
            if (idx <= 0) {
                continue;
            }
            params.put(URLDecoder.decode(pair.substring(0, idx), StandardCharsets.UTF_8),
                    URLDecoder.decode(pair.substring(idx + 1), StandardCharsets.UTF_8));
        }
        return params;
    }
}
