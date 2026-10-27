package com.github.im.client;

/**
 * WebSocket 握手被网关拒绝（400/401/404/503）。
 *
 * <p>为什么要把状态码单独带出来：{@code 401}（token 无效）与 {@code 400}（缺 userId）重连
 * 一万次也是同样结果，而 {@code 503}（实例正在摘流）等一会儿就能连上。
 * 上层只有拿到状态码才能区分「该退避重试」和「该停下来通知宿主」——
 * 统一当成「连接失败」无限重连，是把自己打进风控的标准姿势。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class WebSocketHandshakeException extends java.io.IOException {

    private final int status;

    public WebSocketHandshakeException(int status, String message, Throwable cause) {
        super(message, cause);
        this.status = status;
    }

    public int status() {
        return status;
    }
}
