package com.github.im.client;

import java.io.IOException;

/**
 * 一条到网关的「帧通道」：只管收发已编码好的 JSON 帧，不理解任何业务语义。
 *
 * <h2>为什么抽这一层</h2>
 *
 * <p>同一条 IM 有两个承载：原生 TCP（{@code 4 字节长度 + UTF-8 JSON}，首帧 {@code connect} 认证）
 * 和 WebSocket（{@code /ws?userId=..&deviceId=..}，身份在 HTTP 握手 query 里）。
 * <b>帧内容完全一致</b>，差别只在「怎么建连、怎么分帧、身份在哪声明」。
 * 把这三件事收进实现类，上层的重连/重投/去重逻辑一行都不用改，也不用为两种承载写两份状态机。
 *
 * <h2>{@link Listener#onReady} 的意义</h2>
 *
 * <p>TCP 建连成功不等于认证成功：服务端要收到 {@code connect} 帧并回 {@code connected}，
 * 所以 TCP 通道<b>从不</b>触发 {@code onReady}，由上层等待 {@code connected} 帧；
 * 而 WebSocket 握手成功即认证成功（userId/token 在 query 里），所以它直接触发 {@code onReady}。
 * 上层把「TCP 的 connected 帧」和「WS 的 onReady」汇成同一个 {@code handleAuthenticated}，
 * 这就是两种承载唯一需要对齐的地方。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
interface FrameChannel extends AutoCloseable {

    /**
     * 写一帧。
     *
     * <p>{@code 4 字节长度}与正文是两段写，所以实现必须内部串行化；
     * 写失败即认为通道已坏，调用方收到 {@link IOException} 后应当 {@link #close()}。
     */
    void send(String frame) throws IOException;

    @Override
    void close();

    /** 远端描述（日志用） */
    String remote();

    boolean isClosed();

    interface Listener {

        /** 通道已就绪且身份已确认（仅 WebSocket：TCP 靠 {@code connected} 帧） */
        void onReady(FrameChannel channel);

        void onFrame(FrameChannel channel, String payload);

        /** 通道关闭/握手失败。{@code cause} 可能是 {@link WebSocketHandshakeException} */
        void onClosed(FrameChannel channel, Throwable cause);
    }
}
