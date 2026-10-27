package com.github.im.client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 原生 TCP 承载：{@code 4 字节大端长度 + UTF-8 JSON}，首帧 {@code connect} 认证。
 *
 * <h2>为什么读循环跑在虚拟线程上</h2>
 *
 * <p>阻塞式 socket 读在没有数据时会挂住一整个平台线程。虚拟线程挂住几乎不占资源，
 * 因此「一个客户端 = 一条虚拟线程」在压测/集成测试里同时开几千个客户端也不会有问题。
 *
 * <h2>为什么写要加锁、读不用</h2>
 *
 * <p>读只有这一个线程；写会被「发送线程、重投定时器、心跳、recv 上报」同时调用，
 * 而长度前缀是<b>两段写</b>（长度 + 正文）。两个线程交错写会把字节流写坏。
 *
 * <h2>认证</h2>
 *
 * <p>本通道<b>不</b>触发 {@link Listener#onReady}：TCP 没有 HTTP 握手，
 * 建连成功后还要发 {@code connect} 帧、等服务端回 {@code connected}，那一步由上层负责。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
final class TcpFrameChannel implements FrameChannel {

    private static final System.Logger log = System.getLogger(TcpFrameChannel.class.getName());

    private final Socket socket;
    private final InputStream in;
    private final OutputStream out;
    private final String remote;
    private final int maxFrameBytes;
    private final Listener listener;
    private final AtomicBoolean closed = new AtomicBoolean();

    private TcpFrameChannel(Socket socket, int maxFrameBytes, Listener listener) throws IOException {
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.remote = String.valueOf(socket.getRemoteSocketAddress());
        this.maxFrameBytes = maxFrameBytes;
        this.listener = listener;
    }

    /**
     * 建连并开始读循环。
     *
     * <p>{@code TCP_NODELAY} 必须开：心跳 ping/pong 是几十字节的小包，
     * Nagle 算法会为了凑够 MSS 把它压 40ms——比心跳周期本身还显眼。
     */
    static TcpFrameChannel open(String host, int port, int connectTimeoutMs, int maxFrameBytes,
                                Listener listener) throws IOException {
        Socket socket = new Socket();
        try {
            socket.setTcpNoDelay(true);
            socket.setKeepAlive(true);
            socket.connect(new InetSocketAddress(host, port), connectTimeoutMs);
            TcpFrameChannel channel = new TcpFrameChannel(socket, maxFrameBytes, listener);
            channel.startReader();
            return channel;
        } catch (IOException | RuntimeException e) {
            closeQuietly(socket);
            throw e;
        }
    }

    private void startReader() {
        Thread.ofVirtual().name("im-client-reader-" + remote).start(this::readLoop);
    }

    private void readLoop() {
        Throwable cause = null;
        try {
            while (!closed.get()) {
                String frame = LengthPrefixedCodec.read(in, maxFrameBytes);
                if (frame == null) {
                    cause = new IOException("网关关闭了连接");
                    break;
                }
                if (frame.isEmpty()) {
                    continue;
                }
                try {
                    listener.onFrame(this, frame);
                } catch (RuntimeException e) {
                    // 单帧处理出错不该断连接：可能是某个新帧的字段形状变了，
                    // 记下来继续读，比整条连接崩掉划算
                    log.log(System.Logger.Level.WARNING, "[client] 处理下行帧失败：" + e.getMessage(), e);
                }
            }
        } catch (IOException e) {
            cause = e;
        } finally {
            close();
            listener.onClosed(this, cause == null ? new IOException("连接已关闭") : cause);
        }
    }

    @Override
    public void send(String frame) throws IOException {
        if (closed.get()) {
            throw new IOException("连接已关闭");
        }
        synchronized (this) {
            LengthPrefixedCodec.write(out, frame);
        }
    }

    @Override
    public boolean isClosed() {
        return closed.get();
    }

    @Override
    public String remote() {
        return remote;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            closeQuietly(socket);
        }
    }

    private static void closeQuietly(Socket socket) {
        try {
            socket.close();
        } catch (IOException ignored) {
            // 关闭失败没有补救手段，也不影响上层状态机
        }
    }
}
