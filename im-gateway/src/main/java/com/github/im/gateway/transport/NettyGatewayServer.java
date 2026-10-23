package com.github.im.gateway.transport;

import com.github.im.gateway.drain.GatewayDrainService;
import com.github.im.gateway.metrics.GatewayMetrics;
import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.transport.tcp.TcpFrameInboundHandler;
import com.github.im.gateway.transport.websocket.StringToWebSocketFrameEncoder;
import com.github.im.gateway.transport.websocket.WebSocketFrameInboundHandler;
import com.github.im.gateway.transport.websocket.WebSocketHandshakeGate;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollIoHandler;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Netty 长连接服务：<b>同一个进程内同时</b>提供 WebSocket 与原生 TCP 两种接入。
 *
 * <p>为什么从 Spring WebSocket（Tomcat）换成 Netty：
 * <ul>
 *   <li><b>连接规模</b>：Tomcat 一条连接一个线程模型（即便开虚拟线程，读写仍走 Servlet 栈），
 *       十万级空闲连接的内存与上下文切换成本明显；Netty 是少量 event loop 轮转，
 *       空闲连接几乎零成本——这正是「连接数」作为 HPA 主指标时的关键；</li>
 *   <li><b>协议统一</b>：TCP 和 WebSocket 用同一套 ServerBootstrap 与同一份生命周期逻辑，
 *       不需要为原生 TCP 再引入一个服务端框架；</li>
 *   <li><b>写不再加锁</b>：Spring 的 {@code WebSocketSession} 非线程安全，必须
 *       {@code synchronized(session)}；Netty 的 {@code writeAndFlush} 由 event loop 串行执行，
 *       慢客户端不会阻塞业务线程。</li>
 * </ul>
 *
 * <p>两类 pipeline：
 * <pre>
 * WebSocket: HttpServerCodec -> HttpObjectAggregator -> WebSocketHandshakeGate(摘流/鉴权/路径)
 *            -> IdleStateHandler(计时，需在协议处理器之前才能看见协议级 Ping)
 *            -> WebSocketServerProtocolHandler -> StringToWebSocketFrameEncoder
 *            -> HeartbeatHandler(策略，需在编码器之后) -> FrameInboundHandler
 *
 * TCP      : IdleStateHandler(按原始字节计时) -> LengthFieldBasedFrameDecoder(4B 长度)
 *            -> LengthFieldPrepender(4B) -> StringDecoder/Encoder(UTF-8)
 *            -> HeartbeatHandler -> TcpFrameInboundHandler(首帧 connect 认证)
 * </pre>
 * 两条 pipeline 的最后一棒都是 {@link SessionLifecycle}，因此「心跳、上行转发、掉线清路由」
 * 只有一份实现，不会出现某种传输特有的幽灵 bug。
 *
 * <p><b>两个端口而不是一个</b>：同样的能力也可以用一个端口做协议嗅探（首字节是 "GET " 就当
 * HTTP 升级）来合并，但那会让 LB/防火墙/健康检查无法按传输分流，排障时也不好定位。
 * 这里选择显式双端口，运维语义更清楚。
 */
@Component
public class NettyGatewayServer {

    private static final Logger log = LoggerFactory.getLogger(NettyGatewayServer.class);

    /** 握手请求（HTTP 头）最大长度，与业务帧无关 */
    private static final int MAX_HTTP_HANDSHAKE_BYTES = 64 * 1024;

    /** ping 之后至少要等的关闭判定窗口，避免 close-after ≤ ping-after 配错时立刻掐连接 */
    private static final long MIN_CLOSE_DELAY_MS = 1_000L;

    /**
     * 常见中间设备的空闲超时下限（云 LB / Nginx 的 {@code proxy_read_timeout} 默认 60s 最常见）。
     * 心跳间隔必须小于链路上最短的那个 idle timeout，否则连接会先被中间设备掐断——
     * 客户端只看到「莫名掉线 + 重连」，很难定位。
     */
    private static final long TYPICAL_LB_IDLE_TIMEOUT_MS = 60_000L;

    private final SessionLifecycle lifecycle;
    private final GatewayDrainService drainService;
    private final ClientFrames frames;
    private final GatewayMetrics metrics;

    /** 心跳：每个连接由自己的 event loop 计时，不做全局扫描 */
    private final boolean heartbeatEnabled;
    private final long pingAfterMs;
    private final long closeAfterMs;
    /** 抖动上限：单条连接的 ping 时刻在 [pingAfter, pingAfter+jitter] 之间随机 */
    private final long jitterMs;
    private final boolean wsEnabled;
    private final int wsPort;
    private final String wsPath;
    private final boolean tcpEnabled;
    private final int tcpPort;
    private final int maxFrameBytes;
    private final long tcpAuthTimeoutMs;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private final List<Channel> serverChannels = new ArrayList<>();

    public NettyGatewayServer(SessionLifecycle lifecycle,
                              GatewayDrainService drainService,
                              ClientFrames frames,
                              GatewayMetrics metrics,
                              @Value("${im.gateway.transport.ws-enabled:true}") boolean wsEnabled,
                              @Value("${im.gateway.transport.ws-port:8081}") int wsPort,
                              @Value("${im.gateway.transport.ws-path:/ws}") String wsPath,
                              @Value("${im.gateway.transport.tcp-enabled:true}") boolean tcpEnabled,
                              @Value("${im.gateway.transport.tcp-port:9092}") int tcpPort,
                              @Value("${im.gateway.transport.max-frame-bytes:1048576}") int maxFrameBytes,
                              @Value("${im.gateway.transport.tcp-auth-timeout-ms:10000}") long tcpAuthTimeoutMs,
                              @Value("${im.gateway.idle.enabled:true}") boolean heartbeatEnabled,
                              @Value("${im.gateway.idle.ping-after-ms:45000}") long pingAfterMs,
                              @Value("${im.gateway.idle.close-after-ms:90000}") long closeAfterMs,
                              @Value("${im.gateway.idle.jitter-ms:5000}") long jitterMs) {
        this.lifecycle = lifecycle;
        this.drainService = drainService;
        this.frames = frames;
        this.metrics = metrics;
        this.wsEnabled = wsEnabled;
        this.wsPort = wsPort;
        this.wsPath = wsPath;
        this.tcpEnabled = tcpEnabled;
        this.tcpPort = tcpPort;
        this.maxFrameBytes = maxFrameBytes;
        this.tcpAuthTimeoutMs = tcpAuthTimeoutMs;
        this.heartbeatEnabled = heartbeatEnabled;
        this.pingAfterMs = pingAfterMs;
        this.closeAfterMs = closeAfterMs;
        this.jitterMs = Math.max(jitterMs, 0L);
    }

    @PostConstruct
    public void start() {
        if (!wsEnabled && !tcpEnabled) {
            log.warn("[netty] WebSocket 与 TCP 都已禁用，网关不会接受任何长连接");
            return;
        }
        boolean isLinux = isLinux();
        bossGroup = new MultiThreadIoEventLoopGroup(1, isLinux ? EpollIoHandler.newFactory() : NioIoHandler.newFactory());
        // 0 = 默认 2 * CPU 核数；长连接场景下 event loop 只做编解码与转发，足够
        workerGroup = new MultiThreadIoEventLoopGroup(0, isLinux ? EpollIoHandler.newFactory() : NioIoHandler.newFactory());

        if (heartbeatEnabled) {
            log.info("[netty] 心跳已启用：{}~{}ms 无上行发 ping（每连接随机抖动 ≤{}ms），"
                    + "再按 close-after={}ms 判定超时断开", pingAfterMs, pingAfterMs + jitterMs, jitterMs, closeAfterMs);
            if (pingAfterMs >= TYPICAL_LB_IDLE_TIMEOUT_MS) {
                // 不做硬校验（各环境 LB 配置不同），但这类配置几乎一定会被中间设备先掐断，必须提示
                log.warn("[netty] ping-after-ms={}ms 不小于常见 LB 空闲超时 {}ms，连接可能被中间设备先断开；"
                        + "建议 ping-after-ms 配到该值的一半左右", pingAfterMs, TYPICAL_LB_IDLE_TIMEOUT_MS);
            }
        } else {
            log.warn("[netty] 心跳已禁用（im.gateway.idle.enabled=false），死连接只能靠 TCP keepalive 兜底");
        }

        if (wsEnabled) {
            serverChannels.add(bind("websocket", wsPort, wsInitializer()));
        }
        if (tcpEnabled) {
            serverChannels.add(bind("tcp", tcpPort, tcpInitializer()));
        }
    }

    @PreDestroy
    public void stop() {
        for (Channel channel : serverChannels) {
            channel.close();
        }
        serverChannels.clear();
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS);
        }
        log.info("[netty] 长连接服务已停止");
    }

    /**
     * 判断是否是Linux系统
     */
    private boolean isLinux() {
        return System.getProperty("os.name").toLowerCase().contains("linux") || Epoll.isAvailable();
    }

    /** 暴露给 /admin/status，便于确认当前实例到底监听在哪几个端口 */
    public Map<String, Object> describe() {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("websocket", wsEnabled ? ":" + wsPort + wsPath : "disabled");
        info.put("tcp", tcpEnabled ? ":" + tcpPort : "disabled");
        return info;
    }

    private Channel bind(String name, int port, ChannelInitializer<SocketChannel> initializer) {
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .option(ChannelOption.SO_BACKLOG, 1024)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.SO_KEEPALIVE, true)
                .childHandler(initializer);
        try {
            Channel channel = bootstrap.bind(port).sync().channel();
            log.info("[netty] {} 接入已启动，端口 {}", name, port);
            return channel;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("绑定 " + name + " 端口 " + port + " 被中断", e);
        } catch (RuntimeException e) {
            // 端口占用必须启动即失败：静默降级成「只有一种传输」会让线上流量莫名其妙连不上
            throw new IllegalStateException("绑定 " + name + " 端口 " + port + " 失败: " + e.getMessage(), e);
        }
    }

    /** 单条连接的心跳时序（抖动后每连接不同） */
    private record HeartbeatTiming(long readerIdleMs, long closeDelayMs) {
    }

    /**
     * 为<b>这一条连接</b>随机生成心跳时序。
     *
     * <p>为什么需要抖动：如果几千条连接在同一时刻建立（发版后集中重连就是典型场景），
     * 它们的 {@code IdleStateHandler} 会在同一毫秒一起到期，于是所有 ping 挤在同一个瞬间发出，
     * 形成周期性尖峰（心跳风暴）。给每条连接的 readerIdle 加一个 [0, jitterMs] 的随机偏移，
     * 就把这批 ping 摊到一个窗口里——每连接的「相位」不同，且这个相位在后续读写中保持不变。
     *
     * <p>关闭窗口用 {@code closeAfter - readerIdle} 反算，而不是固定值：
     * 这样「抖动只影响 ping 时刻，不影响判定总时长」，超时总窗口仍然 ≈ closeAfter。
     */
    private HeartbeatTiming heartbeatTiming() {
        long jitter = jitterMs > 0 ? ThreadLocalRandom.current().nextLong(jitterMs + 1) : 0L;
        long readerIdle = pingAfterMs + jitter;
        return new HeartbeatTiming(readerIdle, Math.max(closeAfterMs - readerIdle, MIN_CLOSE_DELAY_MS));
    }

    /**
     * 空闲计时器（纯计时，不做策略）。
     *
     * <p>必须放在「所有入站数据都会经过」的位置——{@link IdleStateHandler} 只统计经过它这一层的读取：
     * WebSocket 的协议级 Ping 帧会被 {@code WebSocketServerProtocolHandler} 吃掉不再往后传，
     * 若把计时器放在协议处理器之后，只用协议级 ping 的原生客户端就会被误判为死连接。
     *
     * <p>readerIdleTime 只看「客户端有没有发东西进来」，服务端自己写出的 ping 不会重置计时。
     */
    private void addIdleTimer(ChannelPipeline pipeline, String prefix, HeartbeatTiming timing) {
        if (timing == null) {
            return;
        }
        pipeline.addLast(prefix + "-idle", new IdleStateHandler(timing.readerIdleMs(), 0, 0, TimeUnit.MILLISECONDS));
    }

    /**
     * 心跳策略（两种传输共用 {@link HeartbeatHandler}）。
     *
     * <p>必须放在出站编码器之后：它要往客户端写 ping，WS 需要经过文本帧编码器、
     * TCP 需要经过长度前缀编码器，否则裸 String 会一路漏到 HTTP / 原始字节层报错。
     */
    private void addHeartbeatHandler(ChannelPipeline pipeline, String prefix, HeartbeatTiming timing) {
        if (timing == null) {
            return;
        }
        pipeline.addLast(prefix + "-heartbeat", new HeartbeatHandler(frames, metrics, timing.closeDelayMs()));
    }

    private ChannelInitializer<SocketChannel> wsInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                // 每条连接单独抽一次抖动值：这样一批同时建立的连接不会在同一毫秒发 ping
                HeartbeatTiming heartbeat = heartbeatEnabled ? heartbeatTiming() : null;
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("http-codec", new HttpServerCodec());
                pipeline.addLast("http-aggregator", new HttpObjectAggregator(MAX_HTTP_HANDSHAKE_BYTES));
                // 必须在协议处理器之前：握手一旦完成就没法再回 503/400 了
                pipeline.addLast("ws-handshake-gate", new WebSocketHandshakeGate(drainService, wsPath));
                // 空闲计时放在协议处理器之前：协议级 WebSocket Ping 也算「活着」
                addIdleTimer(pipeline, "ws", heartbeat);
                pipeline.addLast("ws-protocol",
                        new WebSocketServerProtocolHandler(wsPath, null, true, maxFrameBytes));
                pipeline.addLast("ws-encoder", new StringToWebSocketFrameEncoder());
                addHeartbeatHandler(pipeline, "ws", heartbeat);
                pipeline.addLast("ws-handler", new WebSocketFrameInboundHandler(lifecycle));
            }
        };
    }

    private ChannelInitializer<SocketChannel> tcpInitializer() {
        return new ChannelInitializer<>() {
            @Override
            protected void initChannel(SocketChannel ch) {
                HeartbeatTiming heartbeat = heartbeatEnabled ? heartbeatTiming() : null;
                ChannelPipeline pipeline = ch.pipeline();
                // 放在最前：按原始字节判定活跃，客户端发半帧也会重置空闲计时（不会误踢慢连接）
                addIdleTimer(pipeline, "tcp", heartbeat);
                // 帧格式：4 字节大端长度 + UTF-8 JSON；解码后首尾同一条 String 管线，避免粘包
                pipeline.addLast("tcp-frame-decoder",
                        new LengthFieldBasedFrameDecoder(maxFrameBytes, 0, 4, 0, 4));
                pipeline.addLast("tcp-frame-prepender", new LengthFieldPrepender(4));
                pipeline.addLast("tcp-string-decoder", new StringDecoder(StandardCharsets.UTF_8));
                pipeline.addLast("tcp-string-encoder", new StringEncoder(StandardCharsets.UTF_8));
                addHeartbeatHandler(pipeline, "tcp", heartbeat);
                pipeline.addLast("tcp-handler", new TcpFrameInboundHandler(lifecycle, frames, tcpAuthTimeoutMs));
            }
        };
    }
}
