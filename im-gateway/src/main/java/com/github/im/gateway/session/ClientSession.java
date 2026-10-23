package com.github.im.gateway.session;

/**
 * 一条客户端长连接（传输无关）。
 *
 * <p>为什么要有这层抽象：WebSocket 与原生 TCP 的<b>连接语义完全一样</b>
 * （登记会话、心跳、上行转发、下行推送、摘流迁移），只有「字节怎么上线」不同。
 * 把它抽出来之后，{@link ConnectionManager}、空闲巡检、摘流、指标都不再依赖 Netty
 * 或 Spring 的具体类型，新增一种传输（QUIC/私有二进制）只需要再实现一个 ChannelInitializer。
 *
 * <p><b>线程约束</b>：{@link #send} 会被任意业务线程调用（IM 服务推下来的 gRPC 线程、
 * 摘流线程、巡检线程）。实现必须自行保证线程安全——Netty 的 {@code writeAndFlush}
 * 本身是线程安全的（转交给 event loop 串行执行），不要像 Spring 的 WebSocketSession
 * 那样在外面加锁，那会在慢客户端上把业务线程一起阻塞。
 */
public interface ClientSession {

    /** WebSocket 传输标识（指标/日志维度） */
    String TRANSPORT_WS = "websocket";
    /** 原生 TCP 传输标识 */
    String TRANSPORT_TCP = "tcp";

    /** 连接唯一 ID（日志/排障用，不是 userId） */
    String id();

    /** 绑定的 userId；TCP 在首帧 connect 之前为 null（未认证） */
    String userId();

    /** 设备标识，可空 */
    String deviceId();

    /** 传输类型：{@link #TRANSPORT_WS} / {@link #TRANSPORT_TCP} */
    String transport();

    /**
     * 连接是否打开
     * @return 连接打开/关闭
     */
    boolean isOpen();

    /**
     * 下发一帧（已序列化的 JSON）。
     *
     * @return 连接已关闭/写失败返回 false，由调用方转离线
     */
    boolean send(String payload);

    /** 服务端主动断连（摘流 1001 / 空闲超时 / 协议错误） */
    void close(String reason);
}
