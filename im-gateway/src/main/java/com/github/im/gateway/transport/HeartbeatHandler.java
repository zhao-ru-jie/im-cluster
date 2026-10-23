package com.github.im.gateway.transport;

import com.github.im.gateway.metrics.GatewayMetrics;
import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.session.NettyClientSession;
import com.github.im.gateway.session.SessionAttributes;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.util.concurrent.ScheduledFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * 连接级心跳：探测「客户端进程被杀 / 网络半开（NAT 超时、拔网线）」这类不会发 FIN 的死连接。
 *
 * <p>两段式（阈值来自 {@code im.gateway.idle.ping-after-ms} / {@code close-after-ms}，
 * 实际值由 {@code NettyGatewayServer} <b>按连接</b>叠加一个 [0, jitter-ms] 的随机偏移后传入，
 * 用来把「一批同时建立的连接」的心跳时刻摊开，避免周期性尖峰）：
 * <ol>
 *   <li><b>空闲达到随机化后的 ping 时刻</b>：下发应用层 {@code {"type":"ping"}}，并挂一个
 *       「close-after 之后仍无任何上行」的定时任务；</li>
 *   <li><b>定时任务到期</b>：仍没收到任何帧 → 判定死连接，关闭连接。
 *       关闭会触发 {@code channelInactive}，由 {@link SessionLifecycle} 清本地会话与 Redis 路由
 *       （用户转离线，消息落离线队列）。</li>
 * </ol>
 *
 * <p><b>为什么用 Netty 的 IdleStateHandler 而不是扫全表</b>：旧实现是 Spring 定时任务每 30s
 * 遍历所有连接比对时间戳，连接数上到十万级时每次扫描都要抢一次全局 map 的遍历开销，
 * 而且探测时机是「扫描批次」决定的，会有最多一个周期的抖动。现在每个连接由自己的 event loop
 * 计时，精确、无全局扫描、连接数增加不影响单次开销。
 *
 * <p><b>为什么用应用层 JSON ping 而不是 WebSocket 协议级 Ping 帧</b>：浏览器 JS 无法主动发送
 * 协议级 ping（API 不暴露），若只用协议级 ping，WS 客户端会被误判为死连接；而 TCP 根本没有
 * 协议级 ping。所以统一在业务帧层用 {@code {"type":"ping"}}，两种传输语义一致。
 *
 * <p><b>未认证连接</b>：握手/认证还没完成时客户端解析不了 ping，因此不发探测，
 * 第二次空闲直接断开（TCP 的 10s 认证窗口通常早就先把它关了）。
 */
public class HeartbeatHandler extends ChannelInboundHandlerAdapter {

    private static final Logger log = LoggerFactory.getLogger(HeartbeatHandler.class);

    private final ClientFrames frames;
    private final GatewayMetrics metrics;
    /** ping 发出后等待应答的时长（= close-after-ms - ping-after-ms） */
    private final long closeDelayMs;

    /** 本轮是否已发过 ping；收到任意上行帧即复位 */
    private boolean pinged;
    /** 未认证连接连续空闲次数 */
    private int idleWithoutAuth;
    private ScheduledFuture<?> closeTask;

    public HeartbeatHandler(ClientFrames frames, GatewayMetrics metrics, long closeDelayMs) {
        this.frames = frames;
        this.metrics = metrics;
        this.closeDelayMs = closeDelayMs;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        // 任何上行帧（含业务层的 pong / msg.send）都算「活着」，取消待执行的关闭
        pinged = false;
        idleWithoutAuth = 0;
        cancelCloseTask();
        ctx.fireChannelRead(msg);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (!(evt instanceof IdleStateEvent)) {
            ctx.fireUserEventTriggered(evt);
            return;
        }
        NettyClientSession session = ctx.channel().attr(SessionAttributes.SESSION).get();
        if (session == null) {
            // 还没认证：客户端此时解析不了 ping，攒够两次空闲直接断，防止半开连接常驻
            if (++idleWithoutAuth >= 2) {
                log.debug("close unauthenticated idle connection {}", ctx.channel().id().asShortText());
                ctx.close();
            }
            return;
        }
        if (pinged || closeTask != null) {
            // 已经探测过，等 closeTask 判定；这里不重复发 ping，避免死连接被刷屏
            return;
        }
        pinged = true;
        session.send(frames.ping());
        log.debug("heartbeat ping -> {}", session);
        // 定时关闭：到期前收到任何帧都会 cancelCloseTask
        closeTask = ctx.executor().schedule(() -> {
            if (pinged && ctx.channel().isActive()) {
                log.info("heartbeat timeout after {}ms, closing {}", closeDelayMs, session);
                metrics.sessionIdleClosed(1);
                session.close("heartbeat-timeout");
            }
        }, closeDelayMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        cancelCloseTask();
        ctx.fireChannelInactive();
    }

    private void cancelCloseTask() {
        if (closeTask != null) {
            closeTask.cancel(false);
            closeTask = null;
        }
    }
}
