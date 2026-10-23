package com.github.im.serverboot.discovery;

import com.github.im.proto.ingress.v1.MessageIngressGrpc;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.rpc.common.GrpcChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * 上行入口的自测预热：注册到注册中心之前，先用一个真实 gRPC 连接把自己的
 * {@code MessageIngress} 打热。
 *
 * <h2>为什么需要它（实测数据）</h2>
 *
 * <p>一个<b>刚启动</b>的 im-server，它的第一条上行请求要 <b>0.8~1.2 秒</b>：
 * <ul>
 *   <li>~350ms：首条连接的 TCP + HTTP/2 握手（Netty pipeline 初始化）；</li>
 *   <li>400ms+：首次请求处理（protobuf/Jackson 类加载、Redis 客户端初始化、连接池）；</li>
 * </ul>
 * 而网关的 RPC deadline 只有 300ms，所以「网关先起、im-server 后起」时，
 * 用户在这个窗口里发的消息会稳定地拿到 503，过一会儿（类加载完了）又恢复正常——
 * 表现为「等一会才可以」。
 *
 * <p>把这段开销挪到「注册之前」，网关就只会看到一个已经能快速响应的实例。
 * 这里必须走<b>真实的本机 gRPC 连接</b>而不是进程内直调 {@code MessageService}：
 * 要热的正是 Netty/H2 这条链路，而进程内调用不会碰它。
 *
 * <h2>失败不影响启动</h2>
 *
 * <p>自测失败只意味着「首条消息可能慢」，不该让实例起不来，所以全部异常都吞掉并打日志。
 */
@Component
public class IngressSelfWarmup {

    private static final Logger log = LoggerFactory.getLogger(IngressSelfWarmup.class);

    /** 预热调用次数：够触发 JIT 与各条懒加载路径，又不会让启动明显变慢 */
    private static final int CALLS = 20;
    /** 单次预热调用的超时：冷启动时首条请求本身就慢，给足余量 */
    private static final long CALL_TIMEOUT_MS = 2_000L;
    /** 预热的「用户」：不存在，且与真实 userId 不可能碰撞，避免污染别人的待补发列表 */
    private static final String WARMUP_USER = "__warmup__";

    private final GrpcChannelFactory channels;
    private final int grpcPort;

    public IngressSelfWarmup(GrpcChannelFactory channels,
                             @Value("${im.node.grpc-port:9091}") int grpcPort) {
        this.channels = channels;
        this.grpcPort = grpcPort;
    }

    /**
     * 连自己打 {@link #CALLS} 次 {@code PullPending}（只读，不写业务数据）。
     *
     * <p>选 {@code PullPending} 而不是 {@code SubmitMessage}：它覆盖 gRPC 链路、编解码、
     * 服务分发、Redis 访问，但不会往消息表里写垃圾数据。
     *
     * @return 最慢一次调用的耗时（毫秒）；预热失败返回 -1
     */
    public long warmUp() {
        String target = "127.0.0.1:" + grpcPort;
        try {
            // 先等 channel READY：这一步本身就吃掉了首条连接的握手开销
            channels.warmUp(target);
            MessageIngressGrpc.MessageIngressBlockingStub stub =
                    MessageIngressGrpc.newBlockingStub(channels.channelFor(target))
                            .withDeadlineAfter(CALL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            PullPendingRequest request = PullPendingRequest.newBuilder()
                    .setUserId(WARMUP_USER)
                    .setMax(1)
                    .build();
            long worst = 0L;
            int failed = 0;
            for (int i = 0; i < CALLS; i++) {
                long start = System.nanoTime();
                try {
                    stub.pullPending(request);
                } catch (Exception e) {
                    failed++;
                    log.debug("[warmup] 第 {} 次自测失败: {}", i + 1, e.getMessage());
                }
                worst = Math.max(worst, TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
            }
            log.info("[warmup] 上行入口自测 {} 次完成（失败 {} 次），最慢 {}ms —— 冷启动开销已在注册前付清",
                    CALLS, failed, worst);
            return worst;
        } catch (Exception e) {
            // 不影响启动：最坏情况是恢复成改造前那样「首条消息可能 503」
            log.warn("[warmup] 上行入口自测失败，首条消息可能超时: {}", e.getMessage());
            return -1L;
        }
    }
}
