package com.github.im.rpc.presence;

import com.github.im.contract.PresencePort;
import com.github.im.proto.presence.v1.GetPresenceRequest;
import com.github.im.proto.presence.v1.GetPresenceResponse;
import com.github.im.proto.presence.v1.PresenceIngressGrpc;
import com.github.im.proto.presence.v1.PresenceReport;
import com.github.im.proto.presence.v1.PresenceReportAck;
import com.github.im.proto.presence.v1.TypingRequest;
import com.github.im.proto.presence.v1.TypingResponse;
import com.github.im.rpc.common.GrpcChannelFactory;

import java.util.concurrent.TimeUnit;

/**
 * {@link PresencePort} 的远程实现：网关通过 gRPC 把状态上报给 IM 服务。
 *
 * <h2>为什么整体不打开 waitForReady</h2>
 *
 * <p>与 {@code RemoteMessageIngress} 的 {@code submit} 相反：那里等实例就绪是有价值的
 * （等 1 秒就能把消息发出去，而不是让客户端退避重发），而这里的三种请求都是
 * <b>只在「此刻」有意义</b>的状态——等两秒再送到的「正在输入」和「某人上线」已经过期了。
 *
 * <p>所以策略是「快发快丢」：超时/不可用时直接抛给调用方（网关的 {@code SessionLifecycle}），
 * 由它按 best-effort 静默处理（记 debug 日志，不给客户端错误帧）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RemotePresenceIngress implements PresencePort {

    private final GrpcChannelFactory channels;
    private final String target;
    private final long timeoutMs;

    /**
     * @param target gRPC target：可能是 {@code host:port}（单实例）或多地址 scheme
     *               （{@code ingress:///im-server}，与消息上行共用同一个 channel）
     */
    public RemotePresenceIngress(GrpcChannelFactory channels, String target, long timeoutMs) {
        this.channels = channels;
        this.target = target;
        this.timeoutMs = timeoutMs;
    }

    @Override
    public TypingResponse reportTyping(TypingRequest request) {
        return stub().reportTyping(request);
    }

    @Override
    public GetPresenceResponse presence(GetPresenceRequest request) {
        return stub().getPresence(request);
    }

    @Override
    public PresenceReportAck reportPresence(PresenceReport report) {
        return stub().reportPresence(report);
    }

    private PresenceIngressGrpc.PresenceIngressBlockingStub stub() {
        return PresenceIngressGrpc.newBlockingStub(channels.channelFor(target))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** 当前的 gRPC target（排障用） */
    public String target() {
        return target;
    }
}
