package com.github.im.rpc.ingress;

import com.github.im.contract.MessageIngress;
import com.github.im.proto.ingress.v1.AckPendingRequest;
import com.github.im.proto.ingress.v1.AckPendingResponse;
import com.github.im.proto.ingress.v1.BatchSubmitRequest;
import com.github.im.proto.ingress.v1.BatchSubmitResponse;
import com.github.im.proto.ingress.v1.MessageIngressGrpc;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.rpc.common.GrpcChannelFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * {@link MessageIngress} 的远程实现：网关通过 gRPC 调 IM 服务。
 *
 * <p><b>多实例分摊交给 gRPC 自己做</b>：{@code target} 是一个「能被解析出多个地址」的 URI
 * （如 {@code ingress:///im-server}，由服务发现侧的 {@code NameResolver} 提供地址列表），
 * channel 上配了 {@code round_robin} 策略——轮询、以及「连不上的 subchannel 自动摘掉、
 * 恢复后自动放回」都是 gRPC 内建行为，本类不维护地址表、不选地址。
 *
 * <p>为什么不在应用层手写轮询：取址如果放在「每次 RPC」这一层，全局计数器的周期性会和
 * 「每个业务请求的 RPC 次数」产生别名（2 实例 + 每条消息 2 次 RPC 时，submit 会永远落到同一个
 * 实例）。gRPC 的 LoadBalancer 工作在 subchannel 层，不存在这个问题。
 *
 * <p><b>超时语义</b>：默认 300ms。超时/不可用时向上抛异常，由网关的
 * {@code SessionLifecycle} 转成 {@code error 503}（可重试）——连接保持不断，
 * 因为「连接」比「单条消息的实时性」更稀缺。
 *
 * <p><b>waitForReady 语义</b>：{@code withWaitForReady()} 让 RPC 在「channel 暂时没有任何 READY 的
 * subchannel」时<b>排队等待</b>，而不是立刻返回 UNAVAILABLE——例如所有实例都还在 CONNECTING，
 * 或全部实例短暂 NOT_SERVING。最坏情况仍会以 DEADLINE_EXCEEDED 结束（占满 deadline）。
 *
 * <p>为什么上行默认打开它：网关对失败的反应本来就是回 503 让客户端退避重发，能等到就不必让客户端
 * 多退避一秒以上；而且网关跑的是虚拟线程，等待几乎不占平台线程。下行（{@code RemoteGatewayPushPort}）
 * 默认关闭：那里有「失败即落离线」的确定性兜底，让 IM 服务把线程挂在等待上没有收益。
 *
 * <p><b>它不会掩盖「部分实例不可用」</b>：只要有任意一个 subchannel READY，channel 就是 READY，
 * waitForReady 根本不介入——那种情况由 round_robin（只挑 READY）加健康检查（排除 NOT_SERVING）处理。
 *
 * <p><b>重试语义</b>：本类<b>不做</b>重试。接口本身幂等，重试由上层决定（客户端退避重发），
 * 这样避免「网关重试 + 客户端重试」叠加把 IM 服务打穿。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class RemoteMessageIngress implements MessageIngress {

    private final GrpcChannelFactory channels;
    private final String target;
    private final long timeoutMs;
    private final boolean waitForReady;

    /**
     * @param target       gRPC target：可以是 {@code host:port}（单实例）或多地址 scheme
     *                     （如 {@code ingress:///im-server}，走 round_robin）
     * @param waitForReady 是否用 {@code withWaitForReady()}，见类注释的「waitForReady 语义」
     */
    public RemoteMessageIngress(GrpcChannelFactory channels, String target, long timeoutMs,
                                boolean waitForReady) {
        this.channels = channels;
        this.target = target;
        this.timeoutMs = timeoutMs;
        this.waitForReady = waitForReady;
    }

    @Override
    public SubmitMessageResponse submit(SubmitMessageRequest request) {
        return invoke(stub -> stub.submitMessage(request));
    }

    @Override
    public BatchSubmitResponse submitBatch(BatchSubmitRequest request) {
        return invoke(stub -> stub.batchSubmit(request));
    }

    @Override
    public PullPendingResponse pullPending(PullPendingRequest request) {
        return invoke(stub -> stub.pullPending(request));
    }

    /**
     * 确认一批补发帧已写进连接。
     *
     * <p>与 {@link #submit} / {@link #pullPending} 的差别：<b>不走 waitForReady</b>。
     * 确认失败最坏的结果是「下次重连把同一批消息再发一遍」，客户端按 seq 去重即可；
     * 而 waitForReady 会让一个没有意义的确认占住虚拟线程直到 deadline，把补发路径一起拖慢。
     */
    @Override
    public AckPendingResponse ackPending(AckPendingRequest request) {
        return MessageIngressGrpc.newBlockingStub(channels.channelFor(target))
                .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS)
                .ackPending(request);
    }

    private <T> T invoke(Function<MessageIngressGrpc.MessageIngressBlockingStub, T> call) {
        MessageIngressGrpc.MessageIngressBlockingStub stub =
                MessageIngressGrpc.newBlockingStub(channels.channelFor(target))
                        .withDeadlineAfter(timeoutMs, TimeUnit.MILLISECONDS);
        if (waitForReady) {
            stub = stub.withWaitForReady();
        }
        return call.apply(stub);
    }

    /** 当前的 gRPC target（排障用：日志里能看出走的是单地址还是服务发现） */
    public String target() {
        return target;
    }
}
