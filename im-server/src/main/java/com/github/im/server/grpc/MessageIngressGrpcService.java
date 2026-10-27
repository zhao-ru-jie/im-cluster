package com.github.im.server.grpc;

import com.github.im.proto.ingress.v1.AckPendingRequest;
import com.github.im.proto.ingress.v1.AckPendingResponse;
import com.github.im.proto.ingress.v1.BatchSubmitRequest;
import com.github.im.proto.ingress.v1.BatchSubmitResponse;
import com.github.im.proto.ingress.v1.DeliveredAckRequest;
import com.github.im.proto.ingress.v1.DeliveredAckResponse;
import com.github.im.proto.ingress.v1.ListConversationsRequest;
import com.github.im.proto.ingress.v1.ListConversationsResponse;
import com.github.im.proto.ingress.v1.MarkReadRequest;
import com.github.im.proto.ingress.v1.MarkReadResponse;
import com.github.im.proto.ingress.v1.MessageIngressGrpc;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.message.MessageService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.grpc.server.service.GrpcService;

import java.util.function.Supplier;

/**
 * IM 服务对网关暴露的上行 gRPC 入口。
 *
 * <p>阶段 1（单体进程）网关直接调用本地实现，本服务已注册但不承担流量；
 * 阶段 2 把网关切到 {@code RemoteMessageIngress} 后即可承载真实流量，业务代码零改动。
 * 先落地的好处是：契约、序列化、超时/错误码语义在阶段 1 就能被 Shadow 校验覆盖。
 *
 * <p>依赖注入刻意用具体类型 {@link MessageService} 而不是 {@code MessageIngress} 接口：
 * 服务端永远只把请求交给本地业务实现，绝不可能是「客户端适配器」，
 * 这样单体装配下也不会因为同类型多 bean 而歧义。
 *
 * <h2>为什么每个方法都要自己 try/catch</h2>
 *
 * <p>不处理的话，业务侧抛出的 RuntimeException 会被 gRPC 统一转成 {@code UNKNOWN}
 * （"Application error processing RPC"）：<b>堆栈只留在框架内部的日志里，业务异常的那几行
 * 完全消失</b>，网关侧只能看到「UNKNOWN」，排查时两端都没有可用的线索。
 * 所以这里统一做两件事：先按业务语义记一条带堆栈的日志，再把状态码映射成
 * 「参数错 → INVALID_ARGUMENT」「服务端故障 → INTERNAL」，让网关能区分该重试还是该改参数。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@GrpcService
public class MessageIngressGrpcService extends MessageIngressGrpc.MessageIngressImplBase {

    private static final Logger log = LoggerFactory.getLogger(MessageIngressGrpcService.class);

    private final MessageService messageIngress;

    public MessageIngressGrpcService(MessageService messageIngress) {
        this.messageIngress = messageIngress;
    }

    @Override
    public void submitMessage(SubmitMessageRequest request,
                              StreamObserver<SubmitMessageResponse> responseObserver) {
        handle("submitMessage", request.getTraceId(), responseObserver,
                () -> messageIngress.submit(request));
    }

    @Override
    public void batchSubmit(BatchSubmitRequest request,
                            StreamObserver<BatchSubmitResponse> responseObserver) {
        // 批量请求本身没有 traceId，取第一条的：与单条路径的日志口径保持一致
        String traceId = request.getMessagesCount() > 0 ? request.getMessages(0).getTraceId() : "";
        handle("batchSubmit(" + request.getMessagesCount() + ")", traceId, responseObserver,
                () -> messageIngress.submitBatch(request));
    }

    @Override
    public void pullPending(PullPendingRequest request,
                            StreamObserver<PullPendingResponse> responseObserver) {
        handle("pullPending", request.getUserId(), responseObserver,
                () -> messageIngress.pullPending(request));
    }

    @Override
    public void ackPending(AckPendingRequest request,
                           StreamObserver<AckPendingResponse> responseObserver) {
        handle("ackPending", request.getUserId(), responseObserver,
                () -> messageIngress.ackPending(request));
    }

    @Override
    public void ackDelivered(DeliveredAckRequest request,
                             StreamObserver<DeliveredAckResponse> responseObserver) {
        handle("ackDelivered", request.getUserId(), responseObserver,
                () -> messageIngress.ackDelivered(request));
    }

    @Override
    public void listConversations(ListConversationsRequest request,
                                  StreamObserver<ListConversationsResponse> responseObserver) {
        handle("listConversations", request.getUserId(), responseObserver,
                () -> messageIngress.listConversations(request));
    }

    @Override
    public void markRead(MarkReadRequest request,
                         StreamObserver<MarkReadResponse> responseObserver) {
        handle("markRead", request.getUserId(), responseObserver,
                () -> messageIngress.markRead(request));
    }

    /**
     * 统一执行 + 异常上报。
     *
     * @param rpc     方法名（日志/状态描述用）
     * @param subject 关键入参（submit 系是 traceId，其余是 userId）：两端日志靠它对齐同一条请求
     */
    private <T> void handle(String rpc, String subject, StreamObserver<T> observer, Supplier<T> call) {
        try {
            observer.onNext(call.get());
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            // 参数错是调用方的问题，映射成 INVALID_ARGUMENT：网关据此不该盲目重试
            log.warn("[ingress] {} 参数不合法 subject={} err={}", rpc, subject, e.getMessage());
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription(rpc + ": " + e.getMessage())
                    .asRuntimeException());
        } catch (RuntimeException e) {
            // 服务端故障：必须自己记堆栈，否则只剩一个 UNKNOWN 状态，业务堆栈彻底丢失
            log.error("[ingress] {} 处理失败 subject={} err={}", rpc, subject, e.getMessage(), e);
            observer.onError(Status.INTERNAL
                    .withDescription(rpc + " failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
