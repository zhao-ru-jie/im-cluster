package com.github.im.server.grpc;

import com.github.im.proto.ingress.v1.AckPendingRequest;
import com.github.im.proto.ingress.v1.AckPendingResponse;
import com.github.im.proto.ingress.v1.BatchSubmitRequest;
import com.github.im.proto.ingress.v1.BatchSubmitResponse;
import com.github.im.proto.ingress.v1.MessageIngressGrpc;
import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.message.MessageService;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

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
 * @author zhaorujie
 * @since 1.0.0
 */
@GrpcService
public class MessageIngressGrpcService extends MessageIngressGrpc.MessageIngressImplBase {

    private final MessageService messageIngress;

    public MessageIngressGrpcService(MessageService messageIngress) {
        this.messageIngress = messageIngress;
    }

    @Override
    public void submitMessage(SubmitMessageRequest request,
                              StreamObserver<SubmitMessageResponse> responseObserver) {
        responseObserver.onNext(messageIngress.submit(request));
        responseObserver.onCompleted();
    }

    @Override
    public void batchSubmit(BatchSubmitRequest request,
                            StreamObserver<BatchSubmitResponse> responseObserver) {
        responseObserver.onNext(messageIngress.submitBatch(request));
        responseObserver.onCompleted();
    }

    @Override
    public void pullPending(PullPendingRequest request,
                            StreamObserver<PullPendingResponse> responseObserver) {
        responseObserver.onNext(messageIngress.pullPending(request));
        responseObserver.onCompleted();
    }

    @Override
    public void ackPending(AckPendingRequest request,
                           StreamObserver<AckPendingResponse> responseObserver) {
        responseObserver.onNext(messageIngress.ackPending(request));
        responseObserver.onCompleted();
    }
}
