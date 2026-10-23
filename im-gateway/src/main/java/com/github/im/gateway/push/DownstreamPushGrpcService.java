package com.github.im.gateway.push;

import com.github.im.proto.push.v1.DownstreamPushGrpc;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

/**
 * 下行 gRPC 服务端（长连接服务暴露给 IM 服务 / 其它网关实例）。
 *
 * <p>语义：收到 {@link PushTask} 后按 userId 找本地会话，找不到即如实计入
 * {@code failed_user_ids}。这里<b>不做</b>离线判定、不做重试、不做持久化——
 * 是否转离线由 IM 服务根据返回结果决定（连接侧没有消息存储能力）。
 */
@GrpcService
public class DownstreamPushGrpcService extends DownstreamPushGrpc.DownstreamPushImplBase {

    private final LocalSessionPusher localSessionPusher;

    public DownstreamPushGrpcService(LocalSessionPusher localSessionPusher) {
        this.localSessionPusher = localSessionPusher;
    }

    @Override
    public void push(PushTask request, StreamObserver<PushResult> responseObserver) {
        responseObserver.onNext(localSessionPusher.push(request));
        responseObserver.onCompleted();
    }
}
