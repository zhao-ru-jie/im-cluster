package com.github.im.gateway.push;

import com.github.im.proto.push.v1.DownstreamPushGrpc;
import com.github.im.proto.push.v1.KickResult;
import com.github.im.proto.push.v1.KickTask;
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
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@GrpcService
public class DownstreamPushGrpcService extends DownstreamPushGrpc.DownstreamPushImplBase {

    private final LocalSessionPusher localSessionPusher;
    private final LocalDeviceControl localDeviceControl;

    public DownstreamPushGrpcService(LocalSessionPusher localSessionPusher,
                                     LocalDeviceControl localDeviceControl) {
        this.localSessionPusher = localSessionPusher;
        this.localDeviceControl = localDeviceControl;
    }

    @Override
    public void push(PushTask request, StreamObserver<PushResult> responseObserver) {
        responseObserver.onNext(localSessionPusher.push(request));
        responseObserver.onCompleted();
    }

    /**
     * 踢下线：只处理「连在本实例上」的那部分，命中几条就如实回几条。
     *
     * <p>刻意不做「路由里还有别的实例就转发过去」：转发逻辑属于调用方（IM 服务），
     * 它按路由表给每个实例各发一次；网关之间互相转发会形成环，也会让
     * {@code kicked} 的语义变得不可解释。
     */
    @Override
    public void kick(KickTask request, StreamObserver<KickResult> responseObserver) {
        responseObserver.onNext(localDeviceControl.kick(request));
        responseObserver.onCompleted();
    }
}
