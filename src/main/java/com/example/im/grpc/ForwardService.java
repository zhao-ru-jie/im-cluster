package com.example.im.grpc;

import com.example.im.connection.ConnectionManager;
import io.grpc.stub.StreamObserver;
import org.springframework.grpc.server.service.GrpcService;

/**
 * gRPC 服务端：接收其他节点转过来的消息，推送给连在本节点的目标用户。
 * 使用 Spring Boot 4.1 官方 gRPC 的 @GrpcService（等价于 @Service + BindableService），
 * 服务会被自动注册到 Netty gRPC Server，无需手动启动。
 */
@GrpcService
public class ForwardService extends ForwardServiceGrpc.ForwardServiceImplBase {

    private final ConnectionManager connectionManager;

    public ForwardService(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    @Override
    public void forward(ForwardRequest request, StreamObserver<ForwardResponse> responseObserver) {
        boolean delivered = connectionManager.send(request.getReceiverId(), request.getPayload());
        ForwardResponse response = ForwardResponse.newBuilder()
                .setSuccess(delivered)
                .setMessage(delivered ? "delivered" : "receiver not connected to this node")
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
