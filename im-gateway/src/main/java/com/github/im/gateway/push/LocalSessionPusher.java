package com.github.im.gateway.push;

import com.github.im.gateway.session.ConnectionManager;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 把下行帧推给「连在本实例上」的会话。
 *
 * <p>这是 {@link DownstreamPushGrpcService} 与 {@link GatewayPushPort} 的公共底层：
 * 无论请求来自其它网关实例的 gRPC 转发，还是来自本进程内的 IM 服务，
 * 最终都落到这里。帧内容只透传，不做任何业务解析。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class LocalSessionPusher {

    private final ConnectionManager connectionManager;

    public LocalSessionPusher(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public PushResult push(PushTask task) {
        List<String> delivered = new ArrayList<>();
        List<String> failed = new ArrayList<>();
        for (String userId : task.getTargetUserIdsList()) {
            if (connectionManager.send(userId, task.getFrame())) {
                delivered.add(userId);
            } else {
                failed.add(userId);
            }
        }
        return PushResult.newBuilder()
                .addAllDeliveredUserIds(delivered)
                .addAllFailedUserIds(failed)
                .build();
    }
}
