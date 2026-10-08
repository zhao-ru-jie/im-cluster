package com.example.im.route;

import com.example.im.connection.ConnectionManager;
import com.example.im.connection.OfflineMessageStore;
import com.example.im.connection.UserNodeRegistry;
import com.example.im.discovery.NacosNodeDiscovery;
import com.example.im.grpc.ForwardClient;
import com.example.im.grpc.ForwardRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * 消息路由核心：决定消息是直接本地推送、跨节点 gRPC 转发，还是写入离线队列。
 */
@Component
public class MessageRouter {

    private static final Logger log = LoggerFactory.getLogger(MessageRouter.class);

    private final UserNodeRegistry registry;
    private final ConnectionManager connectionManager;
    private final ForwardClient forwardClient;
    private final OfflineMessageStore offlineMessageStore;
    private final NacosNodeDiscovery nacosDiscovery;

    public MessageRouter(UserNodeRegistry registry,
                         ConnectionManager connectionManager,
                         ForwardClient forwardClient,
                         OfflineMessageStore offlineMessageStore,
                         NacosNodeDiscovery nacosDiscovery) {
        this.registry = registry;
        this.connectionManager = connectionManager;
        this.forwardClient = forwardClient;
        this.offlineMessageStore = offlineMessageStore;
        this.nacosDiscovery = nacosDiscovery;
    }

    public void route(MessageEnvelope env) {
        // nodeOf 的返回值就是目标节点的 gRPC 地址（host:port）；null 表示不在线
        String targetAddress = registry.nodeOf(env.to());

        // 1) 目标用户完全不在线 -> 落离线队列
        if (targetAddress == null) {
            log.info("user {} offline, store message from {}", env.to(), env.from());
            offlineMessageStore.store(env.to(), env.body());
            return;
        }

        // 2) 目标用户就在本节点 -> 本地直接推送
        if (targetAddress.equals(registry.localNodeId())) {
            if (!connectionManager.send(env.to(), env.body())) {
                offlineMessageStore.store(env.to(), env.body());
            }
            return;
        }

        // 3) 跨节点 -> 先经 Nacos 解析出实时可达（健康）地址，再做 gRPC 转发；
        //    Nacos 未启用或不可用时，resolveTarget 直接返回 targetAddress（即 nodeId = host:port），行为不变
        String target = nacosDiscovery.resolveTarget(targetAddress);
        ForwardRequest request = ForwardRequest.newBuilder()
                .setSenderId(env.from())
                .setReceiverId(env.to())
                .setPayload(env.body())
                .setTimestamp(System.currentTimeMillis())
                .build();
        try {
            if (!forwardClient.forward(target, request).getSuccess()) {
                offlineMessageStore.store(env.to(), env.body());
            }
        } catch (Exception e) {
            log.error("forward to {} (resolved from {}) failed, store offline: {}", target, targetAddress, e.getMessage());
            offlineMessageStore.store(env.to(), env.body());
        }
    }
}
