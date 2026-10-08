package com.example.im.ws;

import com.example.im.connection.ConnectionManager;
import com.example.im.connection.OfflineMessageStore;
import com.example.im.connection.UserNodeRegistry;
import com.example.im.route.MessageEnvelope;
import com.example.im.route.MessageRouter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

/**
 * WebSocket 长连接处理器：客户端连上来后注册会话与路由，并补发离线消息；
 * 收到消息后交给 MessageRouter。连接地址示例：ws://host:8080/ws?userId=alice
 */
public class ImWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(ImWebSocketHandler.class);

    private final ConnectionManager connectionManager;
    private final UserNodeRegistry registry;
    private final MessageRouter router;
    private final ObjectMapper objectMapper;
    private final OfflineMessageStore offlineMessageStore;

    public ImWebSocketHandler(ConnectionManager connectionManager,
                              UserNodeRegistry registry,
                              MessageRouter router,
                              ObjectMapper objectMapper,
                              OfflineMessageStore offlineMessageStore) {
        this.connectionManager = connectionManager;
        this.registry = registry;
        this.router = router;
        this.objectMapper = objectMapper;
        this.offlineMessageStore = offlineMessageStore;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        String userId = (String) session.getAttributes().get("userId");
        if (userId == null) {
            return;
        }
        connectionManager.add(userId, session);
        registry.bindUser(userId);
        // 补发离线消息
        for (String msg : offlineMessageStore.drain(userId)) {
            connectionManager.send(userId, msg);
        }
        log.info("user {} connected on node {}", userId, registry.localNodeId());
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) throws Exception {
        String userId = (String) session.getAttributes().get("userId");
        JsonNode node = objectMapper.readTree(message.getPayload());
        String to = node.get("to").asString();
        String body = node.get("body").asString();
        router.route(new MessageEnvelope(userId, to, body));
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        String userId = (String) session.getAttributes().get("userId");
        if (userId != null) {
            connectionManager.remove(userId);
            registry.unbindUser(userId);
            log.info("user {} disconnected from node {}", userId, registry.localNodeId());
        }
    }
}
