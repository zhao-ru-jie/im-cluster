package com.example.im.connection;

import org.springframework.stereotype.Component;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 管理本节点上所有「用户 ID -> WebSocket 会话」的映射。
 * 只负责把消息推送到连在自己身上的客户端。
 */
@Component
public class ConnectionManager {

    private final ConcurrentHashMap<String, WebSocketSession> sessions = new ConcurrentHashMap<>();

    public void add(String userId, WebSocketSession session) {
        sessions.put(userId, session);
    }

    public void remove(String userId) {
        sessions.remove(userId);
    }

    public boolean isLocal(String userId) {
        return sessions.containsKey(userId);
    }

    public Set<String> userIds() {
        return new HashSet<>(sessions.keySet());
    }

    /**
     * 把消息推送给本地连接的指定用户。
     * @return 用户在线且推送成功返回 true，否则 false
     */
    public boolean send(String userId, String payload) {
        WebSocketSession session = sessions.get(userId);
        if (session == null || !session.isOpen()) {
            return false;
        }
        try {
            session.sendMessage(new TextMessage(payload));
            return true;
        } catch (IOException e) {
            sessions.remove(userId);
            return false;
        }
    }
}
