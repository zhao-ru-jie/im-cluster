package com.example.im.ws;

import com.example.im.connection.ConnectionManager;
import com.example.im.connection.OfflineMessageStore;
import com.example.im.connection.UserNodeRegistry;
import com.example.im.route.MessageRouter;
import tools.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;

import java.util.Map;

@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final ConnectionManager connectionManager;
    private final UserNodeRegistry registry;
    private final MessageRouter router;
    private final ObjectMapper objectMapper;
    private final OfflineMessageStore offlineMessageStore;

    public WebSocketConfig(ConnectionManager connectionManager,
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
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(new ImWebSocketHandler(connectionManager, this.registry, router, objectMapper, offlineMessageStore), "/ws")
                .addInterceptors(new HandshakeInterceptor() {
                    @Override
                    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                                   org.springframework.web.socket.WebSocketHandler wsHandler,
                                                   Map<String, Object> attributes) {
                        String query = request.getURI().getQuery();
                        if (query != null && query.startsWith("userId=")) {
                            attributes.put("userId", query.substring("userId=".length()));
                        }
                        return attributes.containsKey("userId");
                    }

                    @Override
                    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                                               org.springframework.web.socket.WebSocketHandler wsHandler,
                                               Exception exception) {
                    }
                })
                .setAllowedOrigins("*");
    }
}
