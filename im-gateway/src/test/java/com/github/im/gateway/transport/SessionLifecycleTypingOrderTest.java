package com.github.im.gateway.transport;

import com.github.im.contract.MessageIngress;
import com.github.im.contract.PresencePort;
import com.github.im.gateway.metrics.GatewayMetrics;
import com.github.im.gateway.protocol.ClientFrames;
import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import com.github.im.gateway.session.GatewayRegistry;
import com.github.im.proto.presence.v1.TypingRequest;
import com.github.im.proto.presence.v1.TypingResponse;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * typing 上行的<b>顺序</b>契约。
 *
 * <p>为什么必须测顺序：输入状态的语义全压在「{@code true} 之后来 {@code false}」上。
 * 曾经这里是「一帧起一个虚拟线程去上报」，于是紧挨着发出的 true/false 会换序到达 IM 服务——
 * 现象是「对方第二次正在输入不出现，只收到停止输入」：先到的 false 把状态清了，
 * 后到的 true 又被 IM 服务的 3s 去抖当成重复帧吞掉。没有异常、没有日志，只有用户看到的状态不对。
 *
 * <p>这里用「true 慢、false 快」的假端口放大这种竞态：只要实现回到「每帧一个线程」，
 * 断言立刻失败。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class SessionLifecycleTypingOrderTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final ClientFrames frames = new ClientFrames(mapper);

    @Test
    @DisplayName("typing：同一连接的 true/false 按上行顺序上报（乱序会让对方只收到停止输入）")
    void typingReportsPreserveUplinkOrder() throws Exception {
        List<String> reported = Collections.synchronizedList(new ArrayList<>());
        PresencePort port = new PresencePort() {
            @Override
            public TypingResponse reportTyping(TypingRequest request) {
                try {
                    // true 故意慢一点：旧实现（每帧一个虚拟线程）下 false 会超车
                    if (request.getTyping()) {
                        Thread.sleep(2);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                reported.add(request.getTyping() ? "true" : "false");
                return TypingResponse.newBuilder().setCode(0).setMessage("ok").build();
            }
        };

        SessionLifecycle lifecycle = newLifecycle(port);
        ClientSession session = mock(ClientSession.class);
        when(session.userId()).thenReturn("alice");

        for (int i = 0; i < 20; i++) {
            lifecycle.onFrame(session, typingFrame(true));
            lifecycle.onFrame(session, typingFrame(false));
        }

        long deadline = System.currentTimeMillis() + 5_000;
        while (reported.size() < 40 && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(40, reported.size(), "20 组 true/false 都应上报，丢帧说明串行队列被打满了");
        for (int i = 0; i < 40; i++) {
            assertEquals(i % 2 == 0 ? "true" : "false", reported.get(i),
                    "第 " + (i + 1) + " 帧顺序错了：true/false 换序会让接收方只看到「停止输入」");
        }
    }

    private String typingFrame(boolean typing) {
        return "{\"ver\":1,\"type\":\"typing\",\"conversationId\":\"s:alice:bob\",\"typing\":" + typing + "}";
    }

    @SuppressWarnings("unchecked")
    private SessionLifecycle newLifecycle(PresencePort port) {
        ObjectProvider<PresencePort> presenceProvider = mock(ObjectProvider.class);
        when(presenceProvider.getIfAvailable()).thenReturn(port);
        ObjectProvider<MessageIngress> ingressProvider = mock(ObjectProvider.class);
        ObjectProvider<MeterRegistry> registryProvider = mock(ObjectProvider.class);
        ConnectionManager connectionManager = mock(ConnectionManager.class);

        return new SessionLifecycle(connectionManager, mock(GatewayRegistry.class), frames,
                ingressProvider, presenceProvider,
                new GatewayMetrics(registryProvider, connectionManager));
    }
}
