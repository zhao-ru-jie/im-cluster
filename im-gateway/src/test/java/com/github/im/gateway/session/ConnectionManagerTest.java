package com.github.im.gateway.session;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 多端在线（同一 userId 多设备并存）的会话表语义。
 *
 * <p>这里守的是三条容易改坏的规则：
 * <ol>
 *   <li><b>同设备重连必须踢旧连接</b>：否则消息会被推到一条僵尸连接上，用户表现为「消息丢了」；</li>
 *   <li><b>不同设备必须都收到</b>：{@code send} 的语义是「推给这个人的所有设备」，不是「推给最后一个」；</li>
 *   <li><b>最后一个设备下线时要清干净</b>：残留空壳会让「连接过的用户」永久占内存，
 *       还会让人误判在线（{@code isLocal}）。</li>
 * </ol>
 */
class ConnectionManagerTest {

    @Test
    @DisplayName("同一 deviceId 重连：踢掉旧连接，连接数仍为 1")
    void sameDeviceReconnectKicksOldSession() {
        ConnectionManager manager = new ConnectionManager(5);
        FakeSession first = new FakeSession("s1", "alice", "mobile", ClientSession.TRANSPORT_TCP);
        FakeSession second = new FakeSession("s2", "alice", "mobile", ClientSession.TRANSPORT_TCP);

        assertTrue(manager.add("alice", first));
        assertTrue(manager.add("alice", second));

        assertFalse(first.isOpen(), "旧连接应被关闭");
        assertEquals("replaced-by-new-connection", first.closeReason());
        assertTrue(second.isOpen());
        assertEquals(1, manager.connectionCount());
        assertEquals(1, manager.deviceCount("alice"));
    }

    @Test
    @DisplayName("不同 deviceId 并存，send 推给所有设备")
    void differentDevicesCoexistAndBothReceive() {
        ConnectionManager manager = new ConnectionManager(5);
        FakeSession phone = new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP);
        FakeSession web = new FakeSession("s2", "alice", "web", ClientSession.TRANSPORT_WS);

        manager.add("alice", phone);
        manager.add("alice", web);

        assertEquals(2, manager.connectionCount());
        assertTrue(manager.send("alice", "frame-1"));
        assertEquals(List.of("frame-1"), phone.received());
        assertEquals(List.of("frame-1"), web.received());
    }

    @Test
    @DisplayName("移除一个设备后用户仍在线；移除全部后本机不再有该用户（不留空壳）")
    void removingLastDeviceCleansUp() {
        ConnectionManager manager = new ConnectionManager(5);
        FakeSession phone = new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP);
        FakeSession web = new FakeSession("s2", "alice", "web", ClientSession.TRANSPORT_WS);
        manager.add("alice", phone);
        manager.add("alice", web);

        manager.remove("alice", web);
        assertTrue(manager.isLocal("alice"), "还有手机在线，不应判为离线");
        assertEquals(1, manager.connectionCount());

        manager.remove("alice", phone);
        assertFalse(manager.isLocal("alice"));
        assertEquals(0, manager.connectionCount());
        assertTrue(manager.userIds().isEmpty(), "最后一个设备下线后不应残留空壳 map");
        assertFalse(manager.send("alice", "frame"));
    }

    @Test
    @DisplayName("旧会话的关闭回调不能删掉同设备的新会话（快速重连的经典 bug）")
    void staleCloseDoesNotRemoveNewSession() {
        ConnectionManager manager = new ConnectionManager(5);
        FakeSession old = new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP);
        FakeSession fresh = new FakeSession("s2", "alice", "phone", ClientSession.TRANSPORT_TCP);
        manager.add("alice", old);
        manager.add("alice", fresh);

        // 旧连接的 channelInactive 可能晚于新连接登记
        manager.remove("alice", old);

        assertTrue(manager.isLocal("alice"));
        assertTrue(manager.send("alice", "frame-1"));
        assertEquals(List.of("frame-1"), fresh.received());
    }

    @Test
    @DisplayName("超过设备上限：拒绝新连接，不影响已有设备")
    void deviceLimitRejectsNewDevice() {
        ConnectionManager manager = new ConnectionManager(2);
        FakeSession phone = new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP);
        FakeSession web = new FakeSession("s2", "alice", "web", ClientSession.TRANSPORT_WS);
        FakeSession tv = new FakeSession("s3", "alice", "tv", ClientSession.TRANSPORT_TCP);
        manager.add("alice", phone);
        manager.add("alice", web);

        assertFalse(manager.add("alice", tv), "超限应返回 false，由调用方关闭这条连接");

        assertEquals(2, manager.connectionCount());
        assertTrue(manager.send("alice", "frame"));
        assertTrue(tv.received().isEmpty(), "被拒绝的连接不应收到任何帧");
    }

    @Test
    @DisplayName("同设备重连接不受上限影响（槽位已存在）")
    void reconnectOfExistingDeviceIsAlwaysAllowed() {
        ConnectionManager manager = new ConnectionManager(1);
        manager.add("alice", new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP));

        assertTrue(manager.add("alice",
                new FakeSession("s2", "alice", "phone", ClientSession.TRANSPORT_TCP)));
        assertEquals(1, manager.connectionCount());
    }

    @Test
    @DisplayName("不带 deviceId 的客户端共用 default 槽位（保持旧的单连接语义）")
    void anonymousClientsShareDefaultSlot() {
        ConnectionManager manager = new ConnectionManager(5);
        FakeSession first = new FakeSession("s1", "alice", null, ClientSession.TRANSPORT_WS);
        FakeSession second = new FakeSession("s2", "alice", "  ", ClientSession.TRANSPORT_TCP);

        manager.add("alice", first);
        manager.add("alice", second);

        assertEquals(1, manager.connectionCount());
        assertFalse(first.isOpen(), "无 deviceId 时后连接踢掉前连接");
    }

    @Test
    @DisplayName("部分设备写失败仍算投递成功（其余设备可重连后按 seq 拉齐）")
    void partialSendFailureStillCountsAsDelivered() {
        ConnectionManager manager = new ConnectionManager(5);
        FakeSession phone = new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP);
        FakeSession web = new FakeSession("s2", "alice", "web", ClientSession.TRANSPORT_WS);
        web.failSend = true;
        manager.add("alice", phone);
        manager.add("alice", web);

        assertTrue(manager.send("alice", "frame"));
        assertEquals(List.of("frame"), phone.received());
        assertTrue(web.received().isEmpty());
    }

    @Test
    @DisplayName("连接数按「连接」而非「用户」统计（HPA 指标不能缩水）")
    void connectionCountCountsConnectionsNotUsers() {
        ConnectionManager manager = new ConnectionManager(5);
        manager.add("alice", new FakeSession("s1", "alice", "phone", ClientSession.TRANSPORT_TCP));
        manager.add("alice", new FakeSession("s2", "alice", "web", ClientSession.TRANSPORT_WS));
        manager.add("bob", new FakeSession("s3", "bob", "phone", ClientSession.TRANSPORT_TCP));

        assertEquals(3, manager.connectionCount());
        assertEquals(2, manager.userIds().size());
        assertEquals(2, manager.connectionCount(ClientSession.TRANSPORT_TCP));
        assertEquals(1, manager.connectionCount(ClientSession.TRANSPORT_WS));
        assertEquals(3, manager.sessions().size());
    }

    /** 传输无关的最小会话实现：记录收到的帧、可模拟写失败 */
    private static final class FakeSession implements ClientSession {

        private static final AtomicInteger SEQ = new AtomicInteger();

        private final String id;
        private final String userId;
        private final String deviceId;
        private final String transport;
        private final List<String> received = new CopyOnWriteArrayList<>();
        private volatile boolean open = true;
        private volatile boolean failSend;
        private volatile String closeReason;

        private FakeSession(String id, String userId, String deviceId, String transport) {
            this.id = id + "-" + SEQ.incrementAndGet();
            this.userId = userId;
            this.deviceId = deviceId;
            this.transport = transport;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public String userId() {
            return userId;
        }

        @Override
        public String deviceId() {
            return deviceId;
        }

        @Override
        public String transport() {
            return transport;
        }

        @Override
        public boolean isOpen() {
            return open;
        }

        @Override
        public boolean send(String payload) {
            if (!open || failSend) {
                return false;
            }
            received.add(payload);
            return true;
        }

        @Override
        public void close(String reason) {
            open = false;
            closeReason = reason;
        }

        private List<String> received() {
            return received;
        }

        private String closeReason() {
            return closeReason;
        }
    }
}
