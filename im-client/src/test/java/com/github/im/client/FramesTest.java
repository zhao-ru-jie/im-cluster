package com.github.im.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 帧的 wire 格式锁定。
 *
 * <p>这里的断言都是「字段名 + 数字/字符串类型」这种看起来琐碎的东西，但它们正是
 * 客户端与服务端之间唯一的契约：字段名写错不会被任何一方拒绝，
 * 只会表现成「消息能发出去但对方永远收不到」。
 */
class FramesTest {

    private final ObjectMapper mapper = new ObjectMapper();
    private final Frames frames = new Frames();

    @Test
    @DisplayName("connect 帧：userId 必带，deviceId/token 为空时不下发")
    void connectFrame() {
        JsonNode full = mapper.readTree(frames.connect("alice", "mobile", "v1.123.sig"));
        assertEquals(1, full.path("ver").asInt());
        assertEquals("connect", full.path("type").asString());
        assertEquals("alice", full.path("userId").asString());
        assertEquals("mobile", full.path("deviceId").asString());
        assertEquals("v1.123.sig", full.path("token").asString());

        JsonNode minimal = mapper.readTree(frames.connect("alice", null, null));
        assertFalse(minimal.has("deviceId"), "deviceId 留空应由服务端落到 default 槽位，而不是发空串");
        assertFalse(minimal.has("token"), "auth.mode=off 时不该硬塞空 token");
    }

    @Test
    @DisplayName("msg.send：纯文本不带 attrs，contentType 缺省 TEXT，clientTs 是数字")
    void sendFrameTextOnly() {
        OutgoingMessage message = OutgoingMessage.builder()
                .clientMsgId("c-1").to("bob").body("hi").clientTs(1_700_000_000_000L).build();
        JsonNode node = mapper.readTree(frames.send(message));

        assertEquals("msg.send", node.path("type").asString());
        assertEquals("c-1", node.path("clientMsgId").asString());
        assertEquals("bob", node.path("to").asString());
        assertEquals("hi", node.path("body").asString());
        assertEquals("TEXT", node.path("contentType").asString());
        assertFalse(node.has("attrs"), "纯文本帧的形状要和老版本一致，不能凭空多出 attrs");
        assertTrue(node.path("clientTs").isNumber(), "数字字段必须发数字：服务端用 asLong 读");
    }

    @Test
    @DisplayName("msg.send：富媒体带 contentType 与 attrs（attrs 是字符串里再套 JSON）")
    void sendFrameMedia() {
        OutgoingMessage message = OutgoingMessage.builder()
                .clientMsgId("c-2").to("bob").contentType("IMAGE")
                .attrs("{\"url\":\"https://cdn/x.png\"}").build();
        JsonNode node = mapper.readTree(frames.send(message));

        assertEquals("IMAGE", node.path("contentType").asString());
        assertTrue(node.path("attrs").isString(), "attrs 必须是字符串，不能展开成对象");
        assertEquals("{\"url\":\"https://cdn/x.png\"}", node.path("attrs").asString());
    }

    @Test
    @DisplayName("群消息：带 conversationId 与 convType，不带 to")
    void sendFrameGroup() {
        OutgoingMessage message = OutgoingMessage.builder()
                .conversationId("g:g1").convType(OutgoingMessage.CONV_GROUP).body("hello").build();
        JsonNode node = mapper.readTree(frames.send(message.normalized()));

        assertEquals("g:g1", node.path("conversationId").asString());
        assertEquals("GROUP", node.path("convType").asString());
        assertFalse(node.has("to"));
    }

    @Test
    @DisplayName("msg.recv：acks 是数字 seq 的数组，不是字符串")
    void recvFrameIsBatchWithNumericSeq() {
        JsonNode node = mapper.readTree(frames.recv(List.of(
                new Frames.DeliveredAck("s:alice:bob", 5),
                new Frames.DeliveredAck("g:g1", 12))));

        assertEquals("msg.recv", node.path("type").asString());
        assertEquals(2, node.path("acks").size());
        assertEquals("s:alice:bob", node.path("acks").get(0).path("conversationId").asString());
        assertTrue(node.path("acks").get(0).path("seq").isNumber());
        assertEquals(12, node.path("acks").get(1).path("seq").asLong());
    }

    @Test
    @DisplayName("sync 帧的 watermarks 是 object 且值是数字")
    void syncFrameCarriesWatermarks() {
        Map<String, Long> watermarks = new LinkedHashMap<>();
        watermarks.put("s:alice:bob", 100L);
        watermarks.put("g:g1", 42L);
        JsonNode node = mapper.readTree(frames.sync(watermarks));

        assertEquals("sync", node.path("type").asString());
        assertEquals(100, node.path("watermarks").path("s:alice:bob").asLong());
        assertEquals(42, node.path("watermarks").path("g:g1").asLong());
        assertTrue(node.path("watermarks").path("g:g1").isNumber());
    }

    @Test
    @DisplayName("下行 msg.push：完整字段解析（含富媒体）")
    void parsePushWithMedia() {
        String payload = """
                {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE",
                 "seq":7,"serverMsgId":"srv-7","clientMsgId":"c-7","from":"alice","to":"bob",
                 "body":"hi","contentType":"IMAGE","attrs":"{\\"w\\":800}","ts":1700000000000}
                """;
        DownlinkFrame frame = frames.parse(payload);

        DownlinkFrame.Push push = (DownlinkFrame.Push) frame;
        assertEquals(Frames.T_PUSH, push.type());
        assertEquals("s:alice:bob", push.conversationId());
        assertEquals(7, push.seq());
        assertEquals("srv-7", push.serverMsgId());
        assertEquals("IMAGE", push.contentType());
        assertEquals("{\"w\":800}", push.attrs());
        assertFalse(push.recalled());
    }

    @Test
    @DisplayName("下行 msg.push：撤回帧没有 attrs、body 为空、recalled=true")
    void parseRecalledPush() {
        String payload = """
                {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE",
                 "seq":8,"serverMsgId":"srv-8","from":"alice","to":"bob","body":"",
                 "contentType":"TEXT","recalled":true,"ts":1700000000000}
                """;
        DownlinkFrame.Push push = (DownlinkFrame.Push) frames.parse(payload);

        assertTrue(push.recalled());
        assertEquals("", push.body());
        assertNull(push.attrs(), "撤回后不允许再带出任何原文/媒体地址");
    }

    @Test
    @DisplayName("下行 msg.ack / error / state / update / group.update / migrate 都能识别")
    void parseControlFrames() {
        DownlinkFrame ack = frames.parse("""
                {"ver":1,"type":"msg.ack","clientMsgId":"c-1","serverMsgId":"srv-1","seq":3,"code":0}
                """);
        assertEquals(0, ((DownlinkFrame.Ack) ack).code());
        assertEquals(3, ((DownlinkFrame.Ack) ack).seq());

        DownlinkFrame error = frames.parse("""
                {"ver":1,"type":"error","clientMsgId":"c-1","code":503,"message":"unavailable"}
                """);
        assertTrue(error instanceof DownlinkFrame.Error);
        assertEquals(503, ((DownlinkFrame.Error) error).code());

        DownlinkFrame state = frames.parse("""
                {"ver":1,"type":"msg.state","conversationId":"s:a:b","convType":"SINGLE",
                 "state":"READ","seq":9,"userId":"bob","ts":1}
                """);
        assertEquals("READ", ((DownlinkFrame.State) state).state());

        DownlinkFrame update = frames.parse("""
                {"ver":1,"type":"msg.update","conversationId":"s:a:b","convType":"SINGLE",
                 "seq":9,"change":"RECALLED","actorId":"alice","ts":1}
                """);
        assertEquals("RECALLED", ((DownlinkFrame.Update) update).change());

        DownlinkFrame group = frames.parse("""
                {"ver":1,"type":"group.update","groupId":"g1","change":"MEMBER_REMOVED",
                 "actorId":"alice","targetId":"bob","ts":1}
                """);
        assertEquals("bob", ((DownlinkFrame.GroupUpdate) group).targetId());

        DownlinkFrame migrate = frames.parse("""
                {"ver":1,"type":"session.migrate","reason":"scale-in","retryAfterMs":1500,"ts":1}
                """);
        assertEquals(1_500, ((DownlinkFrame.Migrate) migrate).retryAfterMs());
        assertEquals("scale-in", ((DownlinkFrame.Migrate) migrate).reason());
    }

    @Test
    @DisplayName("未知 type 不抛异常：老客户端遇到新帧只是「不处理」，不能整条连接崩掉")
    void unknownTypeIsTolerated() {
        DownlinkFrame frame = frames.parse("""
                {"ver":1,"type":"future.thing","x":1}
                """);
        assertTrue(frame instanceof DownlinkFrame.Unknown);
        assertEquals("future.thing", frame.type());
    }

    @Test
    @DisplayName("不是 JSON 对象时必须抛异常：说明字节流已错位，只能断开重连")
    void nonObjectPayloadFails() {
        assertThrows(IllegalArgumentException.class, () -> frames.parse("[1,2,3]"));
        assertThrows(RuntimeException.class, () -> frames.parse("not-json"));
    }

    @Test
    @DisplayName("pong / read / ping 的字段")
    void miscUplinkFrames() {
        JsonNode pong = mapper.readTree(frames.pong());
        assertEquals("pong", pong.path("type").asString());
        assertTrue(pong.path("ts").isNumber());

        JsonNode read = mapper.readTree(frames.read("s:alice:bob", 12));
        assertEquals("msg.read", read.path("type").asString());
        assertEquals("s:alice:bob", read.path("conversationId").asString());
        assertEquals(12, read.path("seq").asLong());
        assertTrue(read.path("seq").isNumber());
    }
}
