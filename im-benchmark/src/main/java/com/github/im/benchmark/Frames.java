package com.github.im.benchmark;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 客户端一侧的帧编解码（与网关的 wire 格式对齐，但不复用服务端代码）。
 *
 * <p>线上格式：WebSocket <b>文本帧</b>，内容是 JSON。proto 只用于网关↔IM 之间的内部 gRPC，
 * 客户端完全看不到 protobuf。
 *
 * <pre>
 * 上行  {"ver":1,"type":"msg.send","clientMsgId":"...","to":"bench-1","body":"..."}
 *       {"ver":1,"type":"pong"}
 * 下行  {"ver":1,"type":"msg.ack","clientMsgId":"...","serverMsgId":"...","seq":7,"code":0}
 *       {"ver":1,"type":"msg.push","from":"bench-0","to":"bench-1","body":"...","ts":...}
 *       {"ver":1,"type":"ping","ts":...}          ← 服务端主动探测，客户端必须回 pong
 *       {"ver":1,"type":"pong","ts":...}
 *       {"ver":1,"type":"error","clientMsgId":"...","code":400|503,"message":"..."}
 *       {"ver":1,"type":"session.migrate","reason":"scale-in","retryAfterMs":1500}
 * </pre>
 *
 * <p>为什么上行帧还要走 Jackson 而不是手拼字符串：字段里的 body 虽然是压测端自己造的，
 * 但一旦加上「可配置的消息体前缀」就会遇到转义问题，手拼 JSON 是那种「跑一周才在某个
 * 特定 body 上炸」的坑，没必要为了每条消息省几微秒去冒这个险。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class Frames {

    public static final String TYPE_SEND = "msg.send";
    public static final String TYPE_ACK = "msg.ack";
    public static final String TYPE_PUSH = "msg.push";
    public static final String TYPE_PING = "ping";
    public static final String TYPE_PONG = "pong";
    public static final String TYPE_ERROR = "error";
    public static final String TYPE_MIGRATE = "session.migrate";

    private static final int VER = 1;

    /** 心跳应答帧内容固定，预编码成常量，避免每条 ping 都走一次序列化。 */
    private static final String PONG_FRAME = "{\"ver\":1,\"type\":\"pong\"}";

    private final ObjectMapper mapper = new ObjectMapper();

    /** 业务上行：单聊消息。不传 convType/conversationId，让服务端按 from/to 推导单聊会话。 */
    public String send(String clientMsgId, String to, String body) {
        Map<String, Object> frame = base(TYPE_SEND);
        frame.put("clientMsgId", clientMsgId);
        frame.put("to", to);
        frame.put("body", body);
        return mapper.writeValueAsString(frame);
    }

    /** 应答服务端心跳探测；网关靠「收到任何上行帧」来重置空闲计时，pong 同时也证明了连接可写。 */
    public String pong() {
        return PONG_FRAME;
    }

    /**
     * 解析下行帧。
     *
     * @return 解析结果；不是 JSON 对象时返回 {@code null}
     */
    public Parsed parse(String payload) {
        JsonNode root = mapper.readTree(payload);
        if (root == null || !root.isObject()) {
            return null;
        }
        return new Parsed(
                root.path("type").asString(null),
                root.path("clientMsgId").asString(null),
                root.path("code").asInt(-1),
                root.path("body").asString(null));
    }

    public record Parsed(String type, String clientMsgId, int code, String body) {
    }

    private Map<String, Object> base(String type) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", type);
        return frame;
    }
}
