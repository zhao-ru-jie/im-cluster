package com.github.im.gateway.protocol;

import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 网关自己产出的控制帧（ACK / PONG / 错误 / 踢下线 / 认证应答）编解码。
 *
 * <p>注意边界：{@code msg.push} 这类业务下行帧<b>不由网关生成</b>，它来自 IM 服务的
 * {@code frame} 字段，网关只透传。网关只负责「连接层语义」的帧。
 *
 * <p>帧格式与传输无关：WebSocket 走 TextMessage、原生 TCP 走「4 字节长度 + UTF-8」，
 * 里面装的是同一份 JSON，客户端两套传输可以共用同一套解析代码。
 *
 * <pre>
 * 客户端 → 网关：{"ver":1,"type":"msg.send|ping","clientMsgId":"...","to":"bob","body":"hi"}
 *                {"ver":1,"type":"sync","watermarks":{"s:alice:bob":100,"g:group1":42}}
 *                兼容旧报文：不带 type 时按 {"to","body"} 处理
 * 网关 → 客户端：{"ver":1,"type":"msg.ack|msg.push|pong|error",...}
 *
 * 仅原生 TCP：连接后<b>首帧</b>必须是 {"ver":1,"type":"connect","userId":"alice","deviceId":"mobile"}，
 * 网关回 {"type":"connected"}；认证前的任何其他帧都会被拒并断开。
 * （WebSocket 不需要这一步：userId 在 HTTP 握手 query 里带，握手成功即认证完成。）
 * </pre>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
// 显式命名：im-server 里也有一个同名类（各自只产出自己负责的帧类型），
// 同进程装配时靠 bean 名区分，拆进程后各自独立。
@Component("gatewayClientFrames")
public class ClientFrames {

    public static final String TYPE_SEND = "msg.send";
    public static final String TYPE_ACK = "msg.ack";
    public static final String TYPE_PING = "ping";
    public static final String TYPE_PONG = "pong";
    public static final String TYPE_ERROR = "error";
    /**
     * 客户端上报「会话 → 已见最大 seq」的消费水位，服务端据此补拉缺口并抬高 seq 发号下界。
     *
     * <p>为什么要客户端来报：seq 发号器的下界取「已落库的最大 seq」，而当消息存储与发号器
     * 共用同一个 Redis 并整体丢失时，这个下界会一起消失、seq 从 1 重来。此时只有客户端手里的
     * lastSeq 还能证明「这些号已发出并被消费过」；缺了它，新消息会被客户端当历史去重丢弃。
     */
    public static final String TYPE_SYNC = "sync";
    /** 服务端要求客户端迁移连接（缩容/发版/摘流） */
    public static final String TYPE_MIGRATE = "session.migrate";
    /** 仅 TCP：客户端首帧声明身份 */
    public static final String TYPE_CONNECT = "connect";
    /** 仅 TCP：服务端确认认证成功 */
    public static final String TYPE_CONNECTED = "connected";

    public static final int VER = 1;

    private final ObjectMapper objectMapper;

    public ClientFrames(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public JsonNode parse(String payload) {
        return objectMapper.readTree(payload);
    }

    public String ack(String clientMsgId, String serverMsgId, long seq) {
        Map<String, Object> frame = base(TYPE_ACK);
        frame.put("clientMsgId", clientMsgId);
        frame.put("serverMsgId", serverMsgId);
        frame.put("seq", seq);
        frame.put("code", 0);
        return write(frame);
    }

    public String error(String clientMsgId, int code, String message) {
        Map<String, Object> frame = base(TYPE_ERROR);
        frame.put("clientMsgId", clientMsgId);
        frame.put("code", code);
        frame.put("message", message);
        return write(frame);
    }

    public String pong() {
        Map<String, Object> frame = base(TYPE_PONG);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    /** 服务端主动探测（空闲巡检用）：客户端应回 {@code {"type":"pong"}} */
    public String ping() {
        Map<String, Object> frame = base(TYPE_PING);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    /** TCP 认证成功应答：客户端据此确认 userId 已被服务端接受 */
    public String connected(String userId, String transport) {
        Map<String, Object> frame = base(TYPE_CONNECTED);
        frame.put("userId", userId);
        frame.put("transport", transport);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    /**
     * 摘流通知帧：客户端收到后应「等 retryAfterMs + 随机抖动」再重连。
     *
     * <p>抖动必须由客户端加：服务端如果把几千个连接安排在同一毫秒重连，
     * 只会把重连风暴从一个实例平移到另一个实例。
     */
    public String migrate(String reason, long retryAfterMs) {
        Map<String, Object> frame = base(TYPE_MIGRATE);
        frame.put("reason", reason);
        frame.put("retryAfterMs", retryAfterMs);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    private Map<String, Object> base(String type) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", type);
        return frame;
    }

    private String write(Map<String, Object> frame) {
        return objectMapper.writeValueAsString(frame);
    }
}
