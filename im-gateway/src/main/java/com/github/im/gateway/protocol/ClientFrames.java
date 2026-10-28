package com.github.im.gateway.protocol;

import com.github.im.proto.presence.v1.PresenceState;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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
 *                traceId 可选：带了服务端全程复用（日志、下行 PushTask、msg.ack 回显），
 *                不带则由网关生成一个 32 位十六进制 ID
 *                富媒体可选：contentType（TEXT/IMAGE/FILE/VOICE/VIDEO/CUSTOM）+ attrs（JSON 串），
 *                两个字段都原样透传，网关不解析
 *                {"ver":1,"type":"msg.send","contentType":"IMAGE","attrs":"{\"url\":\"https://...\",\"w\":800}"}
 *                {"ver":1,"type":"sync","watermarks":{"s:alice:bob":100,"g:group1":42}}
 *                {"ver":1,"type":"msg.recv","acks":[{"conversationId":"s:alice:bob","seq":5}]}
 *                兼容旧报文：不带 type 时按 {"to","body"} 处理
 * 网关 → 客户端：{"ver":1,"type":"msg.ack|msg.push|msg.state|pong|error",...}
 *                msg.state 是回给<b>发送方</b>的状态回执（DELIVERED / READ），
 *                由 IM 服务生成、网关透传，见 im-server 的 ClientFrames#stateFrame
 *
 * 仅原生 TCP：连接后<b>首帧</b>必须是 {"ver":1,"type":"connect","userId":"alice","deviceId":"mobile","token":"..."}，
 * 网关回 {"type":"connected"}；认证前的任何其他帧都会被拒并断开。
 * （WebSocket 不需要这一步：userId/token 在 HTTP 握手 query 里带，握手成功即认证完成。）
 *
 * <p>{@code token} 是否必填取决于 {@code im.auth.mode}：off 时可省略，enforce 时缺失/无效
 * 会分别收到 HTTP 401（WS）或 {"type":"error","code":401}（TCP）。
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
    /**
     * 客户端确认<b>已收到</b>下行消息（端到端 ACK，delivered 的真正来源）。
     *
     * <p>{@code {"ver":1,"type":"msg.recv","acks":[{"conversationId":"s:alice:bob","seq":5},{"conversationId":"g:group1","seq":12}]}}
     *
     * <p>与 {@link #TYPE_SYNC} 的分工：{@code recv} 是「刚收到就告诉你」，高频、增量；
     * {@code sync} 是「重连后对账」，低频、全量。前者让「用户说没收到」能查出证据，
     * 后者是 Redis 计数器整体丢失时 seq 发号的最后一道下界。两者写的是同一张水位表。
     *
     * <p>必须是批量：逐条 ACK 会把「消息数」放大成「RPC 数」，移动端还要多付一份电量和流量。
     * 客户端应攒几条（或 50~100ms）再发一次。
     */
    public static final String TYPE_RECV = "msg.recv";
    /**
     * 客户端上报<b>已读位置</b>（端到端状态机的最后一环）。
     *
     * <p>{@code {"ver":1,"type":"msg.read","conversationId":"s:alice:bob","seq":12}}
     *
     * <p>为什么不只留 HTTP 接口：{@code msg.read} 与 {@code msg.recv} 是同一条路径上的两件事
     * （收到 → 看到），连接已经在了，再绕一次 HTTP 只是多一次 TLS 握手与一次鉴权；
     * 而移动端在切后台前上报已读时，往往正是连接还热、HTTP 栈已经准备休眠的时刻。
     *
     * <p>语义与 {@code MarkRead} 接口完全一致：用户级、只前进、由服务端 cap 到 last_seq。
     */
    public static final String TYPE_READ = "msg.read";
    /**
     * 客户端上报「我正在输入」（{@code {"ver":1,"type":"typing","conversationId":"s:a:b","typing":true}}）。
     *
     * <p>只给会话 ID 也行（单聊可以只给 {@code to}，群聊给 {@code convType":"GROUP","to":"群号"}），
     * 会话 ID 缺失时由 IM 服务按消息路径同一套规则生成——网关不推导会话，避免两处算出不同的 ID。
     *
     * <p>语义是<b>状态</b>而不是事件：网关只做「解析 → 转给 IM 服务」，不排队、不缓存、失败静默。
     * 一条丢掉的 typing 最多让对方少看一次三个点，而它为它加一个重试分支会引入真正的麻烦
     * （重试导致的重复扇出、连接层为装饰品等待）。
     */
    public static final String TYPE_TYPING = "typing";
    /**
     * 查询在线状态（{@code {"ver":1,"type":"presence.get","userIds":["bob","carol"]}}）。
     *
     * <p>为什么长连接上也要这个：会话页打开时「对面在不在」是必答项，而连接本来就是热的，
     * 再走一次 HTTP 只是多一次握手与鉴权。批量好友列表那种几十上百人的查询仍然建议走
     * {@code GET /presence}（可缓存、可用连接池）。
     */
    public static final String TYPE_PRESENCE_GET = "presence.get";
    /** 在线状态查询的应答（由网关直接回给请求的连接，不走 IM 服务的推送通道） */
    public static final String TYPE_PRESENCE_SNAPSHOT = "presence.snapshot";
    /**
     * 踢下线（{@code {"ver":1,"type":"session.kick","deviceId":"pad","reason":"...","ts":..}}）。
     *
     * <p>它是<b>告别</b>而不是命令：连接马上就关，这一帧只是给正常客户端一个明确的退出理由
     * （比如「你在另一台设备登录了」），让它不要当成网络故障去重连。
     * 不指望它约束恶意客户端——真正的下线由服务端关闭 TCP 连接完成。
     */
    public static final String TYPE_SESSION_KICK = "session.kick";
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

    /**
     * @param traceId 回显本帧的链路追踪 ID（客户端带了就原样带回，没带则是服务端生成的）。
     *                客户端把它记进自己的日志，线上排障时「客户端一条日志 ↔ 服务端一条日志」就能对上。
     */
    public String ack(String clientMsgId, String serverMsgId, long seq, String traceId) {
        Map<String, Object> frame = base(TYPE_ACK);
        frame.put("clientMsgId", clientMsgId);
        frame.put("serverMsgId", serverMsgId);
        frame.put("seq", seq);
        frame.put("code", 0);
        if (traceId != null && !traceId.isBlank()) {
            frame.put("traceId", traceId);
        }
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

    /**
     * 踢下线帧：关闭连接前最后一帧。
     *
     * @param deviceId 被踢的设备（客户端可以据此判断「被踢的是不是我自己」，
     *                 多端场景下它也可能收到别的设备的 kick 事件）
     * @param reason   退出理由，原样展示给用户
     */
    public String kickFrame(String deviceId, String reason) {
        Map<String, Object> frame = base(TYPE_SESSION_KICK);
        frame.put("deviceId", deviceId == null ? "" : deviceId);
        frame.put("reason", reason == null ? "" : reason);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    /**
     * 在线状态查询应答帧。
     *
     * <p>为什么由网关渲染而不是让 IM 服务返回一段 frame 字符串：这是<b>请求-应答</b>的控制帧
     * （和 {@code pong} / {@code msg.ack} 同类），它只回给「问的那条连接」，不走推送通道。
     * 业务事件（{@code msg.push} / {@code presence.push}）才是 IM 服务的协议，
     * 由它渲染、网关原样透传。这条边界一旦模糊，网关就会开始「懂」业务字段。
     *
     * @param states 与 {@code GET /presence} 出参同形，客户端可复用同一段渲染代码
     */
    public String presenceSnapshot(Collection<PresenceState> states) {
        Map<String, Object> frame = base(TYPE_PRESENCE_SNAPSHOT);
        List<Map<String, Object>> items = new ArrayList<>(states == null ? 0 : states.size());
        if (states != null) {
            for (PresenceState state : states) {
                Map<String, Object> item = new LinkedHashMap<>();
                item.put("userId", state.getUserId());
                item.put("online", state.getOnline());
                item.put("gateways", state.getGateways());
                item.put("devices", state.getDevices());
                item.put("lastSeen", state.getLastSeen());
                items.add(item);
            }
        }
        frame.put("states", items);
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
