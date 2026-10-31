package com.github.im.client;

import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 客户端侧的帧编解码。
 *
 * <h2>线上格式</h2>
 *
 * <p>TCP：{@code 4 字节大端长度 + UTF-8 JSON}（与网关的 {@code LengthFieldPrepender(4)} 对齐）。
 * 帧内容与 WebSocket 通道完全一致，因此同一套解析代码可复用于两种承载。
 *
 * <pre>
 * 上行  {"ver":1,"type":"connect","userId":"alice","deviceId":"mobile","token":"..."}   ← TCP 首帧
 *       {"ver":1,"type":"msg.send","clientMsgId":"...","to":"bob","body":"hi"}
 *       {"ver":1,"type":"sync","watermarks":{"s:alice:bob":100}}
 *       {"ver":1,"type":"msg.recv","acks":[{"conversationId":"s:alice:bob","seq":5}]}
 *       {"ver":1,"type":"msg.read","conversationId":"s:alice:bob","seq":12}
 *       {"ver":1,"type":"pong"}
 * 下行  {"ver":1,"type":"msg.ack","clientMsgId":"...","serverMsgId":"...","seq":7,"code":0}
 *       {"ver":1,"type":"msg.push","conversationId":"s:a:b","seq":7,"body":"hi",...}
 * </pre>
 *
 * <h2>为什么客户端不信 {@code ver}</h2>
 *
 * <p>服务端<b>只写不校验</b> {@code ver}（全仓库没有任何入站 ver 比较），真正的兼容位是
 * {@code type}：未知 type 回 {@code error 400}。所以客户端仍然发 {@code ver=1}（未来若要
 * 严格化，服务端有据可依），但解析时<b>不</b>依赖它——把 ver 当门槛只会在升级期自伤。
 *
 * <h2>为什么数字字段必须发数字</h2>
 *
 * <p>服务端取值用 {@code asLong()} / {@code asInt()}。把 {@code seq} 写成 {@code "5"} 会
 * 被读成 0（Jackson 的 asLong 对文本节点不报错而是尽力转换或退化为 0），
 * 表现为「水位永远是 0、补发一直从头来过」——这类 bug 排查成本极高，所以编码全走 Map + Jackson。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class Frames {

    public static final int VER = 1;

    /** 仅 TCP：客户端首帧声明身份 */
    public static final String T_CONNECT = "connect";
    /** 仅 TCP：服务端认证成功应答 */
    public static final String T_CONNECTED = "connected";
    public static final String T_SEND = "msg.send";
    public static final String T_ACK = "msg.ack";
    public static final String T_PUSH = "msg.push";
    public static final String T_STATE = "msg.state";
    public static final String T_UPDATE = "msg.update";
    public static final String T_GROUP_UPDATE = "group.update";
    /** 重连对账：上报会话水位并触发一次补拉 */
    public static final String T_SYNC = "sync";
    /** 端到端确认（delivered 的真正来源） */
    public static final String T_RECV = "msg.recv";
    /** 已读上报 */
    public static final String T_READ = "msg.read";
    public static final String T_PING = "ping";
    public static final String T_PONG = "pong";
    public static final String T_ERROR = "error";
    public static final String T_MIGRATE = "session.migrate";
    /** 上行：我正在输入（按节流上报，停止输入时发 typing=false） */
    public static final String T_TYPING = "typing";
    /** 下行：某人正在输入（会话里的其他人） */
    public static final String T_TYPING_PUSH = "typing.push";
    /** 上行：查询在线状态 */
    public static final String T_PRESENCE_GET = "presence.get";
    /** 下行：在线状态查询应答（只回给发起查询的那条连接） */
    public static final String T_PRESENCE_SNAPSHOT = "presence.snapshot";
    /** 下行：好友的在线状态变化 */
    public static final String T_PRESENCE_PUSH = "presence.push";
    /** 下行：同账号的其它设备登录了（多端登录通知，含自己那台，按 deviceId 过滤） */
    public static final String T_SESSION_LOGIN = "session.login";
    /** 下行：本连接被服务端踢下线（收到后连接会被立即关闭，不要重连） */
    public static final String T_SESSION_KICK = "session.kick";
    /** 下行：会话标记（置顶/免打扰）变更，同账号其它端需同步 */
    public static final String T_CONV_UPDATE = "conv.update";
    /** 下行：有人申请加你为好友（{@code fromId} + 验证消息） */
    public static final String T_RELATION_REQUEST = "relation.request";
    /** 下行：好友申请被处理（ACCEPTED / REJECTED / CANCELLED） */
    public static final String T_RELATION_UPDATE = "relation.update";

    public Frames() {

    }


    // ==== 上行编码 ====

    /**
     * TCP 首帧。网关的认证窗口默认 10s，超时或首帧不是 connect 会回 {@code error 401} 并断开。
     *
     * @param token {@code im.auth.mode=enforce} 时必填；off 模式下可为 null
     */
    public String connect(String userId, String deviceId, String token) {
        Map<String, Object> frame = base(T_CONNECT);
        frame.put("userId", userId);
        if (deviceId != null && !deviceId.isBlank()) {
            frame.put("deviceId", deviceId);
        }
        if (token != null && !token.isBlank()) {
            frame.put("token", token);
        }
        return write(frame);
    }

    /** 由本地消息表的一行渲染上行帧：重投时字段与首次发送完全一致（幂等键不变） */
    public String send(LocalMessage row) {
        return send(row.clientMsgId(), row.to(), row.conversationId(), row.convType(), row.body(),
                row.contentType(), row.attrs(), row.traceId(), row.clientTs());
    }

    public String send(OutgoingMessage message) {
        OutgoingMessage normalized = message.normalized();
        return send(normalized.clientMsgId(), normalized.to(), normalized.conversationId(),
                normalized.convType(), normalized.body(), normalized.contentType(), normalized.attrs(),
                normalized.traceId(), normalized.clientTs());
    }

    private String send(String clientMsgId, String to, String conversationId, String convType,
                        String body, String contentType, String attrs, String traceId, long clientTs) {
        Map<String, Object> frame = base(T_SEND);
        frame.put("clientMsgId", clientMsgId);
        if (to != null && !to.isBlank()) {
            frame.put("to", to);
        }
        if (conversationId != null && !conversationId.isBlank()) {
            frame.put("conversationId", conversationId);
        }
        if (convType != null && !convType.isBlank()) {
            frame.put("convType", convType);
        }
        frame.put("body", body == null ? "" : body);
        if (contentType != null && !contentType.isBlank()) {
            frame.put("contentType", contentType);
        }
        // attrs 只在非空时下发：纯文本帧的形状要和老版本一致（网关/服务端都按「有才读」处理）
        if (attrs != null && !attrs.isBlank()) {
            frame.put("attrs", attrs);
        }
        if (traceId != null && !traceId.isBlank()) {
            frame.put("traceId", traceId);
        }
        if (clientTs > 0) {
            frame.put("clientTs", clientTs);
        }
        return write(frame);
    }

    /**
     * 上报会话水位并触发一次补拉。
     *
     * <p>网关在连接建立时已经自动 PullPending 过一次（watermarks 为空），
     * 客户端紧接着发 sync 是为了<b>补上自己的水位</b>：服务端据此把 seq 下界抬到水位之上。
     */
    public String sync(Map<String, Long> watermarks) {
        return sync(watermarks, false);
    }

    /**
     * 同上，但可以显式要求一次「新端回放」。
     *
     * <p>服务端只在「这台设备第一次上线」时自动回放最近 M 个会话 × 每个最近 N 条
     * （见 {@code NewDeviceReplayService}）。客户端本地库被清空、或者重装了 App 但
     * deviceId 没变时，服务端认为这台设备已经回放过，只能靠这个标志再要一次。
     *
     * <p>重复回放是安全的：本地消息表按 {@code (conversationId, seq)} 去重，
     * 重复帧会被 {@code recordInbound} 直接丢弃。
     */
    public String sync(Map<String, Long> watermarks, boolean replay) {
        Map<String, Object> frame = base(T_SYNC);
        frame.put("watermarks", watermarks == null ? Map.of() : watermarks);
        if (replay) {
            frame.put("replay", true);
        }
        return write(frame);
    }

    /**
     * 端到端确认（批量）。
     *
     * <p>必须批量：逐条 ACK 会把「消息数」放大成「帧数」，移动端还要多付一份电量和流量。
     * 语义是「我见到了 seq=N」，不是「1..N 都收到了」——中间的洞由客户端自己发现（{@code onGap}）。
     */
    public String recv(Collection<DeliveredAck> acks) {
        Map<String, Object> frame = base(T_RECV);
        List<Map<String, Object>> items = new ArrayList<>();
        for (DeliveredAck ack : acks) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("conversationId", ack.conversationId());
            item.put("seq", ack.seq());
            items.add(item);
        }
        frame.put("acks", items);
        return write(frame);
    }

    /** 已读上报：用户级、只前进、服务端会 cap 到会话的 last_seq */
    public String read(String conversationId, long seq) {
        Map<String, Object> frame = base(T_READ);
        frame.put("conversationId", conversationId);
        frame.put("seq", seq);
        return write(frame);
    }

    /**
     * 上报输入状态。
     *
     * <p>会话与对端<b>至少要给一个</b>：单聊给 {@code to} 最省事（服务端按消息路径同一套规则
     * 生成会话 ID），群聊给 {@code conversationId = "g:" + groupId}（或给 {@code to = groupId}
     * 并置 {@code convType = "GROUP"}）。
     *
     * <p>服务端会对 {@code typing=true} 去抖（默认 3 秒），所以宿主可以放心地「边打字边报」，
     * 不必自己算节流——但 {@code typing=false} 一定要发：那是对方那三个点熄灭的唯一信号。
     *
     * @return 本帧是否真的写进了连接（没连上时为 false，不需要排队重发）
     */
    public String typing(String conversationId, String to, String convType, boolean typing) {
        Map<String, Object> frame = base(T_TYPING);
        if (conversationId != null && !conversationId.isBlank()) {
            frame.put("conversationId", conversationId);
        }
        if (to != null && !to.isBlank()) {
            frame.put("to", to);
        }
        if (convType != null && !convType.isBlank()) {
            frame.put("convType", convType);
        }
        frame.put("typing", typing);
        return write(frame);
    }

    /**
     * 查询在线状态（批量）。
     *
     * <p>应答是 {@code presence.snapshot} 帧，经 {@link ImClientListener#onPresenceSnapshot} 回调。
     * 好友列表那种「一次几十上百人」的查询建议走 HTTP {@code GET /presence}（可缓存、可重试）。
     */
    public String presenceGet(Collection<String> userIds) {
        Map<String, Object> frame = base(T_PRESENCE_GET);
        frame.put("userIds", userIds == null ? List.of() : new ArrayList<>(userIds));
        return write(frame);
    }

    /** 主动探测（也用于 NAT 保活）：服务端会回 pong */
    public String ping() {
        Map<String, Object> frame = base(T_PING);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    /** 应答服务端探测。不带 ts 也无妨，但带上便于两端对账 RTT */
    public String pong() {
        Map<String, Object> frame = base(T_PONG);
        frame.put("ts", System.currentTimeMillis());
        return write(frame);
    }

    // ==== 下行解析 ====

    /**
     * 解析下行帧。
     *
     * @return 永远是 typesafe 的结果：不认识/字段缺失都不抛异常
     * @throws IllegalArgumentException 只有「根本不是 JSON 对象」时才抛——
     *                                  那意味着连接上的字节流已经错位，必须断开重连，不能继续猜
     */
    public DownlinkFrame parse(String payload) {
        return parse(JsonUtils.toJsonNode(payload));
    }

    /**
     * 解析已经是 JSON 树的帧（HTTP 响应里的 {@code messages[]} 就是这样拿到的，
     * 不必为了走上面那个重载再 {@code toString()} 一次）。
     */
    public DownlinkFrame parse(JsonNode root) {
        if (root == null || !root.isObject()) {
            throw new IllegalArgumentException("下行帧不是 JSON 对象");
        }
        String type = root.path("type").asString(null);
        if (type == null) {
            return new DownlinkFrame.Unknown(null, root);
        }
        return switch (type) {
            case T_CONNECTED -> new DownlinkFrame.Connected(
                    text(root, "userId"), text(root, "transport"), root.path("ts").asLong(0));
            case T_ACK -> new DownlinkFrame.Ack(
                    text(root, "clientMsgId"), text(root, "serverMsgId"),
                    root.path("seq").asLong(0), root.path("code").asInt(-1), text(root, "traceId"));
            case T_PUSH -> new DownlinkFrame.Push(
                    text(root, "conversationId"), text(root, "convType"),
                    root.path("seq").asLong(0), text(root, "serverMsgId"), text(root, "clientMsgId"),
                    text(root, "from"), text(root, "to"), text(root, "body"),
                    text(root, "contentType"), text(root, "attrs"),
                    root.path("recalled").asBoolean(false), root.path("ts").asLong(0));
            case T_STATE -> new DownlinkFrame.State(
                    text(root, "conversationId"), text(root, "convType"), text(root, "state"),
                    root.path("seq").asLong(0), text(root, "userId"), root.path("ts").asLong(0));
            case T_UPDATE -> new DownlinkFrame.Update(
                    text(root, "conversationId"), text(root, "convType"),
                    root.path("seq").asLong(0), text(root, "change"),
                    text(root, "actorId"), root.path("ts").asLong(0));
            case T_GROUP_UPDATE -> new DownlinkFrame.GroupUpdate(
                    text(root, "groupId"), text(root, "change"), text(root, "actorId"),
                    text(root, "targetId"), root.path("ts").asLong(0));
            case T_ERROR -> new DownlinkFrame.Error(
                    text(root, "clientMsgId"), root.path("code").asInt(-1), text(root, "message"));
            case T_PING -> new DownlinkFrame.Ping(root.path("ts").asLong(0));
            case T_PONG -> new DownlinkFrame.Pong(root.path("ts").asLong(0));
            case T_MIGRATE -> new DownlinkFrame.Migrate(
                    text(root, "reason"), root.path("retryAfterMs").asLong(0),
                    root.path("ts").asLong(0));
            case T_TYPING_PUSH -> new DownlinkFrame.Typing(
                    text(root, "conversationId"), text(root, "convType"), text(root, "userId"),
                    root.path("typing").asBoolean(false), root.path("ts").asLong(0));
            case T_PRESENCE_PUSH -> new DownlinkFrame.Presence(
                    text(root, "userId"), root.path("online").asBoolean(false),
                    root.path("ts").asLong(0));
            case T_PRESENCE_SNAPSHOT -> new DownlinkFrame.PresenceSnapshot(
                    presenceEntriesOf(root), root.path("ts").asLong(0));
            case T_SESSION_LOGIN -> new DownlinkFrame.SessionLogin(
                    text(root, "userId"), text(root, "deviceId"), text(root, "transport"),
                    root.path("ts").asLong(0));
            case T_SESSION_KICK -> new DownlinkFrame.SessionKick(
                    text(root, "deviceId"), text(root, "reason"), root.path("ts").asLong(0));
            case T_CONV_UPDATE -> new DownlinkFrame.ConvUpdate(
                    text(root, "conversationId"),
                    root.path("pinned").asBoolean(false),
                    root.path("muted").asBoolean(false),
                    text(root, "originDeviceId"),
                    root.path("ts").asLong(0));
            case T_RELATION_REQUEST -> new DownlinkFrame.RelationRequest(
                    text(root, "fromId"), text(root, "message"), root.path("ts").asLong(0));
            case T_RELATION_UPDATE -> new DownlinkFrame.RelationUpdate(
                    text(root, "peerId"), text(root, "change"), root.path("ts").asLong(0));
            default -> new DownlinkFrame.Unknown(type, root);
        };
    }

    // ==== 内部 ====

    private Map<String, Object> base(String type) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", type);
        return frame;
    }

    private String write(Map<String, Object> frame) {
        return JsonUtils.toJson(frame);
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asString();
    }

    /**
     * 解析 {@code presence.snapshot} 的 {@code states[]}。
     *
     * <p>跳过缺 userId 的项而不是整帧报错：这是一个展示型应答，
     * 一条脏数据不该让整个好友列表变成空白。
     */
    private static List<DownlinkFrame.PresenceEntry> presenceEntriesOf(JsonNode root) {
        JsonNode states = root.get("states");
        if (states == null || !states.isArray()) {
            return List.of();
        }
        List<DownlinkFrame.PresenceEntry> entries = new ArrayList<>(states.size());
        for (JsonNode item : states) {
            String userId = text(item, "userId");
            if (userId == null || userId.isBlank()) {
                continue;
            }
            entries.add(new DownlinkFrame.PresenceEntry(
                    userId,
                    item.path("online").asBoolean(false),
                    item.path("gateways").asInt(0),
                    item.path("devices").asInt(0),
                    item.path("lastSeen").asLong(0)));
        }
        return entries;
    }

    /** {@code msg.recv} 里的一条：我见到这个会话的 seq = N */
    public record DeliveredAck(String conversationId, long seq) {
    }
}
