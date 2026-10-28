package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import org.springframework.stereotype.Component;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 下行帧 {@code msg.push} 的序列化（协议所有者是 IM 服务）。
 *
 * <p>frame 一旦生成就是「完整可下发的报文」，网关通过 {@code PushTask.frame} 原样透传，
 * 不做反序列化。这样新增业务字段（已读回执、卡片消息）不需要改网关。
 *
 * <pre>
 * {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE","seq":12,
 *  "serverMsgId":"0192f0c4-8a3b-7c2d-9f01-4b7a2c8d5e60",
 *  "from":"alice","to":"bob","body":"hi","ts":1750000000000}
 * </pre>
 *
 * <p>实时下发与历史拉取必须产出<b>同一个结构</b>（都从 {@link StoredMessage} 这一份数据渲染），
 * 否则客户端要写两套解析。因此 {@link #pushFrame} 只是「受理时先算出帧」的便捷入口，
 * 真正的模板在 {@link #frameOf}。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
// 显式命名：im-gateway 里也有一个同名类（网关只产出控制帧），避免阶段 1 同进程装配冲突
@Component("serverClientFrames")
public class ClientFrames {

    public static final String TYPE_PUSH = "msg.push";
    /**
     * 消息状态回执：告诉<b>发送方</b>「你发的那条，对方到了哪一步」。
     *
     * <pre>
     * 客户端 → 网关 → IM 服务：msg.recv（已送达） / POST /conv/{id}/read（已读）
     * IM 服务 → 网关 → 发送方：{"ver":1,"type":"msg.state","conversationId":"s:a:b","convType":"SINGLE",
     *                          "state":"READ","seq":12,"userId":"bob","ts":1750000000000}
     * </pre>
     *
     * <p>这是「消息状态机」缺的那一半：{@code msg.ack} 只说「服务端受理了」，
     * 之前没有任何帧把「对方已送达 / 已读」告诉发送方。{@code userId} 是状态的主体
     * （谁已读），发送方据此把「已读」标注到 {@code seq <= seq} 的气泡上。
     */
    public static final String TYPE_STATE = "msg.state";
    /** 已送达（对端设备亲口确认收到） */
    public static final String STATE_DELIVERED = "DELIVERED";
    /** 已读（对端上报已读位置） */
    public static final String STATE_READ = "READ";
    /**
     * 消息变更事件（撤回 / 本地删除）。
     *
     * <pre>
     * {"ver":1,"type":"msg.update","conversationId":"s:a:b","convType":"SINGLE",
     *  "seq":7,"change":"RECALLED","actorId":"alice","ts":1750000000000}
     * </pre>
     *
     * <p>为什么单独一个帧类型而不是复用 {@code msg.state}：{@code state} 是「走到哪一步」
     * （DELIVERED/READ，单调前进、按会话去重），{@code update} 是「内容/可见性变了」
     * （可针对任意 seq、不单调、撤回要广播给所有人而删除只给自己）。语义与去重口径都不同。
     */
    public static final String TYPE_UPDATE = "msg.update";
    /**
     * 群变更事件（治理动作的下行同步）。
     *
     * <pre>
     * {"ver":1,"type":"group.update","groupId":"a1b2c3","change":"MEMBER_REMOVED",
     *  "actorId":"alice","targetId":"bob","ts":1750000000000}
     * </pre>
     *
     * <p>为什么不能塞进 {@code msg.update}：那个帧的定位是「某条消息（会话 + seq）变了」，
     * 而群变更没有 seq，主体是「群」本身。混用会让客户端为了处理「我被踢了」
     * 去解析一个编号字段——语义会在这里崩掉。
     */
    public static final String TYPE_GROUP_UPDATE = "group.update";
    /** 建群（推给初始成员） */
    public static final String GROUP_CREATED = "CREATED";
    /** 群资料变更（改名 / 公告 / 人数上限 / 全员禁言） */
    public static final String GROUP_UPDATED = "UPDATED";
    /** 有人被拉进群 */
    public static final String GROUP_MEMBER_ADDED = "MEMBER_ADDED";
    /** 有人被踢出群 */
    public static final String GROUP_MEMBER_REMOVED = "MEMBER_REMOVED";
    /** 有人主动退群 */
    public static final String GROUP_MEMBER_LEFT = "MEMBER_LEFT";
    /** 角色变更（设/撤管理员） */
    public static final String GROUP_ROLE_CHANGED = "ROLE_CHANGED";
    /** 单人禁言 */
    public static final String GROUP_MUTED = "MUTED";
    /** 解除单人禁言（与 {@link #GROUP_MUTED} 分开：客户端要据此把输入框恢复可用） */
    public static final String GROUP_UNMUTED = "UNMUTED";
    /** 群已解散 */
    public static final String GROUP_DISMISSED = "DISMISSED";

    /**
     * 「正在输入」状态（{@code typing.push}）。
     *
     * <pre>
     * {"ver":1,"type":"typing.push","conversationId":"s:a:b","convType":"SINGLE",
     *  "userId":"alice","typing":true,"ts":1750000000000}
     * </pre>
     *
     * <p>为什么不能复用 {@code msg.state}：那个帧的定位是「某条消息走到了哪一步」，
     * 主体是 {@code seq}；typing 没有消息、没有 seq，主体是「某个人曾经/正在输入」。
     * 混进去就会让客户端为了画三个小点去解析一个不存在的序号。
     *
     * <p>为什么不落库、不进离线队列：一条 5 秒前的「正在输入」现在送出去只会误导
     * （他可能早就发完消息了）。这类帧的正确性由「下一条覆盖上一条」保证，而不是由投递保证。
     */
    public static final String TYPE_TYPING = "typing.push";
    /**
     * 在线状态变化（{@code presence.push}），推给<b>好友</b>。
     *
     * <pre>
     * {"ver":1,"type":"presence.push","userId":"bob","online":true,"ts":1750000000000}
     * </pre>
     *
     * <p>与 {@code typing.push} 一样是「状态」而不是「事件」：重复收到 {@code online:true}
     * 是无害的（客户端幂等覆盖），但<b>错的 offline 有害</b>（好友头像会一直灰着）。
     * 因此下线判定从严：只有确认「路由表里已经没有这个用户」才推。
     */
    public static final String TYPE_PRESENCE = "presence.push";

    /**
     * 多端登录通知（{@code session.login}），推给<b>同一个账号的其它在线设备</b>。
     *
     * <pre>
     * {"ver":1,"type":"session.login","userId":"bob","deviceId":"pad",
     *  "transport":"websocket","ts":1750000000000}
     * </pre>
     *
     * <p>为什么要带 {@code deviceId}：推送粒度只到用户（{@link com.github.im.proto.push.v1.PushTask}
     * 的 target 是 userId），所以这条帧会同时到达所有设备，<b>包括刚登录的那台</b>。
     * 客户端按 {@code deviceId != 自己的} 过滤即可——用「排除某台设备」的字段去做这件事，
     * 需要在推送链路上增加设备粒度寻址，为一条提示帧不值得。
     */
    public static final String TYPE_SESSION_LOGIN = "session.login";

    /**
     * 会话标记变更（{@code conv.update}）：置顶 / 免打扰在这台设备上改了，
     * 同账号的其它设备要跟着变。
     *
     * <pre>
     * {"ver":1,"type":"conv.update","conversationId":"s:a:b","pinned":true,"muted":false,
     *  "originDeviceId":"web","ts":1750000000000}
     * </pre>
     *
     * <p>为什么必须广播：标记是「用户级」状态（存在 {@code im:user:{id}:flags} 里，不分设备），
     * 任何一台设备改完，其它设备本地缓存就过期了。不通知的后果是「手机置顶了，网页还是乱的」，
     * 而且永远不会自愈——直到下次冷启动重新拉会话列表。
     *
     * <p>{@code originDeviceId} 让发起方忽略自己的回声（同样是因为推送粒度只到用户）。
     */
    public static final String TYPE_CONV_UPDATE = "conv.update";

    /** 撤回：会话级，广播给会话内所有人（含发送方自己的其他端） */
    public static final String CHANGE_RECALLED = "RECALLED";
    /** 本地删除：用户级，只推给操作者自己的其他端；对端不受影响 */
    public static final String CHANGE_DELETED = "DELETED";
    private static final int VER = 1;

    private final ObjectMapper objectMapper;

    public ClientFrames(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /** 受理路径用：拿到 seq/serverMsgId 后立刻渲染出要推送的帧 */
    public String pushFrame(MessageEnvelope envelope, String serverMsgId, long seq, long createdAt) {
        return frameOf(new StoredMessage(
                envelope.conversationId(), seq, serverMsgId,
                envelope.from(), envelope.to(), envelope.convType(),
                envelope.clientMsgId(), envelope.body(), createdAt,
                envelope.contentType(), envelope.attrs()));
    }

    /** 读路径用：把存储里的记录还原成帧（历史消息、离线补拉都走这里） */
    public String frameOf(StoredMessage message) {
        return objectMapper.writeValueAsString(frameMap(message));
    }

    /** 同上，但按「是否已撤回」脱敏（补发已撤回的消息时不能再带原文） */
    public String frameOf(StoredMessage message, boolean recalled) {
        return objectMapper.writeValueAsString(frameMap(message, recalled));
    }

    /**
     * 出站状态回执帧（{@code msg.state}）。
     *
     * @param conversationId 会话 ID
     * @param convType       SINGLE / GROUP
     * @param state          {@link #STATE_DELIVERED} / {@link #STATE_READ}
     * @param seq            该用户确认到的最大 seq（发送方把 {@code <= seq} 的气泡都标成该状态）
     * @param userId         状态主体（谁已读/已送达）
     * @param ts             回执生成时间
     */
    public String stateFrame(String conversationId, String convType, String state, long seq,
                             String userId, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_STATE);
        frame.put("conversationId", conversationId);
        frame.put("convType", convType);
        frame.put("state", state);
        frame.put("seq", seq);
        frame.put("userId", userId);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }

    /**
     * 帧的字段模板（唯一来源）。
     *
     * <p>{@code ts} 用存储时的服务端受理时间，而不是渲染时间——否则同一条历史消息
     * 每次拉取的 ts 都会变，客户端排序/去重会出错。
     */
    public Map<String, Object> frameMap(StoredMessage message) {
        return frameMap(message, false);
    }

    /**
     * 同上，但可以渲染「已撤回」的消息。
     *
     * <p>撤回不是删除：seq 仍然占位（否则客户端会当成空洞去反复拉齐），
     * 但那一条<b>不能再带出任何原文</b>——包括 {@code body} 与 {@code attrs}
     * （attrs 里可能是图片 URL，撤回后仍然可访问就等于没撤回）。
     * 所以这里直接把两者抹掉，只留 {@code "recalled":true} 让客户端画「已撤回」占位。
     *
     * @param recalled 该 seq 是否已撤回（由存储层的 {@code recalledSeqs} 批量判定）
     */
    public Map<String, Object> frameMap(StoredMessage message, boolean recalled) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_PUSH);
        frame.put("conversationId", message.conversationId());
        frame.put("convType", message.convType());
        frame.put("seq", message.seq());
        frame.put("serverMsgId", message.serverMsgId());
        frame.put("clientMsgId", message.clientMsgId());
        frame.put("from", message.from());
        frame.put("to", message.to());
        if (recalled) {
            frame.put("body", "");
            frame.put("contentType", MessageEnvelope.CT_TEXT);
            frame.put("recalled", true);
            frame.put("ts", message.createdAt());
            return frame;
        }
        frame.put("body", message.body());
        // 富媒体：类型 + 元数据。attrs 只在非空时下发，纯文本帧保持与旧版一致的长度与形状
        frame.put("contentType", message.contentType());
        if (message.attrs() != null && !message.attrs().isBlank()) {
            frame.put("attrs", message.attrs());
        }
        frame.put("ts", message.createdAt());
        return frame;
    }

    /**
     * 一条已下发帧的定位信息（会话 + seq）。
     *
     * @param conversationId 会话 ID
     * @param seq            会话内序号
     */
    public record FrameRef(String conversationId, long seq) {
    }

    /**
     * 从离线队列里存的帧快照中取出「会话 + seq」。
     *
     * <p>离线队列存的是<b>发送时刻的帧</b>，它不知道后来发生的撤回/删除。所以补发出口处
     * 必须按最新状态重新判定一次——这就需要先从帧里读回定位信息。
     *
     * @return 解析失败（脏帧/格式变化）返回 {@code null}，调用方按「原样下发」处理
     */
    public FrameRef refOf(String rawFrame) {
        if (rawFrame == null || rawFrame.isBlank()) {
            return null;
        }
        try {
            JsonNode node = objectMapper.readTree(rawFrame);
            String conversationId = node.path("conversationId").asString("");
            long seq = node.path("seq").asLong(0);
            return conversationId.isEmpty() || seq <= 0 ? null : new FrameRef(conversationId, seq);
        } catch (RuntimeException e) {
            return null;
        }
    }

    /**
     * 把帧快照改写成「已撤回」形态：抹掉 {@code body} 与 {@code attrs}，加 {@code recalled:true}。
     *
     * <p>为什么不重建一条 {@code StoredMessage}：快照里可能已经带上了当时的形状
     * （老版本帧没有 contentType 等字段），重建反而会让「原样兼容」失效。
     * 只在原 JSON 上做最小改写，剩下的字段（serverMsgId / clientMsgId / ts）保持不动。
     */
    public String recalledFrame(String rawFrame) {
        try {
            JsonNode node = objectMapper.readTree(rawFrame);
            if (!(node instanceof ObjectNode object)) {
                return rawFrame;
            }
            object.put("body", "");
            object.remove("attrs");
            object.put("contentType", MessageEnvelope.CT_TEXT);
            object.put("recalled", true);
            return objectMapper.writeValueAsString(object);
        } catch (RuntimeException e) {
            return rawFrame;
        }
    }

    /**
     * 群变更事件帧。
     *
     * @param change   {@link #GROUP_UPDATED} 等
     * @param actorId  发起人（群主动作是群主，退群是本人）
     * @param targetId 被操作的人（成员增删/角色/禁言时有值，其余为空串）
     */
    public String groupUpdateFrame(String groupId, String change, String actorId, String targetId, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_GROUP_UPDATE);
        frame.put("groupId", groupId);
        frame.put("change", change);
        frame.put("actorId", actorId == null ? "" : actorId);
        frame.put("targetId", targetId == null ? "" : targetId);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }

    /**
     * 「正在输入」帧。
     *
     * @param conversationId 会话 ID（客户端按会话定位输入状态，单个会话同一时刻只显示最后一个人）
     * @param convType       SINGLE / GROUP
     * @param userId         谁在输入（收件人据此决定「是不是对方」而不是自己）
     * @param typing         true=正在输入，false=停止（停止必须及时下发，否则那三个点会一直闪）
     */
    public String typingFrame(String conversationId, String convType, String userId, boolean typing, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_TYPING);
        frame.put("conversationId", conversationId);
        frame.put("convType", convType);
        frame.put("userId", userId);
        frame.put("typing", typing);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }

    /**
     * 在线状态变化帧（推给好友）。
     *
     * <p>只带 {@code online} 布尔值，不带「几台设备」：好友列表要的是「亮/不亮」，
     * 多端细节是用户自己的隐私（几台手机在线不该被好友看见），
     * 需要设备数的只有用户自己，那走 REST 的 {@code /presence}。
     */
    public String presenceFrame(String userId, boolean online, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_PRESENCE);
        frame.put("userId", userId);
        frame.put("online", online);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }

    /**
     * 多端登录通知帧。
     *
     * @param deviceId 登录的那台设备（收件人据此把自己排除掉）
     */
    public String loginFrame(String userId, String deviceId, String transport, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_SESSION_LOGIN);
        frame.put("userId", userId);
        frame.put("deviceId", deviceId);
        frame.put("transport", transport);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }

    /**
     * 会话标记变更帧（置顶 / 免打扰）。
     *
     * @param originDeviceId 发起变更的设备；为空表示变更来自服务端/后台（客户端都该更新）
     */
    public String convUpdateFrame(String conversationId, boolean pinned, boolean muted,
                                  String originDeviceId, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_CONV_UPDATE);
        frame.put("conversationId", conversationId);
        frame.put("pinned", pinned);
        frame.put("muted", muted);
        frame.put("originDeviceId", originDeviceId == null ? "" : originDeviceId);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }

    /**
     * 消息变更事件帧（撤回 / 本地删除）。
     *
     * <p>与 {@link #TYPE_STATE} 的分工：{@code msg.state} 是「这条消息走到了哪一步」
     * （DELIVERED / READ，单调前进），{@code msg.update} 是「这条消息的内容/可见性变了」
     * （RECALLED 会话级、DELETED 用户级，可以针对任意 seq，不单调）。
     * 两者共用同一条下行通道（PushPort），但去重口径不同，所以是两个帧类型。
     *
     * @param change  {@link #CHANGE_RECALLED}（广播给会话内所有人）/ {@link #CHANGE_DELETED}（只给操作者自己的其他端）
     * @param actorId 发起变更的人
     */
    public String updateFrame(String conversationId, String convType, long seq, String change,
                              String actorId, long ts) {
        Map<String, Object> frame = new LinkedHashMap<>();
        frame.put("ver", VER);
        frame.put("type", TYPE_UPDATE);
        frame.put("conversationId", conversationId);
        frame.put("convType", convType);
        frame.put("seq", seq);
        frame.put("change", change);
        frame.put("actorId", actorId);
        frame.put("ts", ts);
        return objectMapper.writeValueAsString(frame);
    }
}
