package com.github.im.client;

import tools.jackson.databind.JsonNode;

import java.util.List;

/**
 * 下行帧（网关 / IM 服务 → 客户端）的类型化表示。
 *
 * <p>用 sealed + record 而不是「一个大 Map」：客户端要写 {@code switch} 分发，
 * sealed 让「新增帧类型时漏处理」在<b>编译期</b>就暴露（switch 缺分支不通过编译），
 * 而不是等到线上发现某类帧永远被丢进 default。
 *
 * <p>字段一律可空（{@code String} 可能为 null），因为下行帧来自两个不同的生产者
 * （网关只发控制帧，IM 服务发业务帧），历史报文形状并不完全一致——
 * 一个缺字段的老帧不该让整个客户端抛异常。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public sealed interface DownlinkFrame {

    /** 帧类型字面量（与服务端常量一致） */
    String type();

    /** 仅 TCP：认证成功。收到它之前发的业务帧会被服务端拒绝。 */
    record Connected(String userId, String transport, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_CONNECTED;
        }
    }

    /** 上行受理成功（≠ 对方收到）。{@code code != 0} 时表示受理失败。 */
    record Ack(String clientMsgId, String serverMsgId, long seq, int code, String traceId)
            implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_ACK;
        }

        public boolean ok() {
            return code == 0;
        }
    }

    /**
     * 业务消息下行：实时推送、离线补发、历史拉取<b>同一结构</b>。
     *
     * @param body        正文；撤回后为空串
     * @param contentType 缺省 TEXT
     * @param attrs       媒体元数据 JSON；非媒体消息为 null
     * @param recalled    撤回占位（此时 body 为空、attrs 为 null）
     */
    record Push(String conversationId, String convType, long seq, String serverMsgId,
                String clientMsgId, String from, String to, String body,
                String contentType, String attrs, boolean recalled, long ts)
            implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_PUSH;
        }
    }

    /** 状态回执：发给<b>发送方</b>，{@code state} ∈ DELIVERED / READ */
    record State(String conversationId, String convType, String state, long seq,
                 String userId, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_STATE;
        }
    }

    /** 消息变更：{@code change} ∈ RECALLED（会话级广播）/ DELETED（只给操作者自己的端） */
    record Update(String conversationId, String convType, long seq, String change,
                  String actorId, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_UPDATE;
        }
    }

    /** 群治理事件：{@code change} ∈ CREATED / MEMBER_REMOVED / DISMISSED / MUTED ... */
    record GroupUpdate(String groupId, String change, String actorId, String targetId, long ts)
            implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_GROUP_UPDATE;
        }
    }

    /**
     * 服务端错误帧。
     *
     * <p>客户端必须按 {@code code} 分流而不是一律重试：
     * {@code 503} 是可重试（服务端临时不可用），{@code 400/403/404} 重试多少次都一样，
     * {@code 401} 是身份问题——继续重连只会把自己的账号/IP 打进风控。
     */
    record Error(String clientMsgId, int code, String message) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_ERROR;
        }
    }

    /** 空闲探测：必须回 pong（不回则 ~90s 后被断） */
    record Ping(long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_PING;
        }
    }

    /** 客户端 ping 的应答（也用来刷活跃时间） */
    record Pong(long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_PONG;
        }
    }

    /** 摘流/缩容：等 {@code retryAfterMs + 客户端抖动} 后重连 */
    record Migrate(String reason, long retryAfterMs, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_MIGRATE;
        }
    }

    /**
     * 某人正在输入（会话里的其他人）。
     *
     * @param typing false = 停止输入。这个信号必须被 UI 当真：停不下来等于那三个点一直亮着
     */
    record Typing(String conversationId, String convType, String userId, boolean typing, long ts)
            implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_TYPING_PUSH;
        }
    }

    /**
     * 好友的在线状态变化。
     *
     * <p>语义是<b>状态</b>而不是事件：重复收到 {@code online=true} 无害（幂等覆盖），
     * 所以宿主直接按 userId 更新头像亮度即可，不必去重。
     */
    record Presence(String userId, boolean online, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_PRESENCE_PUSH;
        }
    }

    /**
     * 在线状态查询应答（对应上行 {@code presence.get}）。
     *
     * <p>只回给发起查询的那条连接，不会扇出到该用户的其他端——网页刷新好友列表
     * 没有理由让手机也闪一下。
     */
    record PresenceSnapshot(List<PresenceEntry> states, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_PRESENCE_SNAPSHOT;
        }

        /** 便捷查询：没查到的用户在语义上是「未知」，这里按离线返回 */
        public boolean online(String userId) {
            return states.stream()
                    .anyMatch(state -> state.userId().equals(userId) && state.online());
        }
    }

    /** 一个用户的在线状态（与 {@code GET /presence} 的 {@code states[]} 同形） */
    record PresenceEntry(String userId, boolean online, int gateways, int devices, long lastSeen) {
    }

    /**
     * 多端登录通知：同账号的某台设备登录了。
     *
     * <p>推送粒度是用户级，所以<b>刚登录的那台自己也会收到</b>；宿主按
     * {@code deviceId != 自己的 deviceId} 过滤后提示「你的账号在新设备登录」。
     */
    record SessionLogin(String userId, String deviceId, String transport, long ts)
            implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_SESSION_LOGIN;
        }
    }

    /**
     * 踢下线：本连接马上会被服务端关闭。
     *
     * <p>{@link ImClient} 收到它就会停机、不再重连（重连等于自己把自己又踢回去一次），
     * 然后经 {@link ImClientListener#onSessionKick} 交给宿主。
     * {@code reason} 可以直接展示给用户（例如「你在另一台设备登录了」）。
     */
    record SessionKick(String deviceId, String reason, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_SESSION_KICK;
        }
    }

    /**
     * 会话标记变更（置顶 / 免打扰）：同账号其它端改了，本地要跟着改。
     *
     * @param originDeviceId 发起变更的设备；等于自己的 deviceId 时忽略（那是自己操作的回声）
     */
    record ConvUpdate(String conversationId, boolean pinned, boolean muted,
                      String originDeviceId, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_CONV_UPDATE;
        }
    }

    /**
     * 有人申请加你为好友。
     *
     * <p>不是消息帧：申请没有会话、没有 seq、不进离线队列。宿主应该把它变成一条
     * 「新的朋友」入口上的红点，而不是在聊天列表里造一个假会话。
     *
     * @param fromId  申请人
     * @param message 验证消息，可能为空串
     */
    record RelationRequest(String fromId, String message, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_RELATION_REQUEST;
        }
    }

    /**
     * 好友申请被处理。
     *
     * @param peerId 另一方；{@code ACCEPTED/REJECTED} 时是审批人，{@code CANCELLED} 时是申请人
     * @param change ACCEPTED / REJECTED / CANCELLED
     */
    record RelationUpdate(String peerId, String change, long ts) implements DownlinkFrame {
        @Override
        public String type() {
            return Frames.T_RELATION_UPDATE;
        }
    }

    /**
     * 不认识的帧类型。
     *
     * <p>不抛异常、不断连：服务端加新帧时，老客户端只是「不处理」，
     * 而不该因此整条连接崩掉（协议演进的前向兼容就是这么来的）。
     */
    record Unknown(String type, JsonNode raw) implements DownlinkFrame {
    }
}
