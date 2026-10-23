package com.github.im.contract;

import java.util.Locale;
import java.util.UUID;

/**
 * 一条待投递的消息信封。
 *
 * <p>相比拆分前的 {@code record MessageEnvelope(String from, String to, String body)}，
 * 这里补齐了拆分后「跨服务传消息」必须具备的两类字段：
 * <ul>
 *   <li><b>幂等</b>：{@link #clientMsgId} —— 网关重试 / 客户端重连重发时，IM 服务据此去重，
 *       保证「至少一次投递」在语义上等价于「恰好一次」。</li>
 *   <li><b>保序</b>：{@link #conversationId} + {@link #convType} —— seq 按会话号分配，
 *       客户端按 seq 排序并拉齐空洞；{@link #clientTs} 仅作参考。</li>
 * </ul>
 *
 * <p>字段缺省全部由紧凑构造器补齐，因此老的客户端报文 {@code {"to":"bob","body":"hi"}}
 * 仍然可用（{@code clientMsgId} 自动生成 UUID，{@code conversationId} 按单聊规范生成）。
 *
 * @param from        发送方 userId
 * @param to          单聊接收方 userId / 群聊群 ID
 * @param body        业务消息体（已序列化，网关与 IM 存储层均不解析）
 * @param clientMsgId 客户端幂等键
 * @param conversationId      会话 ID
 * @param convType    {@link #CONV_SINGLE} / {@link #CONV_GROUP}
 * @param clientTs    客户端发送时间戳（毫秒）
 */
public record MessageEnvelope(String from,
                              String to,
                              String body,
                              String clientMsgId,
                              String conversationId,
                              String convType,
                              Long clientTs) {

    public static final String CONV_SINGLE = "SINGLE";
    public static final String CONV_GROUP = "GROUP";

    public MessageEnvelope {
        if (from == null || from.isBlank()) {
            throw new IllegalArgumentException("from is required");
        }
        if (to == null || to.isBlank()) {
            throw new IllegalArgumentException("to is required");
        }
        if (clientMsgId == null || clientMsgId.isBlank()) {
            clientMsgId = UUID.randomUUID().toString();
        }
        convType = (convType == null || convType.isBlank())
                ? CONV_SINGLE
                : convType.toUpperCase(Locale.ROOT);
        if (conversationId == null || conversationId.isBlank()) {
            conversationId = CONV_GROUP.equals(convType) ? "g:" + to : singleConversationId(from, to);
        }
        if (clientTs == null || clientTs <= 0) {
            clientTs = System.currentTimeMillis();
        }
    }

    /** 单聊会话 ID：两个 userId 排序后拼接，保证 A→B 与 B→A 落在同一会话 */
    public static String singleConversationId(String a, String b) {
        return a.compareTo(b) <= 0 ? "s:" + a + ":" + b : "s:" + b + ":" + a;
    }

    /** 幂等键：发送方 + 客户端消息 ID */
    public String idempotencyKey() {
        return from + "|" + clientMsgId;
    }

    public boolean isGroup() {
        return CONV_GROUP.equals(convType);
    }
}
