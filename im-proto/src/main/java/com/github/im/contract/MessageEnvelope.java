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
 * <h2>富媒体（{@link #contentType} + {@link #attrs}）</h2>
 *
 * <p>消息体本身始终是 {@link #body} 这一个字符串，富媒体<b>不改变 body 的地位</b>：
 * <ul>
 *   <li>{@code contentType} 决定气泡怎么渲染（{@code TEXT} 是纯文本，{@code IMAGE} /
 *       {@code FILE} / {@code VOICE} / {@code VIDEO} 走媒体气泡，{@code CUSTOM} 交给业务方）；</li>
 *   <li>{@code attrs} 是媒体元数据的 JSON 串（{@code url}、{@code size}、{@code name}、
 *       {@code duration}、{@code width}/{@code height}、{@code thumb} 等），
 *       网关与 IM 服务<b>都不解析</b>——媒体字段的演进速度远快于消息通道，逐字段建模
 *       只会让每次加一个属性都动一次协议与表结构。</li>
 * </ul>
 *
 * <p>文件本体不经本通道传输：客户端直传对象存储，这里只走 URL。把二进制塞进消息链路
 * 会让 seq 发号、幂等表、离线队列全部被大对象拖慢，而它们都不需要看到字节。
 *
 * <p>字段缺省全部由紧凑构造器补齐，因此老的客户端报文 {@code {"to":"bob","body":"hi"}}
 * 仍然可用（{@code clientMsgId} 自动生成 UUID，{@code conversationId} 按单聊规范生成，
 * {@code contentType} 缺省为 {@code TEXT}）。
 *
 * @param from        发送方 userId
 * @param to          单聊接收方 userId / 群聊群 ID
 * @param body        业务消息体（已序列化，网关与 IM 存储层均不解析）；媒体消息里通常放摘要文本
 * @param clientMsgId 客户端幂等键
 * @param conversationId      会话 ID
 * @param convType    {@link #CONV_SINGLE} / {@link #CONV_GROUP}
 * @param clientTs    客户端发送时间戳（毫秒）
 * @param contentType 消息类型，见 {@link #CT_TEXT} 等；缺省 {@code TEXT}
 * @param attrs       媒体元数据 JSON，纯文本消息为空串
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record MessageEnvelope(String from,
                              String to,
                              String body,
                              String clientMsgId,
                              String conversationId,
                              String convType,
                              Long clientTs,
                              String contentType,
                              String attrs) {

    public static final String CONV_SINGLE = "SINGLE";
    public static final String CONV_GROUP = "GROUP";

    /** 纯文本：body 就是给人看的内容 */
    public static final String CT_TEXT = "TEXT";
    /** 图片：body 可为空或占位摘要，真实地址在 attrs.url */
    public static final String CT_IMAGE = "IMAGE";
    /** 文件：attrs 需带 url/name/size */
    public static final String CT_FILE = "FILE";
    /** 语音：attrs 需带 url/duration */
    public static final String CT_VOICE = "VOICE";
    /** 视频：attrs 需带 url/duration/width/height/thumb */
    public static final String CT_VIDEO = "VIDEO";
    /** 业务自定义：渲染方式由接入方约定，服务端只负责投递 */
    public static final String CT_CUSTOM = "CUSTOM";

    /** 老报文兼容：不带富媒体字段时按纯文本处理 */
    public MessageEnvelope(String from,
                           String to,
                           String body,
                           String clientMsgId,
                           String conversationId,
                           String convType,
                           Long clientTs) {
        this(from, to, body, clientMsgId, conversationId, convType, clientTs, null, null);
    }

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
        contentType = (contentType == null || contentType.isBlank())
                ? CT_TEXT
                : contentType.toUpperCase(Locale.ROOT);
        if (attrs == null) {
            attrs = "";
        }
    }

    /** 单聊会话 ID：两个 userId 排序后拼接，保证 A→B 与 B→A 落在同一会话 */
    public static String singleConversationId(String a, String b) {
        return a.compareTo(b) <= 0 ? ("s:" + a + ":" + b) : ("s:" + b + ":" + a);
    }

    /** 幂等键：发送方 + 客户端消息 ID */
    public String idempotencyKey() {
        return from + "|" + clientMsgId;
    }

    public boolean isGroup() {
        return CONV_GROUP.equals(convType);
    }

    /** 是否纯文本（决定会话列表用 body 还是类型占位作为摘要） */
    public boolean isText() {
        return CT_TEXT.equals(contentType);
    }
}
