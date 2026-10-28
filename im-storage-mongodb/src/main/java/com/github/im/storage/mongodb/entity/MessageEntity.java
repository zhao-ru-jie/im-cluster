package com.github.im.storage.mongodb.entity;

import com.github.im.server.message.StoredMessage;
import com.github.im.storage.mongodb.MongoCollections;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * {@code im_message} 的持久化对象（一条文档 = 一条已受理消息）。
 *
 * <h2>为什么 {@code _id} 用 serverMsgId，而不是「会话 + seq」</h2>
 *
 * <p>一条消息的<b>身份</b>就是 {@code serverMsgId}（UUIDv7）：它出现在受理响应、
 * 下行帧、客户端回执里，客户端认的是它；{@code (conversationId, seq)} 是<b>定位</b>
 * （排序、漫游、补发的游标），不是身份。MySQL 形态也是这个划分——自增主键才是行身份，
 * {@code uk_msg_conv_seq} 是另外一条唯一键。
 *
 * <p>把「会话 + seq」拼成 {@code _id} 有两个实际代价：
 * <ol>
 *   <li><b>多一条无用索引</b>：{@code _id} 上的唯一约束与 {@code uk_msg_conv_seq} 完全重复。
 *       而 {@code _id} 索引<b>又不能删</b>、也不能替代后者——字符串 {@code conv:9} 与
 *       {@code conv:10} 的字典序不等于序号大小，{@code seq > afterSeq} 的范围扫描用不上它，
 *       所以 {@code (conversationId, seq)} 这条有序索引仍然必须存在；</li>
 *   <li><b>写放大且不友好</b>：UUIDv7 全局按时间递增，{@code _id} 索引是「尾部追加」；
 *       而「会话 + seq」是跨会话交错插入的，等于在这棵 B 树中间到处打洞。</li>
 * </ol>
 *
 * <p>换成 {@code _id = serverMsgId} 之后，唯一的损失是「按 (conv, seq) 点查」从 {@code _id}
 * 查找变成 {@code uk_msg_conv_seq} 查找——都是一次索引 seek，代价可以忽略。
 * 重复写入的拦截也从 {@code _id} 转移到 {@code uk_msg_conv_seq} 唯一索引上，
 * 保证强度不变（同样抛 {@code DuplicateKeyException}）。
 *
 * <h2>索引</h2>
 *
 * <ul>
 *   <li>{@code _id}（= serverMsgId）：隐式唯一，不再需要额外的 {@code uk_msg_server_id}；</li>
 *   <li>{@code uk_msg_conv_seq}：保序、漫游拉取、离线补发的范围扫描，同时是
 *       「发号器不重复」与「同一条消息不会被写两次」的最终防线；</li>
 *   <li>{@code uk_msg_idem}：幂等防线（同一发送方的同一 clientMsgId 只允许落一条，
 *       Redis 幂等表过期后靠它兜底）。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Document(collection = MongoCollections.MESSAGE)
@CompoundIndexes({
        // 保序 + 漫游拉取 + 离线补发的范围扫描主索引；同时兜住「同一 seq 不会被写两次」
        @CompoundIndex(name = "uk_msg_conv_seq", def = "{'conversationId': 1, 'seq': 1}", unique = true),
        // 幂等的最终防线：Redis 幂等表过期或丢数据时，仍能拒绝重复落库
        @CompoundIndex(name = "uk_msg_idem", def = "{'from': 1, 'clientMsgId': 1}", unique = true)
})
public class MessageEntity {

    /**
     * 服务端消息 ID（UUIDv7，全局唯一，与 seq 解耦）。
     *
     * <p>映射到 {@code _id}：它就是这条消息的身份，唯一性由 {@code _id} 索引本身保证。
     * 非空是硬要求——见 {@code MongoMessageRepository#append} 的前置校验。
     */
    @MongoId
    private String serverMsgId;

    /** 会话 ID：单聊 {@code s:{a}:{b}}，群聊 {@code g:{groupId}} */
    private String conversationId;

    /** 会话内单调序号（保序与拉齐依据） */
    private long seq;

    /** 发送方 userId */
    private String from;

    /** 单聊接收方 / 群 ID */
    private String to;

    /** SINGLE / GROUP */
    private String convType;

    /** 客户端幂等键 */
    private String clientMsgId;

    /** 业务消息体（网关透传，服务端不解析） */
    private String body;

    /** 服务端受理时间（毫秒） */
    private long createdAt;

    /** 消息类型：TEXT / IMAGE / FILE / VOICE / VIDEO / CUSTOM */
    private String contentType;

    /** 媒体元数据 JSON（url/size/name/duration/...），服务端不解析 */
    private String attrs;

    /** 撤回时间（毫秒），null = 未撤回；不放进 StoredMessage，渲染由 recalledSeqs 批量判定 */
    private Long recalledAt;

    public static MessageEntity from(StoredMessage message) {
        MessageEntity entity = new MessageEntity();
        entity.setServerMsgId(message.serverMsgId());
        entity.setConversationId(message.conversationId());
        entity.setSeq(message.seq());
        entity.setFrom(message.from());
        entity.setTo(message.to());
        entity.setConvType(message.convType());
        entity.setClientMsgId(message.clientMsgId());
        entity.setBody(message.body());
        entity.setCreatedAt(message.createdAt());
        entity.setContentType(message.contentType());
        entity.setAttrs(message.attrs());
        return entity;
    }

    public StoredMessage toStoredMessage() {
        return new StoredMessage(conversationId, seq, serverMsgId, from, to, convType, clientMsgId, body,
                createdAt, contentType, attrs);
    }

    public String getServerMsgId() {
        return serverMsgId;
    }

    public void setServerMsgId(String serverMsgId) {
        this.serverMsgId = serverMsgId;
    }

    public String getConversationId() {
        return conversationId;
    }

    public void setConversationId(String conversationId) {
        this.conversationId = conversationId;
    }

    public long getSeq() {
        return seq;
    }

    public void setSeq(long seq) {
        this.seq = seq;
    }

    public String getFrom() {
        return from;
    }

    public void setFrom(String from) {
        this.from = from;
    }

    public String getTo() {
        return to;
    }

    public void setTo(String to) {
        this.to = to;
    }

    public String getConvType() {
        return convType;
    }

    public void setConvType(String convType) {
        this.convType = convType;
    }

    public String getClientMsgId() {
        return clientMsgId;
    }

    public void setClientMsgId(String clientMsgId) {
        this.clientMsgId = clientMsgId;
    }

    public String getBody() {
        return body;
    }

    public void setBody(String body) {
        this.body = body;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public String getContentType() {
        return contentType;
    }

    public void setContentType(String contentType) {
        this.contentType = contentType;
    }

    public String getAttrs() {
        return attrs;
    }

    public void setAttrs(String attrs) {
        this.attrs = attrs;
    }

    public Long getRecalledAt() {
        return recalledAt;
    }

    public void setRecalledAt(Long recalledAt) {
        this.recalledAt = recalledAt;
    }
}
