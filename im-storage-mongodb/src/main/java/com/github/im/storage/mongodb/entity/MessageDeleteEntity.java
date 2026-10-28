package com.github.im.storage.mongodb.entity;

import com.github.im.storage.mongodb.MongoCollections;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * {@code im_message_delete} 的持久化对象：用户级「我删了这条」。
 *
 * <p>为什么不给消息文档加个「被谁删过」的数组：删除是<b>用户级</b>的（我删了对方还要看），
 * 一条群消息可能被 N 个人各删一次，数组会随着会话规模无限增长、还会和消息本身抢同一个文档的写锁。
 * 独立集合 + {@code _id = userId:conversationId:seq} 让「删过没有」变成一次点查/一次幂等插入。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Document(collection = MongoCollections.MESSAGE_DELETE)
@CompoundIndexes({
        // 读路径是「某人在某会话里删了哪些 seq」
        @CompoundIndex(name = "idx_msg_delete_user", def = "{'userId': 1, 'conversationId': 1}"),
        @CompoundIndex(name = "idx_msg_delete_conv", def = "{'conversationId': 1, 'seq': 1}")
})
public class MessageDeleteEntity {

    /** {@code userId + ':' + conversationId + ':' + seq}：重复删除天然被 _id 挡住 */
    @MongoId
    private String id;

    /** 谁删的 */
    private String userId;

    private String conversationId;

    /** 删掉的是这个会话里的第几号 */
    private long seq;

    private long createdAt;

    public static String idOf(String userId, String conversationId, long seq) {
        return userId + ':' + conversationId + ':' + seq;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
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

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }
}
