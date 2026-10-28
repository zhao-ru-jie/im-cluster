package com.github.im.storage.mongodb.entity;

import com.github.im.storage.mongodb.MongoCollections;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * {@code im_relation} 的持久化对象：一条文档 = 一条<b>有向边</b>。
 *
 * <p>好友写成两条（A→B、B→A），黑名单写一条（我拉黑了谁）。这样「A 和 B 是不是好友 /
 * A 有没有拉黑 B」永远是 {@code _id} 上的点查，而准入判定是每条单聊消息都要跑一次的热路径。
 * 写放大 2 倍换热路径 O(1)——与 MySQL 形态把 {@code (owner_id, target_id, relation_type)}
 * 做成唯一键是同一个取舍。
 *
 * <p>黑名单单向且不通知被拉黑方：能查到「你被谁拉黑了」本身就是信息泄露。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Document(collection = MongoCollections.RELATION)
@CompoundIndex(name = "uk_relation", def = "{'ownerId': 1, 'kind': 1, 'targetId': 1}", unique = true)
public class RelationEntity {

    /** 好友 */
    public static final String TYPE_FRIEND = "FRIEND";

    /** 黑名单（单向） */
    public static final String TYPE_BLOCK = "BLOCK";

    /** {@code kind + ':' + ownerId + ':' + targetId}：有向边由「类型 + 主体 + 对象」唯一确定 */
    @MongoId
    private String id;

    /** 关系主体 */
    private String ownerId;

    /** 关系对象；被拉黑的那些人（审计/风控）按它反查 */
    @Indexed(name = "idx_relation_target")
    private String targetId;

    /** FRIEND / BLOCK */
    private String kind;

    private long createdAt;

    public static String idOf(String kind, String ownerId, String targetId) {
        return kind + ':' + ownerId + ':' + targetId;
    }

    public static RelationEntity of(String ownerId, String targetId, String kind, long createdAt) {
        RelationEntity entity = new RelationEntity();
        entity.setId(idOf(kind, ownerId, targetId));
        entity.setOwnerId(ownerId);
        entity.setTargetId(targetId);
        entity.setKind(kind);
        entity.setCreatedAt(createdAt);
        return entity;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getTargetId() {
        return targetId;
    }

    public void setTargetId(String targetId) {
        this.targetId = targetId;
    }

    public String getKind() {
        return kind;
    }

    public void setKind(String kind) {
        this.kind = kind;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }
}
