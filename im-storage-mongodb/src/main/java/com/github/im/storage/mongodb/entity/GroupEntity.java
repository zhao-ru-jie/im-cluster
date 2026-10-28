package com.github.im.storage.mongodb.entity;

import com.github.im.server.group.GroupInfo;
import com.github.im.storage.mongodb.MongoCollections;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoId;

/**
 * {@code im_group} 的持久化对象（群资料 + 群级开关）。
 *
 * <p>{@code _id} 直接用 groupId：群 ID 会出现在会话 ID（{@code g:{groupId}}）并落进所有消息文档，
 * 换一次 ID 等于迁移全量数据——与 MySQL 形态把 {@code group_id} 当主键是同一个理由。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Document(collection = MongoCollections.GROUP)
public class GroupEntity {

    /** 群 ID */
    @MongoId
    private String groupId;

    private String name;

    /** 群主：解散/转让的唯一权威；按群主反查（「我建的群」）是唯一的二级查询 */
    @Indexed(name = "idx_group_owner")
    private String ownerId;

    private String announcement;

    private int maxMembers;

    private long createdAt;

    /** 全员禁言：只有群主/管理员能发言 */
    private boolean mutedAll;

    /** 已解散（逻辑删除，历史仍可读） */
    private boolean dismissed;

    public static GroupEntity from(GroupInfo info) {
        GroupEntity entity = new GroupEntity();
        entity.setGroupId(info.groupId());
        entity.setName(info.name());
        entity.setOwnerId(info.ownerId());
        entity.setAnnouncement(info.announcement());
        entity.setMaxMembers(info.maxMembers());
        entity.setCreatedAt(info.createdAt());
        entity.setMutedAll(info.mutedAll());
        entity.setDismissed(info.dismissed());
        return entity;
    }

    /** 缺字段时按「未解散、未全员禁言、上限 200」兜底：读群资料在消息准入的热路径上，不该因脏数据 500 */
    public GroupInfo toInfo() {
        return new GroupInfo(groupId,
                name == null || name.isBlank() ? groupId : name,
                ownerId == null ? "" : ownerId,
                announcement == null ? "" : announcement,
                maxMembers,
                createdAt,
                mutedAll,
                dismissed);
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOwnerId() {
        return ownerId;
    }

    public void setOwnerId(String ownerId) {
        this.ownerId = ownerId;
    }

    public String getAnnouncement() {
        return announcement;
    }

    public void setAnnouncement(String announcement) {
        this.announcement = announcement;
    }

    public int getMaxMembers() {
        return maxMembers;
    }

    public void setMaxMembers(int maxMembers) {
        this.maxMembers = maxMembers;
    }

    public long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public boolean isMutedAll() {
        return mutedAll;
    }

    public void setMutedAll(boolean mutedAll) {
        this.mutedAll = mutedAll;
    }

    public boolean isDismissed() {
        return dismissed;
    }

    public void setDismissed(boolean dismissed) {
        this.dismissed = dismissed;
    }
}
