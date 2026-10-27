package com.github.im.storage.jdbc.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import com.github.im.server.group.GroupInfo;

/**
 * {@code im_group} 的持久化对象（群资料 + 群级开关）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@TableName("im_group")
public class GroupEntity {

    /** 群 ID 由业务生成（短随机串），不用自增：它要落进会话 ID 与所有消息行 */
    @TableId
    private String groupId;

    private String name;

    private String ownerId;

    private String announcement;

    private Integer maxMembers;

    private Long createdAt;

    /** 0/1：全员禁言 */
    private Integer mutedAll;

    /** 0/1：已解散 */
    private Integer dismissed;

    public static GroupEntity from(GroupInfo info) {
        GroupEntity entity = new GroupEntity();
        entity.setGroupId(info.groupId());
        entity.setName(info.name());
        entity.setOwnerId(info.ownerId());
        entity.setAnnouncement(info.announcement());
        entity.setMaxMembers(info.maxMembers());
        entity.setCreatedAt(info.createdAt());
        entity.setMutedAll(info.mutedAll() ? 1 : 0);
        entity.setDismissed(info.dismissed() ? 1 : 0);
        return entity;
    }

    public GroupInfo toInfo() {
        return new GroupInfo(groupId, name, ownerId, announcement,
                maxMembers == null ? 200 : maxMembers,
                createdAt == null ? 0L : createdAt,
                mutedAll != null && mutedAll == 1,
                dismissed != null && dismissed == 1);
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

    public Integer getMaxMembers() {
        return maxMembers;
    }

    public void setMaxMembers(Integer maxMembers) {
        this.maxMembers = maxMembers;
    }

    public Long getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Long createdAt) {
        this.createdAt = createdAt;
    }

    public Integer getMutedAll() {
        return mutedAll;
    }

    public void setMutedAll(Integer mutedAll) {
        this.mutedAll = mutedAll;
    }

    public Integer getDismissed() {
        return dismissed;
    }

    public void setDismissed(Integer dismissed) {
        this.dismissed = dismissed;
    }
}
