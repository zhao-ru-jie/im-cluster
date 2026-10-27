package com.github.im.server.group;

/**
 * 群实体（群资料 + 群级开关）。
 *
 * <p>在此之前「群」只是一个 ID 和一个成员集合：没有群名、没有群主、没有公告，
 * 于是做不了「谁能解散这个群」「全员禁言」这类最基础的治理动作——
 * 而这类动作恰恰是把「能聊天」变成「能用的群」的分界线。
 *
 * @param groupId      群 ID（会话 ID 是 {@code g:{groupId}}）
 * @param name         群名（展示用，允许重名）
 * @param ownerId      群主；解散/转让的唯一权威
 * @param announcement 群公告（纯文本，客户端展示）
 * @param maxMembers   人数上限（建群时定，可改）
 * @param createdAt    创建时间（毫秒）
 * @param mutedAll     全员禁言：只有 OWNER/ADMIN 能发言
 * @param dismissed    已解散；解散后消息一律拒绝，历史仍可读
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record GroupInfo(String groupId,
                        String name,
                        String ownerId,
                        String announcement,
                        int maxMembers,
                        long createdAt,
                        boolean mutedAll,
                        boolean dismissed) {

    /** 人数上限的硬边界：再大就不是「群」而是「广播」，扇出与成员表都撑不住 */
    public static final int MAX_MEMBERS_LIMIT = 5_000;

    public static GroupInfo create(String groupId, String name, String ownerId, int maxMembers, long now) {
        return new GroupInfo(groupId, name, ownerId, "", clampMaxMembers(maxMembers), now, false, false);
    }

    public GroupInfo {
        if (announcement == null) {
            announcement = "";
        }
        if (name == null || name.isBlank()) {
            name = groupId;
        }
        maxMembers = clampMaxMembers(maxMembers);
    }

    /** 把时长/上限之类的入参夹到合法区间，避免调用方传 0 或负数把群变成不可用 */
    public static int clampMaxMembers(int maxMembers) {
        if (maxMembers <= 0) {
            return 200;
        }
        return Math.min(maxMembers, MAX_MEMBERS_LIMIT);
    }

    public GroupInfo withName(String newName) {
        return new GroupInfo(groupId, newName, ownerId, announcement, maxMembers, createdAt, mutedAll, dismissed);
    }

    public GroupInfo withAnnouncement(String text) {
        return new GroupInfo(groupId, name, ownerId, text, maxMembers, createdAt, mutedAll, dismissed);
    }

    public GroupInfo withMaxMembers(int max) {
        return new GroupInfo(groupId, name, ownerId, announcement, clampMaxMembers(max), createdAt, mutedAll, dismissed);
    }

    public GroupInfo withMutedAll(boolean muted) {
        return new GroupInfo(groupId, name, ownerId, announcement, maxMembers, createdAt, muted, dismissed);
    }

    public GroupInfo withOwner(String newOwnerId) {
        return new GroupInfo(groupId, name, newOwnerId, announcement, maxMembers, createdAt, mutedAll, dismissed);
    }

    public GroupInfo dismissedAt(long now) {
        return new GroupInfo(groupId, name, ownerId, announcement, maxMembers, createdAt, mutedAll, true);
    }
}
