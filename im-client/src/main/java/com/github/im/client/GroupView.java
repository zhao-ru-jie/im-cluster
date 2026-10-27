package com.github.im.client;

/**
 * 群实体快照（服务端各群接口里 {@code group{}} 字段）。
 *
 * @param groupId      群 ID（12 位 base36 随机串，不可枚举）
 * @param name         群名
 * @param ownerId      群主
 * @param announcement 群公告
 * @param maxMembers   人数上限
 * @param createdAt    建群时间（毫秒）
 * @param mutedAll     全员禁言
 * @param dismissed    已解散（解散是逻辑删除，成员集合保留）
 * @author zhaorujie
 * @since 1.0.0
 */
public record GroupView(
        String groupId,
        String name,
        String ownerId,
        String announcement,
        int maxMembers,
        long createdAt,
        boolean mutedAll,
        boolean dismissed) {
}
