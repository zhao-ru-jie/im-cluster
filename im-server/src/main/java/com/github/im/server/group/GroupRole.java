package com.github.im.server.group;

/**
 * 群内角色。
 *
 * <p>只有三档，刻意不做「自定义角色」：群里的权限判断必须能在一次查询内回答，
 * 一旦允许自定义角色，"这个人能不能踢人"就变成读一张权限矩阵——而群消息是热路径，
 * 治理动作是低频操作，两者的实现难度完全不对等。三档能覆盖绝大多数场景，
 * 需要的更细粒度（比如「只能撤销某人发言」）应该在业务层用标签表达，而不是塞进 IM 的鉴权。
 *
 * <p>排序即权限顺序（{@link #ordinal()} 越大权限越高），比较时不需要查表。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public enum GroupRole {

    /** 普通成员：能发言、能退群 */
    MEMBER,
    /** 管理员：能踢普通成员、禁言普通成员、改群公告、@全体 */
    ADMIN,
    /** 群主：管理员的一切 + 设/撤管理员、改名、解散、转让、踢管理员 */
    OWNER;

    /** 能否管理群（改名/公告/全体禁言/解散） */
    public boolean canManageGroup() {
        return this != MEMBER;
    }

    /** 能否管理其他成员（踢人/禁言/设角色），实际还要比较被操作者的角色 */
    public boolean canManageMembers() {
        return this != MEMBER;
    }

    /**
     * 能否操作目标角色。
     *
     * <p>必须是<b>严格大于</b>：管理员不能踢管理员（同级互踢会让群一天内变成无主状态），
     * 群主可以操作管理员，任何人不能操作群主（群主的退出只能走解散或转让）。
     */
    public boolean outranks(GroupRole target) {
        return target != null && ordinal() > target.ordinal();
    }

    /** 解析存储里的字符串；脏数据按最小权限处理，不做「解析失败就放行」 */
    public static GroupRole parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return MEMBER;
        }
        try {
            return valueOf(raw.trim().toUpperCase(java.util.Locale.ROOT));
        } catch (IllegalArgumentException e) {
            return MEMBER;
        }
    }
}
