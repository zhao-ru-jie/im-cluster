package com.github.im.server.group;

/**
 * 一条入群申请。
 *
 * <h2>为什么不复用 {@code FriendRequest}</h2>
 *
 * <p>两者形状相似（申请人、附言、状态机），但<b>审批轴完全不同</b>：好友申请只有一个审批人
 * （{@code toId}），群的审批人是「群主 + 全部管理员」这个<b>会变的集合</b>。硬套好友申请会得到
 * 一个永远填不对的 {@code toId}，而审批权限的判定又必须回到群角色表——于是同一个问题有
 * 两个答案。所以这里只保留 {@code groupId + applicantId}，谁能审批由 {@link GroupRole} 决定。
 *
 * <h2>状态机</h2>
 *
 * <pre>
 *          ┌────────── approve ─────────> APPROVED   （同时加成员 / 写会话）
 * PENDING ─┼────────── reject  ─────────> REJECTED
 *          └────────── cancel  ─────────> 记录被删除（申请人撤回，不留痕）
 * </pre>
 *
 * <p>{@code APPROVED / REJECTED} 是终态，重复审批按「已处理」返回 404——群场景下这尤其重要：
 * 多个管理员同时看到待审列表是常态，谁点慢了一步都会撞上同一个申请。
 *
 * <p>被拒绝后允许重新申请：{@code REJECTED} 的记录会被新申请覆盖回 {@code PENDING}。
 * 冷却时间/封禁属于风控，不放在这里。
 *
 * @param groupId     目标群
 * @param applicantId 申请人
 * @param status      {@link #STATUS_PENDING} / {@link #STATUS_APPROVED} / {@link #STATUS_REJECTED}
 * @param message     申请理由（可为空串）
 * @param createdAt   首次申请时间（重新申请不刷新，用于排序与「谁等了多久」）
 * @param updatedAt   最后一次状态变更时间
 * @author zhaorujie
 * @since 1.0.0
 */
public record GroupJoinRequest(String groupId,
                               String applicantId,
                               String status,
                               String message,
                               long createdAt,
                               long updatedAt) {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_APPROVED = "APPROVED";
    public static final String STATUS_REJECTED = "REJECTED";

    /** 申请理由长度上限：它会随审批列表整条下发，不设上限等于允许把一篇文章当理由 */
    public static final int MAX_MESSAGE_LENGTH = 120;

    public static GroupJoinRequest pending(String groupId, String applicantId, String message, long now) {
        return pending(groupId, applicantId, message, now, now);
    }

    /**
     * 重新申请：{@code createdAt} 保留首次申请的时间。
     *
     * <p>不刷新它有两个用处：审批列表按「谁等了最久」排序不会被反复申请顶到最后，
     * 以及「同一人一天内申请了 40 次」这种骚扰信号还看得出来（附言会随每次申请更新）。
     */
    public static GroupJoinRequest pending(String groupId, String applicantId, String message,
                                           long createdAt, long updatedAt) {
        return new GroupJoinRequest(groupId, applicantId, STATUS_PENDING, clip(message), createdAt, updatedAt);
    }

    public GroupJoinRequest withStatus(String newStatus, long now) {
        return new GroupJoinRequest(groupId, applicantId, newStatus, message, createdAt, now);
    }

    public boolean isPending() {
        return STATUS_PENDING.equals(status);
    }

    private static String clip(String message) {
        if (message == null || message.isBlank()) {
            return "";
        }
        String trimmed = message.strip();
        return trimmed.length() <= MAX_MESSAGE_LENGTH
                ? trimmed
                : trimmed.substring(0, MAX_MESSAGE_LENGTH);
    }
}
