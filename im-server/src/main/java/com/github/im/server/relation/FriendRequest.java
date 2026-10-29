package com.github.im.server.relation;

/**
 * 一条好友申请。
 *
 * <h2>为什么单独建模，而不是往 {@code im_relation} 里加一种 {@code relation_type}</h2>
 *
 * <p>{@code im_relation} 的唯一键是 {@code (owner, target, relation_type)}，一条边只有
 * 「存在 / 不存在」两种状态，没有附言、没有时间、没有「被拒绝」这种终态之外的中间态。
 * 申请需要 {@code status + message + updatedAt}，塞进边表就意味着给一个只回答
 * 「是不是好友」的热路径表加三个与它无关的列。
 *
 * <h2>状态机</h2>
 *
 * <pre>
 *          ┌────────── accept ──────────> ACCEPTED   （同时写入双向好友边）
 * PENDING ─┼────────── reject ──────────> REJECTED
 *          └────────── cancel ──────────> 记录被删除（申请人撤回，不留痕）
 * </pre>
 *
 * <p>{@code ACCEPTED / REJECTED} 是终态：再次 accept/reject 一律按「已处理」返回 404，
 * 而不是重复执行 —— 那正是「对方连点两下同意」时唯一需要保证的幂等。
 *
 * <p>被拒绝后允许重新申请：{@link #STATUS_REJECTED} 的记录会被新申请覆盖回
 * {@link #STATUS_PENDING}。真实产品里通常还要加「冷却时间/申请次数上限」，
 * 那属于风控，不放在这里。
 *
 * @param fromId    申请人
 * @param toId      被申请人
 * @param status    {@link #STATUS_PENDING} / {@link #STATUS_ACCEPTED} / {@link #STATUS_REJECTED}
 * @param message   验证消息（可为空串）
 * @param createdAt 首次申请时间（重新申请不刷新，用于「骚扰判定」与展示顺序）
 * @param updatedAt 最后一次状态变更时间
 * @author zhaorujie
 * @since 1.0.0
 */
public record FriendRequest(String fromId,
                            String toId,
                            String status,
                            String message,
                            long createdAt,
                            long updatedAt) {

    public static final String STATUS_PENDING = "PENDING";
    public static final String STATUS_ACCEPTED = "ACCEPTED";
    public static final String STATUS_REJECTED = "REJECTED";

    /** 验证消息长度上限：它会被整条塞进推送帧，不设上限等于允许客户端把一篇文章当附言 */
    public static final int MAX_MESSAGE_LENGTH = 120;

    /** 新建一条待处理申请（会裁掉超长附言） */
    public static FriendRequest pending(String fromId, String toId, String message, long now) {
        return new FriendRequest(fromId, toId, STATUS_PENDING, clip(message), now, now);
    }

    public FriendRequest withMessage(String newMessage, long now) {
        return new FriendRequest(fromId, toId, status, clip(newMessage), createdAt, now);
    }

    public FriendRequest withStatus(String newStatus, long now) {
        return new FriendRequest(fromId, toId, newStatus, message, createdAt, now);
    }

    public boolean isPending() {
        return STATUS_PENDING.equals(status);
    }

    public boolean involves(String userId) {
        return fromId.equals(userId) || toId.equals(userId);
    }

    /** 与 {@code userId} 相对的另一方 */
    public String peerOf(String userId) {
        return fromId.equals(userId) ? toId : fromId;
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
