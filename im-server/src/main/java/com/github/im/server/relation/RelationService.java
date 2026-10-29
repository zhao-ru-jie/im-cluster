package com.github.im.server.relation;

import com.github.im.server.message.ClientFrames;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.List;

/**
 * 好友申请的状态机：申请 → 通知 → 同意 / 拒绝 / 撤回。
 *
 * <h2>为什么必须有 service 层</h2>
 *
 * <p>在这个类出现之前，{@code RelationController} 直接把 {@code addFriends} 接到 REST 上，
 * 关系链只有「写」没有「流程」。申请流一旦加进来，规则立刻需要一个落脚点：
 * 不能加自己、已是好友不能再申请、被拉黑时的处理、重复申请的幂等、
 * 「我申请他、他也申请我」时直接互加、同意时必须<b>同时</b>写好友边并更新申请状态。
 * 这些散在 controller 里，任何一个入口（HTTP、gRPC、后台脚本）都会漏掉其中几条。
 *
 * <h2>为什么「反向已有申请」直接互加</h2>
 *
 * <p>A 申请 B 的同时 B 也在申请 A，说明双方都想加对方。此时再让谁去点一次「同意」
 * 纯属多余，而且现实中很容易卡死：双方都在等对方审批，列表里两条 PENDING 谁也点不了
 * （他点「同意」同意的是对方的申请，但自己那条还挂着）。
 *
 * <h2>错误码约定</h2>
 *
 * <p>与 {@code GroupService} 一致：{@code 0} 成功，{@code 400} 参数问题，{@code 403} 被拒绝
 * （拉黑），{@code 404} 申请不存在/已处理，{@code 409} 状态冲突（已是好友）。
 * 客户端按 {@code code != 0} 直接展示 {@code message}，不再自己编文案。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class RelationService {

    public static final int CODE_OK = 0;
    public static final int CODE_BAD_REQUEST = 400;
    public static final int CODE_FORBIDDEN = 403;
    public static final int CODE_NOT_FOUND = 404;
    public static final int CODE_CONFLICT = 409;

    private final RelationRepository relations;
    private final FriendRequestRepository requests;
    private final RelationEventNotifier notifier;

    public RelationService(RelationRepository relations,
                           FriendRequestRepository requests,
                           RelationEventNotifier notifier) {
        this.relations = relations;
        this.requests = requests;
        this.notifier = notifier;
    }

    /**
     * 动作结果。
     *
     * @param code      {@code 0} 成功，其余见类注释
     * @param message   可直接展示给用户的中文说明
     * @param request   本次动作涉及的那条申请（列表接口不需要）
     * @param friends   动作后的好友列表（同意时返回，省一次往返）
     */
    public record Result(int code, String message, FriendRequest request, List<String> friends) {

        public boolean ok() {
            return code == CODE_OK;
        }

        static Result ok(String message, FriendRequest request, List<String> friends) {
            return new Result(CODE_OK, message, request, friends == null ? List.of() : friends);
        }

        static Result ok(String message, FriendRequest request) {
            return ok(message, request, List.of());
        }

        static Result fail(int code, String message) {
            return new Result(code, message, null, List.of());
        }
    }

    /* ------------------------------- 申请 ------------------------------- */

    /**
     * 发送好友申请。
     *
     * <p>幂等：同一对用户已有 PENDING 申请时不重复推送，只更新附言——否则对方会被
     * 「同一个人申请了 5 次」的提示淹没，而他其实只是又点了一次按钮。
     */
    public Result request(String fromId, String toId, String message) {
        if (!StringUtils.hasText(fromId) || !StringUtils.hasText(toId)) {
            return Result.fail(CODE_BAD_REQUEST, "userId 不能为空");
        }
        if (fromId.equals(toId)) {
            return Result.fail(CODE_BAD_REQUEST, "不能添加自己为好友");
        }
        if (relations.areFriends(fromId, toId)) {
            return Result.fail(CODE_CONFLICT, "你们已经是好友了");
        }
        if (relations.isBlocked(fromId, toId)) {
            return Result.fail(CODE_FORBIDDEN, "请先把对方移出黑名单");
        }
        if (relations.isBlocked(toId, fromId)) {
            // 不暴露「你被对方拉黑了」：那本身就是信息泄露。对调用方只说「发不出去」
            return Result.fail(CODE_FORBIDDEN, "暂时无法向对方发送好友申请");
        }

        long now = System.currentTimeMillis();
        FriendRequest reverse = requests.find(toId, fromId);
        if (reverse != null && reverse.isPending()) {
            // 双方都在申请对方：直接互加，省掉一次「谁先同意」的死锁
            relations.addFriends(fromId, List.of(toId));
            FriendRequest accepted = reverse.withStatus(FriendRequest.STATUS_ACCEPTED, now);
            requests.save(accepted);
            notifier.notifyUpdate(toId, fromId, ClientFrames.RELATION_ACCEPTED);
            return Result.ok("对方也在申请加你，已互为好友", accepted, relations.friendsOf(fromId));
        }

        FriendRequest existing = requests.find(fromId, toId);
        // existing 存在时保留 createdAt（首次申请时间），只刷新附言与 updatedAt
        FriendRequest next = existing == null
                ? FriendRequest.pending(fromId, toId, message, now)
                : existing.withStatus(FriendRequest.STATUS_PENDING, now).withMessage(message, now);
        requests.save(next);

        boolean duplicate = existing != null && existing.isPending();
        if (!duplicate) {
            notifier.notifyRequest(fromId, toId, next.message());
        }
        return Result.ok(duplicate ? "已发送过申请，等待对方验证" : "好友申请已发送", next);
    }

    /* ------------------------------- 审批 ------------------------------- */

    /** 同意：写双向好友边 + 把申请置为 ACCEPTED + 通知申请人 */
    public Result accept(String userId, String requesterId) {
        if (!StringUtils.hasText(userId) || !StringUtils.hasText(requesterId)) {
            return Result.fail(CODE_BAD_REQUEST, "userId 不能为空");
        }
        FriendRequest request = requests.find(requesterId, userId);
        if (request == null || !request.isPending()) {
            // 终态不可再迁移：连点两下「同意」的第二下走到这里
            return Result.fail(CODE_NOT_FOUND, "好友申请不存在或已处理");
        }
        long now = System.currentTimeMillis();
        relations.addFriends(requesterId, List.of(userId));
        FriendRequest accepted = request.withStatus(FriendRequest.STATUS_ACCEPTED, now);
        requests.save(accepted);
        notifier.notifyUpdate(requesterId, userId, ClientFrames.RELATION_ACCEPTED);
        return Result.ok("已添加为好友", accepted, relations.friendsOf(userId));
    }

    /** 拒绝：不写好友边，通知申请人结果（他需要知道「别再等了」） */
    public Result reject(String userId, String requesterId) {
        if (!StringUtils.hasText(userId) || !StringUtils.hasText(requesterId)) {
            return Result.fail(CODE_BAD_REQUEST, "userId 不能为空");
        }
        FriendRequest request = requests.find(requesterId, userId);
        if (request == null || !request.isPending()) {
            return Result.fail(CODE_NOT_FOUND, "好友申请不存在或已处理");
        }
        FriendRequest rejected = request.withStatus(FriendRequest.STATUS_REJECTED, System.currentTimeMillis());
        requests.save(rejected);
        notifier.notifyUpdate(requesterId, userId, ClientFrames.RELATION_REJECTED);
        return Result.ok("已拒绝该申请", rejected);
    }

    /** 撤回：只能撤自己发出的、仍待处理的那条；记录直接删除（对申请人而言等于没发生） */
    public Result cancel(String userId, String peerId) {
        if (!StringUtils.hasText(userId) || !StringUtils.hasText(peerId)) {
            return Result.fail(CODE_BAD_REQUEST, "userId 不能为空");
        }
        FriendRequest request = requests.find(userId, peerId);
        if (request == null || !request.isPending()) {
            return Result.fail(CODE_NOT_FOUND, "申请不存在或已处理");
        }
        requests.delete(userId, peerId);
        notifier.notifyUpdate(peerId, userId, ClientFrames.RELATION_CANCELLED);
        return Result.ok("已撤回申请", null);
    }

    /* ------------------------------- 查询 ------------------------------- */

    public List<FriendRequest> incoming(String userId) {
        return StringUtils.hasText(userId) ? requests.incoming(userId) : List.of();
    }

    public List<FriendRequest> outgoing(String userId) {
        return StringUtils.hasText(userId) ? requests.outgoing(userId) : List.of();
    }
}
