package com.github.im.server.controller;

import com.github.im.server.group.GroupInfo;
import com.github.im.server.group.GroupInvite;
import com.github.im.server.group.GroupJoinRequest;
import com.github.im.server.group.GroupService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 入群：申请审批与邀请码（邀请链接 / 二维码）。
 *
 * <pre>
 * POST   /group/{groupId}/join-requests?userId=&message=            提交入群申请
 * GET    /group/{groupId}/join-requests/{userId}                    我在这群里的申请状态
 * GET    /group/{groupId}/join-requests?operatorId=&offset=&limit=   待审列表（群主/管理员，分页）
 * POST   /group/{groupId}/join-requests/{userId}/approve?operatorId=  通过
 * POST   /group/{groupId}/join-requests/{userId}/reject?operatorId=   拒绝
 *
 * POST   /group/{groupId}/invites?operatorId=&ttlHours=&maxUses=&approvalRequired=  生成邀请码
 * GET    /group/{groupId}/invites?operatorId=                       当前有效邀请码
 * DELETE /group/{groupId}/invites/{code}?operatorId=                吊销
 * POST   /group/join?userId=&code=                                  用邀请码加入（可能需要审批）
 * </pre>
 *
 * <h2>为什么扫二维码不需要单独的接口</h2>
 *
 * <p>二维码就是这条邀请链接的图形化：{@code im://group/join?code=xxxx}。扫码之后客户端
 * 拿到的仍然是 {@code code}，走的仍然是 {@code POST /group/join}。服务端只为「码」建模，
 * 避免出现「二维码表」和「邀请码表」两份必须保持一致的数据。
 *
 * <h2>权限</h2>
 *
 * <p>全部判断在 {@link GroupService}：待审列表与邀请码管理限群主/管理员，申请与用码加入限本人。
 * 这个类只做参数搬运——把规则写在 controller 会与消息准入的判断分叉。
 *
 * <p>与其它接口同一前提：接入层负责鉴权，这里的 {@code userId} / {@code operatorId}
 * 被当成已认证身份（REST 无鉴权是本项目已知短板，见 README「安全」一节）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/group")
public class GroupJoinController {

    private final GroupService groupService;

    public GroupJoinController(GroupService groupService) {
        this.groupService = groupService;
    }

    // ==== 入群申请 ====

    @PostMapping("/{groupId}/join-requests")
    public Map<String, Object> requestJoin(@PathVariable String groupId,
                                           @RequestParam(name = "userId") String userId,
                                           @RequestParam(name = "message", defaultValue = "") String message) {
        GroupService.JoinRequestResult result = groupService.requestJoin(groupId, userId, message);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", result.code());
        body.put("message", result.message());
        if (result.request() != null) {
            body.put("request", view(result.request()));
        }
        return body;
    }

    @GetMapping("/{groupId}/join-requests/{userId}")
    public Map<String, Object> joinRequestOf(@PathVariable String groupId,
                                             @PathVariable String userId) {
        GroupService.JoinRequestResult result = groupService.joinRequestOf(groupId, userId);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", result.code());
        body.put("message", result.message());
        if (result.request() != null) {
            body.put("request", view(result.request()));
        }
        return body;
    }

    @GetMapping("/{groupId}/join-requests")
    public Map<String, Object> pendingJoins(@PathVariable String groupId,
                                            @RequestParam(name = "operatorId") String operatorId,
                                            @RequestParam(name = "offset", defaultValue = "0") int offset,
                                            @RequestParam(name = "limit", defaultValue = "0") int limit) {
        GroupService.JoinRequestPage page = groupService.pendingJoins(operatorId, groupId, offset, limit);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", page.code());
        body.put("message", page.message());
        if (page.code() != 0) {
            return body;
        }
        body.put("groupId", groupId);
        body.put("total", page.total());
        body.put("offset", page.offset());
        body.put("limit", page.limit());
        body.put("requests", page.requests().stream().map(GroupJoinController::view).toList());
        return body;
    }

    @PostMapping("/{groupId}/join-requests/{userId}/approve")
    public Map<String, Object> approve(@PathVariable String groupId,
                                       @PathVariable String userId,
                                       @RequestParam(name = "operatorId") String operatorId) {
        return result(groupService.approveJoin(operatorId, groupId, userId));
    }

    @PostMapping("/{groupId}/join-requests/{userId}/reject")
    public Map<String, Object> reject(@PathVariable String groupId,
                                      @PathVariable String userId,
                                      @RequestParam(name = "operatorId") String operatorId) {
        return result(groupService.rejectJoin(operatorId, groupId, userId));
    }

    // ==== 邀请码 / 邀请链接 / 二维码 ====

    @PostMapping("/{groupId}/invites")
    public Map<String, Object> createInvite(@PathVariable String groupId,
                                            @RequestParam(name = "operatorId") String operatorId,
                                            @RequestParam(name = "ttlHours", defaultValue = "0") long ttlHours,
                                            @RequestParam(name = "maxUses", defaultValue = "0") int maxUses,
                                            @RequestParam(name = "approvalRequired", defaultValue = "false")
                                            boolean approvalRequired) {
        GroupService.InviteResult result = groupService.createInvite(
                operatorId, groupId, ttlHours, maxUses, approvalRequired);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", result.code());
        body.put("message", result.message());
        if (result.invite() != null) {
            body.put("invite", view(result.invite()));
            // link 直接给客户端展示/复制；二维码由客户端拿 code 自己生成
            body.put("link", result.link());
        }
        return body;
    }

    @GetMapping("/{groupId}/invites")
    public Map<String, Object> invites(@PathVariable String groupId,
                                       @RequestParam(name = "operatorId") String operatorId) {
        GroupService.InvitePage page = groupService.invites(operatorId, groupId);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", page.code());
        body.put("message", page.message());
        if (page.code() != 0) {
            return body;
        }
        body.put("groupId", groupId);
        body.put("linkBase", page.linkBase());
        List<Map<String, Object>> invites = new ArrayList<>(page.invites().size());
        for (GroupInvite invite : page.invites()) {
            Map<String, Object> item = view(invite);
            item.put("link", invite.link(page.linkBase()));
            invites.add(item);
        }
        body.put("invites", invites);
        return body;
    }

    @DeleteMapping("/{groupId}/invites/{code}")
    public Map<String, Object> revokeInvite(@PathVariable String groupId,
                                            @PathVariable String code,
                                            @RequestParam(name = "operatorId") String operatorId) {
        return result(groupService.revokeInvite(operatorId, groupId, code));
    }

    /**
     * 用邀请码加入。
     *
     * <p>{@code approvalPending=true} 表示这个码需要审批：此次调用只是提交了一条申请，
     * 客户端应显示「已申请，等待管理员通过」而不是「已加入」。
     */
    @PostMapping("/join")
    public Map<String, Object> join(@RequestParam(name = "userId") String userId,
                                    @RequestParam(name = "code") String code) {
        GroupService.JoinByInviteResult result = groupService.joinByInvite(userId, code);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", result.code());
        body.put("message", result.message());
        if (result.groupId() != null) {
            body.put("groupId", result.groupId());
            body.put("approvalPending", result.approvalPending());
            if (result.group() != null) {
                body.put("group", view(result.group()));
            }
        }
        return body;
    }

    // ==== JSON 拼装 ====

    private Map<String, Object> result(GroupService.GroupResult result) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", result.code());
        body.put("message", result.message());
        if (result.group() != null) {
            body.put("group", view(result.group()));
        }
        return body;
    }

    private static Map<String, Object> view(GroupJoinRequest request) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("groupId", request.groupId());
        item.put("applicantId", request.applicantId());
        item.put("status", request.status());
        item.put("message", request.message());
        item.put("createdAt", request.createdAt());
        item.put("updatedAt", request.updatedAt());
        return item;
    }

    private static Map<String, Object> view(GroupInvite invite) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("code", invite.code());
        item.put("groupId", invite.groupId());
        item.put("inviterId", invite.inviterId());
        item.put("createdAt", invite.createdAt());
        item.put("expiresAt", invite.expiresAt());
        item.put("maxUses", invite.maxUses());
        item.put("uses", invite.uses());
        item.put("approvalRequired", invite.approvalRequired());
        return item;
    }

    private static Map<String, Object> view(GroupInfo info) {
        Map<String, Object> group = new LinkedHashMap<>();
        group.put("groupId", info.groupId());
        group.put("name", info.name());
        group.put("ownerId", info.ownerId());
        group.put("announcement", info.announcement());
        group.put("maxMembers", info.maxMembers());
        group.put("createdAt", info.createdAt());
        group.put("mutedAll", info.mutedAll());
        group.put("dismissed", info.dismissed());
        return group;
    }
}
