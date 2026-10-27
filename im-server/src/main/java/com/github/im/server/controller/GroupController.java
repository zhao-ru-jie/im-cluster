package com.github.im.server.controller;

import com.github.im.server.group.GroupInfo;
import com.github.im.server.group.GroupRole;
import com.github.im.server.group.GroupService;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 群治理入口（群资料 / 成员 / 角色 / 禁言 / 解散）。
 *
 * <pre>
 * POST   /group?ownerId=alice&name=销售群                    建群（返回不可枚举的 groupId）
 * GET    /group/{groupId}                                    群详情（成员 + 角色 + 禁言）
 * GET    /group/{groupId}/members                            成员列表（含角色、禁言）
 * POST   /group/{groupId}/rename?operatorId=&name=           改名（仅群主）
 * POST   /group/{groupId}/announcement?operatorId=&text=     公告（群主/管理员）
 * POST   /group/{groupId}/mute-all?operatorId=&muted=true    全员禁言（群主/管理员）
 * POST   /group/{groupId}/max-members?operatorId=&max=500    人数上限（仅群主）
 * POST   /group/{groupId}/members?operatorId=                拉人（body: ["bob","carol"]）
 * DELETE /group/{groupId}/members/{targetId}?operatorId=     踢人（角色需严格高于对方）
 * POST   /group/{groupId}/leave?userId=                      退群（群主 409）
 * POST   /group/{groupId}/roles/{targetId}?operatorId=&role=ADMIN   设/撤管理员（仅群主）
 * POST   /group/{groupId}/mute/{targetId}?operatorId=&seconds=600   单人禁言；seconds<=0 解除
 * POST   /group/{groupId}/disband?operatorId=                解散（仅群主，逻辑删除）
 * </pre>
 *
 * <p>所有权限判断都在 {@link GroupService} 里，这个类只做参数搬运与 JSON 拼装——
 * 把规则写在 controller 会导致「接口拦了但消息准入没拦」，那是这类系统最典型的漏洞形态。
 *
 * <p>鉴权仍然留给接入层：这些接口把 {@code operatorId}/{@code ownerId} 当成已认证身份。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/group")
public class GroupController {

    private final GroupService groupService;

    public GroupController(GroupService groupService) {
        this.groupService = groupService;
    }

    @PostMapping
    public Map<String, Object> create(@RequestParam(name = "ownerId") String ownerId,
                                      @RequestParam(name = "name", required = false) String name,
                                      @RequestParam(name = "maxMembers", defaultValue = "0") int maxMembers) {
        return result(groupService.create(ownerId, name, maxMembers));
    }

    @GetMapping("/{groupId}")
    public Map<String, Object> detail(@PathVariable String groupId) {
        Optional<GroupService.GroupDetail> detail = groupService.detail(groupId);
        if (detail.isEmpty()) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("code", 404);
            body.put("message", "group not found");
            return body;
        }
        GroupService.GroupDetail view = detail.get();
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", 0);
        body.put("group", view(view.group()));
        body.put("memberCount", view.roles().size());
        Map<String, String> roles = new LinkedHashMap<>();
        view.roles().forEach((userId, role) -> roles.put(userId, role.name()));
        body.put("roles", roles);
        body.put("mutes", view.mutes());
        return body;
    }

    /**
     * 成员列表。
     *
     * <p>和 {@link #detail} 的区别只是「不返回群资料」：扇出与成员来源在服务端，
     * 客户端刷新成员列表时不需要顺带拉一遍群名/公告，少一次无意义的刷新。
     */
    @GetMapping("/{groupId}/members")
    public Map<String, Object> members(@PathVariable String groupId) {
        Optional<GroupService.GroupDetail> detail = groupService.detail(groupId);
        if (detail.isEmpty()) {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("code", 404);
            body.put("message", "group not found");
            return body;
        }
        GroupService.GroupDetail view = detail.get();
        Map<String, String> roles = new LinkedHashMap<>();
        view.roles().forEach((userId, role) -> roles.put(userId, role.name()));
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", 0);
        body.put("groupId", groupId);
        body.put("size", view.roles().size());
        body.put("members", List.copyOf(view.roles().keySet()));
        body.put("roles", roles);
        body.put("mutes", view.mutes());
        return body;
    }

    @PostMapping("/{groupId}/rename")
    public Map<String, Object> rename(@PathVariable String groupId,
                                      @RequestParam(name = "operatorId") String operatorId,
                                      @RequestParam(name = "name") String name) {
        return result(groupService.rename(operatorId, groupId, name));
    }

    @PostMapping("/{groupId}/announcement")
    public Map<String, Object> announcement(@PathVariable String groupId,
                                            @RequestParam(name = "operatorId") String operatorId,
                                            @RequestParam(name = "text", defaultValue = "") String text) {
        return result(groupService.announce(operatorId, groupId, text));
    }

    @PostMapping("/{groupId}/mute-all")
    public Map<String, Object> muteAll(@PathVariable String groupId,
                                       @RequestParam(name = "operatorId") String operatorId,
                                       @RequestParam(name = "muted", defaultValue = "true") boolean muted) {
        return result(groupService.muteAll(operatorId, groupId, muted));
    }

    @PostMapping("/{groupId}/max-members")
    public Map<String, Object> maxMembers(@PathVariable String groupId,
                                          @RequestParam(name = "operatorId") String operatorId,
                                          @RequestParam(name = "max") int max) {
        return result(groupService.setMaxMembers(operatorId, groupId, max));
    }

    @PostMapping("/{groupId}/members")
    public Map<String, Object> addMembers(@PathVariable String groupId,
                                          @RequestParam(name = "operatorId") String operatorId,
                                          @RequestBody List<String> userIds) {
        return result(groupService.addMembers(operatorId, groupId, userIds));
    }

    @DeleteMapping("/{groupId}/members/{targetId}")
    public Map<String, Object> kick(@PathVariable String groupId,
                                    @PathVariable String targetId,
                                    @RequestParam(name = "operatorId") String operatorId) {
        return result(groupService.kick(operatorId, groupId, targetId));
    }

    @PostMapping("/{groupId}/leave")
    public Map<String, Object> leave(@PathVariable String groupId,
                                     @RequestParam(name = "userId") String userId) {
        return result(groupService.leave(userId, groupId));
    }

    @PostMapping("/{groupId}/roles/{targetId}")
    public Map<String, Object> setRole(@PathVariable String groupId,
                                       @PathVariable String targetId,
                                       @RequestParam(name = "operatorId") String operatorId,
                                       @RequestParam(name = "role") String role) {
        return result(groupService.setRole(operatorId, groupId, targetId, GroupRole.parse(role)));
    }

    @PostMapping("/{groupId}/mute/{targetId}")
    public Map<String, Object> mute(@PathVariable String groupId,
                                    @PathVariable String targetId,
                                    @RequestParam(name = "operatorId") String operatorId,
                                    @RequestParam(name = "seconds", defaultValue = "600") long seconds) {
        return result(groupService.mute(operatorId, groupId, targetId, seconds));
    }

    @PostMapping("/{groupId}/disband")
    public Map<String, Object> disband(@PathVariable String groupId,
                                       @RequestParam(name = "operatorId") String operatorId) {
        return result(groupService.disband(operatorId, groupId));
    }

    private Map<String, Object> result(GroupService.GroupResult result) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("code", result.code());
        body.put("message", result.message());
        if (result.group() != null) {
            body.put("group", view(result.group()));
        }
        return body;
    }

    private Map<String, Object> view(GroupInfo info) {
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
