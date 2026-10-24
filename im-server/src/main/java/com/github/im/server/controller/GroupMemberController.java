package com.github.im.server.controller;

import com.github.im.server.group.GroupMemberRepository;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 群成员维护入口（IM 服务）：没有它，群聊扇出就没有成员来源。
 *
 * <pre>
 * POST   /group/{groupId}/members        ["alice","bob"]   加成员（幂等）
 * DELETE /group/{groupId}/members/{uid}                    移出成员
 * GET    /group/{groupId}/members                          列成员
 * </pre>
 *
 * <p>定位与 {@link TestSendController} 相同：demo 阶段的可验证入口，鉴权/群主校验等留给接入层。
 * 写入的介质跟随 {@code im.storage}——Redis Set 或 {@code im_group_member} 表。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/group")
public class GroupMemberController {

    private final GroupMemberRepository groupMemberRepository;

    public GroupMemberController(GroupMemberRepository groupMemberRepository) {
        this.groupMemberRepository = groupMemberRepository;
    }

    @PostMapping("/{groupId}/members")
    public Map<String, Object> addMembers(@PathVariable String groupId,
                                          @RequestBody List<String> userIds) {
        groupMemberRepository.addMembers(groupId, userIds);
        return snapshot(groupId);
    }

    @DeleteMapping("/{groupId}/members/{userId}")
    public Map<String, Object> removeMember(@PathVariable String groupId,
                                            @PathVariable String userId) {
        groupMemberRepository.removeMember(groupId, userId);
        return snapshot(groupId);
    }

    @GetMapping("/{groupId}/members")
    public Map<String, Object> members(@PathVariable String groupId) {
        return snapshot(groupId);
    }

    private Map<String, Object> snapshot(String groupId) {
        List<String> members = groupMemberRepository.membersOf(groupId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("groupId", groupId);
        result.put("size", members.size());
        result.put("members", members);
        return result;
    }
}
