package com.github.im.server.controller;

import com.github.im.server.relation.RelationRepository;
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
 * 关系链维护入口（好友 / 黑名单）。
 *
 * <pre>
 * GET    /relation/{userId}/friends             好友列表
 * POST   /relation/{userId}/friends             ["bob","carol"]   建立双向好友（幂等）
 * DELETE /relation/{userId}/friends/{friendId}                   解除好友（双向）
 * GET    /relation/{userId}/blocks              黑名单
 * POST   /relation/{userId}/blocks              ["spammer"]       拉黑（单向，不通知对方）
 * DELETE /relation/{userId}/blocks/{blockedId}                   取消拉黑
 * </pre>
 *
 * <p>定位与 {@link GroupController} 中的成员维护相同：<b>由接入方的好友/组织架构系统同步过来</b>。
 * 「申请 → 通知 → 同意」的两阶段流程需要通知子系统（红点、未读、跨端已处理状态），
 * 那是业务侧的事；IM 侧只消费「关系成立之后」这个事实。
 *
 * <p>鉴权同样留给接入层：这些接口会把 {@code {userId}} 当成已认证身份，
 * 对外暴露时必须由网关/业务层校验调用者就是这个 {@code userId}。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/relation")
public class RelationController {

    private final RelationRepository relations;

    public RelationController(RelationRepository relations) {
        this.relations = relations;
    }

    @GetMapping("/{userId}/friends")
    public Map<String, Object> friends(@PathVariable String userId) {
        return snapshot(userId, "friends", relations.friendsOf(userId));
    }

    @PostMapping("/{userId}/friends")
    public Map<String, Object> addFriends(@PathVariable String userId,
                                          @RequestBody List<String> friendIds) {
        relations.addFriends(userId, friendIds);
        return snapshot(userId, "friends", relations.friendsOf(userId));
    }

    @DeleteMapping("/{userId}/friends/{friendId}")
    public Map<String, Object> removeFriend(@PathVariable String userId,
                                            @PathVariable String friendId) {
        relations.removeFriend(userId, friendId);
        return snapshot(userId, "friends", relations.friendsOf(userId));
    }

    @GetMapping("/{userId}/blocks")
    public Map<String, Object> blocks(@PathVariable String userId) {
        return snapshot(userId, "blocked", relations.blockedOf(userId));
    }

    @PostMapping("/{userId}/blocks")
    public Map<String, Object> block(@PathVariable String userId,
                                     @RequestBody List<String> blockedIds) {
        for (String blockedId : blockedIds) {
            relations.block(userId, blockedId);
        }
        return snapshot(userId, "blocked", relations.blockedOf(userId));
    }

    @DeleteMapping("/{userId}/blocks/{blockedId}")
    public Map<String, Object> unblock(@PathVariable String userId,
                                       @PathVariable String blockedId) {
        relations.unblock(userId, blockedId);
        return snapshot(userId, "blocked", relations.blockedOf(userId));
    }

    private Map<String, Object> snapshot(String userId, String field, List<String> values) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("size", values.size());
        result.put(field, values);
        return result;
    }
}
