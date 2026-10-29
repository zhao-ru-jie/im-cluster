package com.github.im.server.controller;

import com.github.im.server.relation.FriendRequest;
import com.github.im.server.relation.RelationRepository;
import com.github.im.server.relation.RelationService;
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
 * 关系链入口（好友 / 好友申请 / 黑名单）。
 *
 * <pre>
 * GET    /relation/{userId}/friends                        好友列表
 * DELETE /relation/{userId}/friends/{friendId}             解除好友（双向）
 *
 * GET    /relation/{userId}/requests                       收到的 + 发出的申请（一次往返）
 * POST   /relation/{userId}/requests                       {"to":"bob","message":"我是小李"}  发送申请
 * POST   /relation/{userId}/requests/{fromId}/accept       同意（{userId} 是被申请人）
 * POST   /relation/{userId}/requests/{fromId}/reject       拒绝
 * DELETE /relation/{userId}/requests/{toId}                撤回自己发出的申请
 *
 * GET    /relation/{userId}/blocks                         黑名单
 * POST   /relation/{userId}/blocks                         ["spammer"]  拉黑（单向，不通知对方）
 * DELETE /relation/{userId}/blocks/{blockedId}             取消拉黑
 * </pre>
 *
 * <h2>为什么没有「直接加好友」的接口</h2>
 *
 * <p>曾经有 {@code POST /relation/{userId}/friends}（一条请求就把双向好友边写死）。
 * 它与「必须经过对方同意」的规则直接冲突：任何客户端只要拿着自己的 userId 就能把
 * 别人加进自己的好友列表，对方甚至不会收到任何提示。因此该路由被移除，
 * 建立关系的唯一入口是<b>申请 + 对方同意</b>；{@code addFriends} 只出现在
 * {@link RelationService#accept} 里，即「申请已经被同意」之后。
 *
 * <h2>返回体风格</h2>
 *
 * <p>写操作沿用 {@link GroupController} 的 {@code {code, message, ...}}：{@code code != 0}
 * 时前端直接展示 {@code message}；只有 {@code 0} 才算成功。读操作（好友 / 黑名单）保持
 * 原来的 {@code {userId, size, friends}} 形态，避免历史调用方跟着改。
 *
 * <p>鉴权仍留给接入层：这些接口把 {@code {userId}} 当成已认证身份，
 * 对外暴露时必须由网关/业务层校验调用者就是这个 {@code userId} —— 尤其是 accept/reject，
 * 校验不严等于允许任何人替别人同意好友申请。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/relation")
public class RelationController {

    /** 申请请求体：{@code to} 是目标用户，{@code message} 是验证消息（可省略） */
    public record FriendRequestForm(String to, String message) {
    }

    private final RelationRepository relations;
    private final RelationService service;

    public RelationController(RelationRepository relations, RelationService service) {
        this.relations = relations;
        this.service = service;
    }

    /* ------------------------------- 好友 ------------------------------- */

    @GetMapping("/{userId}/friends")
    public Map<String, Object> friends(@PathVariable String userId) {
        return snapshot(userId, "friends", relations.friendsOf(userId));
    }

    @DeleteMapping("/{userId}/friends/{friendId}")
    public Map<String, Object> removeFriend(@PathVariable String userId,
                                            @PathVariable String friendId) {
        relations.removeFriend(userId, friendId);
        return snapshot(userId, "friends", relations.friendsOf(userId));
    }

    /* ------------------------------ 好友申请 ------------------------------ */

    /**
     * 收到的与发出的申请一起返回。
     *
     * <p>合并成一个接口而不是两个：通讯录的「新的朋友」页要同时展示
     * 「别人加我」和「我加别人（等待验证）」，拆成两个接口会让这个页面
     * 在弱网下出现「我发出的都在、别人加我的还没到」的半成品状态。
     */
    @GetMapping("/{userId}/requests")
    public Map<String, Object> requests(@PathVariable String userId) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", RelationService.CODE_OK);
        result.put("userId", userId);
        result.put("incoming", service.incoming(userId));
        result.put("outgoing", service.outgoing(userId));
        return result;
    }

    @PostMapping("/{userId}/requests")
    public Map<String, Object> sendRequest(@PathVariable String userId,
                                           @RequestBody(required = false) FriendRequestForm form) {
        if (form == null) {
            return result(RelationService.CODE_BAD_REQUEST, "缺少请求体（需要 {\"to\":\"...\"}）", null, List.of());
        }
        return toMap(service.request(userId, form.to(), form.message()));
    }

    @PostMapping("/{userId}/requests/{fromId}/accept")
    public Map<String, Object> accept(@PathVariable String userId, @PathVariable String fromId) {
        return toMap(service.accept(userId, fromId));
    }

    @PostMapping("/{userId}/requests/{fromId}/reject")
    public Map<String, Object> reject(@PathVariable String userId, @PathVariable String fromId) {
        return toMap(service.reject(userId, fromId));
    }

    /** 撤回：{@code {toId}} 是我当初申请的那个人 */
    @DeleteMapping("/{userId}/requests/{toId}")
    public Map<String, Object> cancel(@PathVariable String userId, @PathVariable String toId) {
        return toMap(service.cancel(userId, toId));
    }

    /* ------------------------------- 黑名单 ------------------------------- */

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

    /* ------------------------------- 内部 ------------------------------- */

    private static Map<String, Object> toMap(RelationService.Result outcome) {
        return result(outcome.code(), outcome.message(), outcome.request(), outcome.friends());
    }

    private static Map<String, Object> result(int code, String message, FriendRequest request,
                                              List<String> friends) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", code);
        result.put("message", message == null ? "" : message);
        if (request != null) {
            result.put("request", request);
        }
        if (!friends.isEmpty()) {
            result.put("friends", friends);
        }
        return result;
    }

    private Map<String, Object> snapshot(String userId, String field, List<String> values) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("size", values.size());
        result.put(field, values);
        return result;
    }
}
