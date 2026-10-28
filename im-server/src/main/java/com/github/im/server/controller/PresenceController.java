package com.github.im.server.controller;

import com.github.im.proto.presence.v1.PresenceState;
import com.github.im.server.presence.PresenceService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 在线状态查询入口。
 *
 * <pre>
 * GET /presence?userIds=alice,bob    批量查询（好友列表刷新）
 * GET /presence/{userId}             单人查询
 * </pre>
 *
 * <p>为什么既要有 REST 又要有 {@code presence.get} 帧：两者的调用场景不同。
 * 好友列表页、组织架构树这类「一次要几十上百人」的查询走 HTTP（能复用连接池、
 * 能缓存、失败重试语义清晰）；而长连接客户端在会话页里问「对面在不在」时，
 * 连接本来就是热的，绕一次 HTTP 只是多一次握手与鉴权。
 *
 * <p>两者共用 {@link PresenceService#statesOf}，所以不会出现「REST 说在线、帧说离线」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/presence")
public class PresenceController {

    private final PresenceService presence;

    public PresenceController(PresenceService presence) {
        this.presence = presence;
    }

    /**
     * 批量查询。
     *
     * <p>{@code userIds} 同时接受 {@code ?userIds=a,b}（逗号分隔）与
     * {@code ?userIds=a&userIds=b}（重复参数）两种写法：前者是手写 URL 时最自然的写法，
     * 后者是多数 HTTP 客户端对 List 参数的默认序列化方式。
     */
    @GetMapping
    public Map<String, Object> batch(@RequestParam(name = "userIds") List<String> userIds) {
        List<String> flat = new ArrayList<>(userIds.size());
        for (String value : userIds) {
            if (value == null) {
                continue;
            }
            for (String item : value.split(",")) {
                if (!item.isBlank()) {
                    flat.add(item.trim());
                }
            }
        }
        List<PresenceState> states = presence.statesOf(flat);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", states.size());
        result.put("states", states.stream().map(PresenceController::toMap).toList());
        return result;
    }

    @GetMapping("/{userId}")
    public Map<String, Object> single(@PathVariable String userId) {
        List<PresenceState> states = presence.statesOf(List.of(userId));
        return states.isEmpty() ? Map.of() : toMap(states.getFirst());
    }

    /** 出参形状与 {@code presence.snapshot} 帧保持一致，客户端可以复用同一段渲染代码 */
    private static Map<String, Object> toMap(PresenceState state) {
        Map<String, Object> item = new LinkedHashMap<>();
        item.put("userId", state.getUserId());
        item.put("online", state.getOnline());
        item.put("gateways", state.getGateways());
        item.put("devices", state.getDevices());
        item.put("lastSeen", state.getLastSeen());
        return item;
    }
}
