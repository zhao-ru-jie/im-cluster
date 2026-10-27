package com.github.im.server.controller;

import com.github.im.server.message.ClientFrames;
import com.github.im.server.message.MessageService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 会话历史消息（漫游 / 拉齐）。
 *
 * <pre>GET /conv/{conversationId}/messages?userId=alice&limit=50
 * 单聊 conversationId 形如 s:alice:bob，群聊 g:{groupId}</pre>
 *
 * <p>返回的 {@code messages} 与实时下发的 {@code msg.push} <b>同构</b>（都由
 * {@link ClientFrames#frameMap} 渲染）：客户端断线重连后「先拉历史、再接实时帧」不需要两套解析逻辑。
 *
 * <h2>为什么要带 userId</h2>
 *
 * <p>因为「本地删除」是<b>用户级</b>的：同一条消息，我删了、对方没删。没有 userId 就分不清
 * 该按谁的视角过滤，只能返回全量——那等于用户的删除在重装 App 后失效。
 * 撤回则相反，是会话级的，不分视角（所有人都看不到原文）。
 *
 * <p>不带 {@code userId} 时按服务端调试视角返回（不过滤删除），仅用于排障。
 *
 * <p>读的是 {@link MessageService#history}，因此 {@code im.storage} 切到 jdbc 后
 * 自动走 {@code im_message} 的 {@code uk_msg_conv_seq} 索引，Redis 形态下走 List 尾部范围读。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/conv")
public class ConversationHistoryController {

    private static final int DEFAULT_LIMIT = 50;
    /** 上限与 MessageService 的离线补拉量级对齐，避免一次拉爆客户端 */
    private static final int MAX_LIMIT = 200;

    private final MessageService messageService;

    public ConversationHistoryController(MessageService messageService) {
        this.messageService = messageService;
    }

    @GetMapping("/{conversationId}/messages")
    public Map<String, Object> recent(@PathVariable String conversationId,
                                      @RequestParam(name = "userId", required = false) String userId,
                                      @RequestParam(name = "limit", defaultValue = "0") int limit) {
        int safeLimit = limit <= 0 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
        List<Map<String, Object>> messages = messageService.history(userId, conversationId, safeLimit);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        result.put("limit", safeLimit);
        result.put("count", messages.size());
        result.put("messages", messages);
        return result;
    }
}
