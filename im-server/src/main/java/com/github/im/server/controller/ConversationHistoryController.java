package com.github.im.server.controller;

import com.github.im.server.message.ClientFrames;
import com.github.im.server.message.MessageService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 会话历史消息（漫游 / 拉齐 / 上滑翻页）。
 *
 * <pre>
 * 第一页：GET /conv/{conversationId}/messages?userId=alice&limit=50
 * 上滑：  GET /conv/{conversationId}/messages?userId=alice&limit=50&beforeSeq={上一页的 nextBeforeSeq}
 * 单聊 conversationId 形如 s:alice:bob，群聊 g:{groupId}
 * </pre>
 *
 * <p>返回的 {@code messages} 与实时下发的 {@code msg.push} <b>同构</b>（都由
 * {@link ClientFrames#frameMap} 渲染）：客户端断线重连后「先拉历史、再接实时帧」不需要两套解析逻辑。
 *
 * <h2>翻页契约</h2>
 *
 * <p>游标是<b>排他的 seq</b>（{@code seq < beforeSeq}），不是 offset：seq 单调且只追加，
 * 翻页期间来了新消息不会让切片错位；offset 则会整体偏移，表现为重复或漏消息。
 * 响应里的 {@code nextBeforeSeq} 原样带回来即可继续上滑，{@code hasMore=false} 时停止。
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

    private final MessageService messageService;

    public ConversationHistoryController(MessageService messageService) {
        this.messageService = messageService;
    }

    @GetMapping("/{conversationId}/messages")
    public Map<String, Object> messages(@PathVariable String conversationId,
                                        @RequestParam(name = "userId", required = false) String userId,
                                        @RequestParam(name = "beforeSeq", defaultValue = "0") long beforeSeq,
                                        @RequestParam(name = "limit", defaultValue = "0") int limit) {
        // 条数上限由 service 夹取（单一出处），这里只负责回显实际生效值
        MessageService.HistoryPage page = messageService.history(userId, conversationId, beforeSeq, limit);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        result.put("limit", page.limit());
        result.put("count", page.messages().size());
        // 翻页游标：客户端上滑时把 nextBeforeSeq 原样带回来；hasMore=false 时别再请求
        result.put("nextBeforeSeq", page.nextBeforeSeq());
        result.put("hasMore", page.hasMore());
        result.put("messages", page.messages());
        return result;
    }
}
