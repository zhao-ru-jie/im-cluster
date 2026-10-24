package com.github.im.server.controller;

import com.github.im.server.message.ClientFrames;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.message.StoredMessage;
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
 * 会话历史消息（漫游 / 拉齐）。
 *
 * <pre>GET /conv/{conversationId}/messages?limit=50   单聊 conversationId 形如 s:alice:bob，群聊 g:{groupId}</pre>
 *
 * <p>返回的 {@code messages} 与实时下发的 {@code msg.push} <b>同构</b>（都由
 * {@link ClientFrames#frameMap} 渲染）：客户端断线重连后「先拉历史、再接实时帧」不需要两套解析逻辑。
 *
 * <p>读的就是 {@code MessageRepository.recent}，因此 {@code im.storage} 切到 jdbc 后
 * 这里自动走 {@code im_message} 的 {@code uk_msg_conv_seq} 索引，Redis 形态下走 List 尾部范围读。
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

    private final MessageRepository messageRepository;
    private final ClientFrames clientFrames;

    public ConversationHistoryController(MessageRepository messageRepository, ClientFrames clientFrames) {
        this.messageRepository = messageRepository;
        this.clientFrames = clientFrames;
    }

    @GetMapping("/{conversationId}/messages")
    public Map<String, Object> recent(@PathVariable String conversationId,
                                      @RequestParam(name = "limit", defaultValue = "0") int limit) {
        int safeLimit = limit <= 0 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
        List<StoredMessage> messages = messageRepository.recent(conversationId, safeLimit);
        List<Map<String, Object>> frames = new ArrayList<>(messages.size());
        for (StoredMessage message : messages) {
            frames.add(clientFrames.frameMap(message));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        result.put("limit", safeLimit);
        result.put("count", frames.size());
        result.put("messages", frames);
        return result;
    }
}
