package com.github.im.server.controller;

import com.github.im.proto.ingress.v1.ConversationSummary;
import com.github.im.proto.ingress.v1.ListConversationsRequest;
import com.github.im.proto.ingress.v1.ListConversationsResponse;
import com.github.im.proto.ingress.v1.MarkReadRequest;
import com.github.im.proto.ingress.v1.MarkReadResponse;
import com.github.im.server.conversation.ConversationStore.ConversationFlag;
import com.github.im.server.message.MessageService;
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
 * 会话列表、已读上报、消息生命周期（撤回 / 本地删除）。
 *
 * <pre>
 * GET  /conv?userId=alice&limit=50                             会话列表（置顶优先，带未读数）
 * POST /conv/{conversationId}/read?userId=alice&seq=100         上报已读（用户级，只前进）
 * POST /conv/{conversationId}/messages/{seq}/recall?operatorId=alice   撤回（会话级，仅发送方 + 时限内）
 * POST /conv/{conversationId}/messages/{seq}/delete?userId=alice       本地删除（用户级，对方不受影响）
 * </pre>
 *
 * <p>未读数是 {@code lastSeq - readSeq} 算出来的，不是存的计数：群聊里存计数是
 * 「消息数 × 成员数」的写放大，而且一次写失败就永久和真实值不一致，两个游标相减天然自愈。
 *
 * <p>已读是<b>用户级</b>的，所以这里没有 {@code deviceId} 参数——手机读完，网页的角标
 * 下次拉列表就是 0。按设备区分的是同步水位（见 {@code im:wm:{userId}:{deviceId}}），两回事。
 *
 * <p>与 {@link ConversationHistoryController} 共用 {@code /conv} 前缀：
 * 那个负责「某个会话里的消息」，这里负责「我有哪些会话」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/conv")
public class ConversationController {

    private static final int DEFAULT_LIMIT = 50;
    /** 上限刻意保守：会话列表要连带每条会话的元信息，页太大会放大成几十次 HGETALL */
    private static final int MAX_LIMIT = 200;

    private final MessageService messageService;

    public ConversationController(MessageService messageService) {
        this.messageService = messageService;
    }

    @GetMapping
    public Map<String, Object> list(@RequestParam(name = "userId") String userId,
                                    @RequestParam(name = "limit", defaultValue = "0") int limit) {
        int safeLimit = limit <= 0 ? DEFAULT_LIMIT : Math.min(limit, MAX_LIMIT);
        ListConversationsResponse response = messageService.listConversations(
                ListConversationsRequest.newBuilder().setUserId(userId).setLimit(safeLimit).build());
        List<Map<String, Object>> conversations = new ArrayList<>(response.getConversationsCount());
        for (ConversationSummary summary : response.getConversationsList()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("conversationId", summary.getConversationId());
            item.put("type", summary.getType());
            item.put("lastSeq", summary.getLastSeq());
            item.put("lastTime", summary.getLastTime());
            item.put("lastFrom", summary.getLastFrom());
            item.put("lastDigest", summary.getLastDigest());
            item.put("lastMsgId", summary.getLastMsgId());
            item.put("readSeq", summary.getReadSeq());
            item.put("unread", summary.getUnread());
            item.put("pinned", summary.getPinned());
            item.put("muted", summary.getMuted());
            conversations.add(item);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("limit", safeLimit);
        result.put("count", conversations.size());
        result.put("conversations", conversations);
        return result;
    }

    @PostMapping("/{conversationId}/read")
    public Map<String, Object> markRead(@PathVariable String conversationId,
                                        @RequestParam(name = "userId") String userId,
                                        @RequestParam(name = "seq", defaultValue = "0") long seq) {
        MarkReadResponse response = messageService.markRead(MarkReadRequest.newBuilder()
                .setUserId(userId)
                .setConversationId(conversationId)
                .setSeq(seq)
                .build());
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        result.put("code", response.getCode());
        result.put("message", response.getMessage());
        result.put("readSeq", response.getReadSeq());
        result.put("unread", response.getUnread());
        return result;
    }

    /**
     * 撤回一条消息：会话级，会话里所有人都看不到原文（含发送方自己的其他端）。
     *
     * <p>只有发送方、且在 {@code im.recall.window} 内（默认 2 分钟）——规则在服务层，
     * 这里不做二次判断，避免两处规则漂移。
     */
    @PostMapping("/{conversationId}/messages/{seq}/recall")
    public Map<String, Object> recall(@PathVariable String conversationId,
                                      @PathVariable long seq,
                                      @RequestParam(name = "operatorId") String operatorId) {
        return change(conversationId, seq, messageService.recall(conversationId, seq, operatorId));
    }

    /**
     * 本地删除一条消息：只影响这一个用户的视图，对方照旧看得到。
     *
     * <p>删除后仍可通过 {@code GET /conv/{id}/messages?userId=} 验证——返回里不会再有这一条；
     * 撤回则仍在列表里，只是 {@code body} 为空且带 {@code recalled:true}。
     */
    @PostMapping("/{conversationId}/messages/{seq}/delete")
    public Map<String, Object> delete(@PathVariable String conversationId,
                                      @PathVariable long seq,
                                      @RequestParam(name = "userId") String userId) {
        return change(conversationId, seq, messageService.deleteForUser(conversationId, seq, userId));
    }

    private Map<String, Object> change(String conversationId, long seq,
                                       MessageService.MessageChangeResult result) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("conversationId", conversationId);
        body.put("seq", seq);
        body.put("code", result.code());
        body.put("message", result.message());
        return body;
    }

    /**
     * 设置会话标记：置顶 / 免打扰。
     *
     * <pre>
     * POST /conv/{conversationId}/flag?userId=alice&pinned=true
     * POST /conv/{conversationId}/flag?userId=alice&muted=false
     * </pre>
     *
     * <p>参数缺省 = 这一项不改（不是 false）：两个开关是独立的，
     * 「设为置顶」不应该顺手把免打扰清掉。
     */
    @PostMapping("/{conversationId}/flag")
    public Map<String, Object> flag(@PathVariable String conversationId,
                                    @RequestParam(name = "userId") String userId,
                                    @RequestParam(name = "pinned", required = false) Boolean pinned,
                                    @RequestParam(name = "muted", required = false) Boolean muted) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("conversationId", conversationId);
        try {
            ConversationFlag flag = messageService.setConversationFlags(userId, conversationId, pinned, muted);
            result.put("code", 0);
            result.put("pinned", flag.pinned());
            result.put("muted", flag.muted());
        } catch (IllegalArgumentException e) {
            result.put("code", 400);
            result.put("message", e.getMessage());
        }
        return result;
    }
}
