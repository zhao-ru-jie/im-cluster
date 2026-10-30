package com.github.im.server.controller;

import com.github.im.server.message.MessageReadStatService;
import com.github.im.server.message.MessageReadStatService.ReadStat;
import com.github.im.server.message.MessageReadStatService.ReadStatResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 群聊已读聚合（谁读了 / 还有谁没读）。
 *
 * <pre>
 * 最新一条：GET /conv/g:{groupId}/read-stat?userId=alice
 * 指定消息：GET /conv/g:{groupId}/read-stat?userId=alice&seq=100&limit=50
 * 单聊也能查：GET /conv/s:alice:bob/read-stat?userId=alice&seq=100
 * </pre>
 *
 * <p>单聊的「对端读到哪」有实时回执（{@code msg.state}），这个接口主要服务群聊：
 * 群里没有逐条回执帧（扇出是「成员数 × 消息数」，见 {@link
 * com.github.im.server.message.MessageStateNotifier} 的类注释），所以已读人数是
 * <b>按需聚合</b>出来的。
 *
 * <h2>两个口径</h2>
 *
 * <ul>
 *   <li>{@code total} <b>不包含该消息的发送者</b>：「已读 3/3」比「已读 4/4」有用；</li>
 *   <li>{@code readCount + unreadCount == total}，且 {@code members} 只是<b>明细</b>——
 *       被 {@code limit} 截断时 {@code truncated=true}，但计数仍是全量。</li>
 * </ul>
 *
 * <p>返回值里的 {@code members} 已读在前、各自按 readSeq 倒序，直接渲染即可，不需要客户端再排。
 *
 * <p>结果带 {@code im.read-stat.cache-ttl}（默认 2 秒）的快照缓存：滑动一屏连续问十几条消息
 * 只会打一次存储，代价是最多 2 秒的滞后（调用者自己的已读位置例外，永远实时）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/conv")
public class ConversationReadStatController {

    private final MessageReadStatService readStatService;

    public ConversationReadStatController(MessageReadStatService readStatService) {
        this.readStatService = readStatService;
    }

    @GetMapping("/{conversationId}/read-stat")
    public Map<String, Object> readStat(@PathVariable String conversationId,
                                        @RequestParam(name = "userId") String userId,
                                        @RequestParam(name = "seq", defaultValue = "0") long seq,
                                        @RequestParam(name = "limit", defaultValue = "0") int limit) {
        // 条数上限由 service 夹取（单一出处），这里只负责回显
        ReadStatResult result = readStatService.stat(conversationId, userId, seq, limit);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("conversationId", conversationId);
        body.put("code", result.code());
        body.put("message", result.message());
        ReadStat stat = result.stat();
        if (stat != null) {
            body.put("convType", stat.convType());
            body.put("seq", stat.seq());
            body.put("viewerReadSeq", stat.viewerReadSeq());
            body.put("total", stat.total());
            body.put("readCount", stat.readCount());
            body.put("unreadCount", stat.unreadCount());
            body.put("truncated", stat.truncated());
            body.put("members", stat.members());
        }
        return body;
    }
}
