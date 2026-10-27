package com.github.im.gateway.transport;

import com.github.im.proto.ingress.v1.DeliveredAck;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 两条「上报回执」帧的字段契约。
 *
 * <p>为什么值得单独测：这两个上报是<b>静默失败</b>的（异步发出去、出错只打 debug 日志、不回错误帧），
 * 所以字段名一旦对不上，表现不是报错，而是「已读一直不生效」「送达状态一直不更新」——
 * 客户端和网关都不会有异常，只能靠抓包或读代码才发现。
 *
 * <p>字段名固定为 {@code conversationId} / {@code seq}（曾用简写 {@code c} / {@code s}，
 * 现已全部改为完整单词）。下面同时锁住「旧简写不再被识别」，避免两套名字并存。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class SessionLifecycleFrameParsingTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    @DisplayName("msg.read：conversationId + seq 解析成已读位置")
    void readReceiptIsParsed() {
        SessionLifecycle.ReadReceipt receipt =
                SessionLifecycle.readReceiptOf(parse("{\"ver\":1,\"type\":\"msg.read\","
                        + "\"conversationId\":\"s:alice:bob\",\"seq\":12}"));

        assertNotNull(receipt, "完整字段必须能解析出来");
        assertEquals("s:alice:bob", receipt.conversationId());
        assertEquals(12L, receipt.seq());
    }

    @Test
    @DisplayName("msg.read：旧的简写 c/s 不再识别")
    void readReceiptRejectsShortKeys() {
        assertNull(SessionLifecycle.readReceiptOf(parse(
                        "{\"ver\":1,\"type\":\"msg.read\",\"c\":\"s:alice:bob\",\"s\":12}")),
                "两套字段名并存会让「改对了还是没生效」这种问题查很久，旧简写必须直接不认");
    }

    @Test
    @DisplayName("msg.read：缺字段或非法 seq 时返回 null，不抛异常")
    void readReceiptToleratesBadInput() {
        assertNull(SessionLifecycle.readReceiptOf(parse(
                "{\"type\":\"msg.read\",\"conversationId\":\"s:alice:bob\"}")), "缺 seq");
        assertNull(SessionLifecycle.readReceiptOf(parse(
                "{\"type\":\"msg.read\",\"conversationId\":\"s:alice:bob\",\"seq\":0}")), "seq 必须为正");
        assertNull(SessionLifecycle.readReceiptOf(parse(
                "{\"type\":\"msg.read\",\"conversationId\":\"  \",\"seq\":5}")), "空会话 ID");
    }

    @Test
    @DisplayName("msg.recv：批量 acks 按 conversationId + seq 解析")
    void deliveredAcksAreParsed() {
        List<DeliveredAck> acks = SessionLifecycle.deliveredAcksOf(parse(
                "{\"ver\":1,\"type\":\"msg.recv\",\"acks\":["
                        + "{\"conversationId\":\"s:alice:bob\",\"seq\":5},"
                        + "{\"conversationId\":\"g:group1\",\"seq\":12}]}"));

        assertEquals(2, acks.size(), "两条 ack 都要解析出来");
        assertEquals("s:alice:bob", acks.get(0).getConversationId());
        assertEquals(5L, acks.get(0).getSeq());
        assertEquals("g:group1", acks.get(1).getConversationId());
        assertEquals(12L, acks.get(1).getSeq());
    }

    @Test
    @DisplayName("msg.recv：旧的简写 c/s 解析结果为空，不会被当成无效数据抛错")
    void deliveredAcksRejectShortKeys() {
        assertTrue(SessionLifecycle.deliveredAcksOf(parse(
                        "{\"type\":\"msg.recv\",\"acks\":[{\"c\":\"s:alice:bob\",\"s\":5}]}")).isEmpty(),
                "旧简写解析不出东西是预期行为，但必须安静地返回空而不是抛异常");
    }

    @Test
    @DisplayName("msg.recv：脏项只跳过自己，不影响同一批里的合法项")
    void deliveredAcksSkipBadItemsOnly() {
        List<DeliveredAck> acks = SessionLifecycle.deliveredAcksOf(parse(
                "{\"type\":\"msg.recv\",\"acks\":["
                        + "{\"conversationId\":\"s:alice:bob\",\"seq\":0},"      // seq 非法
                        + "{\"seq\":7},"                                         // 缺会话
                        + "{\"conversationId\":\"s:alice:bob\",\"seq\":9}]}"));  // 合法

        assertEquals(1, acks.size(), "一条脏 ack 不该让整批回执作废");
        assertEquals(9L, acks.get(0).getSeq());
        assertTrue(SessionLifecycle.deliveredAcksOf(parse("{\"type\":\"msg.recv\"}")).isEmpty(),
                "没有 acks 字段时返回空");
        assertTrue(SessionLifecycle.deliveredAcksOf(parse(
                "{\"type\":\"msg.recv\",\"acks\":{}}")).isEmpty(), "acks 不是数组时返回空");
    }

    private JsonNode parse(String json) {
        return mapper.readTree(json);
    }
}
