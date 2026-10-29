package com.github.im.server.utils;

import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * JSON ↔ Java 对象的统一入口（SDK 内唯一一份 {@link ObjectMapper} 配置）。
 *
 * <h2>为什么需要这一层</h2>
 *
 * <p>直接用 {@code ObjectMapper} 时，每个调用方都要自己回答三个问题：读文本还是读树、
 * 泛型怎么带（{@link TypeReference}）、服务端多给了字段要不要炸。这三件事在任何一处答错，
 * 症状都是「静默少字段」或者「服务端加个字段客户端就解析崩」，而且都要到线上才发现。
 * 所以配置只留一份，转换只走这里的方法。
 *
 * <pre>{@code
 * // 从响应树里直接拿对象
 * ConversationSummary one = JsonUtils.treeToObject(node, ConversationSummary.class);
 * List<ConversationSummary> all = JsonUtils.treeToList(root.path("conversations"), ConversationSummary.class);
 * // 从文本拿对象
 * DownlinkFrame.Push push = JsonUtils.toObject(json, DownlinkFrame.Push.class);
 * // 反向
 * String body = JsonUtils.toJson(request);
 * }</pre>
 *
 * <h2>容错策略是刻意与 Jackson 默认相反的</h2>
 *
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class JsonUtils {

    /**
     * 共享 mapper。Jackson 3 的 {@code ObjectMapper} 构建后不可变（配置改动要先 {@code rebuild()}），
     * 所以这个单例可以安全地被多条连接、多个虚拟线程同时使用。
     */
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_MISSING_CREATOR_PROPERTIES)
            .disable(DeserializationFeature.FAIL_ON_NULL_FOR_PRIMITIVES)
            .build();

    private JsonUtils() {
    }

    /**
     * 共享 mapper，给需要底层能力的场景用（自定义 {@code JavaType}、{@code MappingIterator} 等）。
     *
     * <p>不要基于它 {@code rebuild()} 出一份「只在本处生效」的配置——那等于把本类要消灭的
     * 配置分叉又造回来；确有全局需求请改这里的静态配置。
     */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    // ==== 文本 → 对象 ====

    /**
     * JSON 文本 → 对象（record / 普通 bean 都行）。
     *
     * @throws IllegalArgumentException json 为空
     * @throws JacksonException         不是合法 JSON，或字段类型对不上
     */
    public static <T> T toObject(String json, Class<T> type) {
        requireText(json);
        return MAPPER.readValue(json, type);
    }

    /**
     * JSON 文本 → 泛型对象，例如 {@code new TypeReference<List<ConversationSummary>>(){}}。
     *
     * <p>普通集合更常用 {@link #toList(String, Class)}，不必自己写匿名子类。
     */
    public static <T> T toObject(String json, TypeReference<T> type) {
        requireText(json);
        return MAPPER.readValue(json, type);
    }

    /**
     * JSON 文本 → {@code List<elementType>}。
     *
     * @throws JacksonException 顶层不是数组时
     */
    public static <T> List<T> toList(String json, Class<T> elementType) {
        requireText(json);
        return treeToList(MAPPER.readTree(json), elementType);
    }

    // ==== 树 → 对象 ====

    /**
     * 响应树 → 对象，省掉逐字段 {@code node.path("x").asLong(0)} 那一串。
     *
     * <p>{@code node} 为 {@code null}、{@code MissingNode}、{@code NullNode} 时返回
     * {@code null}：服务端「这一项没有」和「这一项是 null」在语义上是一回事，
     * 调用方拿 null 判一次就够了，不必再区分两种「空」。
     */
    public static <T> T treeToObject(JsonNode node, Class<T> type) {
        if (isEmpty(node)) {
            return null;
        }
        return MAPPER.treeToValue(node, type);
    }

    /** 同 {@link #treeToObject(JsonNode, Class)}，用于带泛型的类型 */
    public static <T> T treeToObject(JsonNode node, TypeReference<T> type) {
        if (isEmpty(node)) {
            return null;
        }
        return MAPPER.treeToValue(node, type);
    }

    /**
     * 数组节点 → {@code List<elementType>}。
     *
     * <p>{@code node} 为 {@code null}/缺失/null 时返回<b>空列表</b>而不是 null 或抛错：
     * 「没有这个数组」在调用方眼里就是「一个都没有」，返回 null 只会逼出满地的判空。
     * 返回的是可写的 {@link ArrayList}，调用方自行增删不会影响本类。
     *
     * @throws JacksonException 节点存在但不是数组（那多半是两端字段名撞车了，不能当空糊过去）
     */
    public static <T> List<T> treeToList(JsonNode node, Class<T> elementType) {
        List<T> values = new ArrayList<>();
        if (isEmpty(node)) {
            return values;
        }
        if (!node.isArray()) {
            throw new IllegalArgumentException("期望 JSON 数组，实际是 " + node.getNodeType());
        }
        for (JsonNode element : node) {
            values.add(treeToObject(element, elementType));
        }
        return values;
    }

    // ==== 对象 → JSON ====

    /** 对象 → JSON 文本（不含缩进，发给服务端的就是最终形态） */
    public static String toJson(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    /**
     * JSON 文本 → 树，用于「只想看几个字段」或者把整棵树原样转交给别人（如
     * {@link Frames#parse(JsonNode)}）的场景。
     *
     * @throws IllegalArgumentException json 为空
     * @throws JacksonException         不是合法 JSON
     */
    public static JsonNode toJsonNode(String json) {
        requireText(json);
        return MAPPER.readTree(json);
    }

    // ==== 内部 ====

    private static boolean isEmpty(JsonNode node) {
        return node == null || node.isNull() || node.isMissingNode();
    }

    private static void requireText(String json) {
        if (json == null || json.isBlank()) {
            throw new IllegalArgumentException("json 不能为空");
        }
    }
}
