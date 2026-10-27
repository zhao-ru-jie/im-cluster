package com.github.im.client;

import tools.jackson.databind.JsonNode;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 短请求客户端：会话列表、历史消息、已读、撤回/删除、置顶/免打扰、群治理、好友/黑名单。
 *
 * <h2>为什么长连接之外还需要这一层</h2>
 *
 * <p>长连接解决的是「实时」；而「打开会话先拉最近 50 条」「进会话列表」「改群名」这类操作
 * 天生是「一次请求一次答复」，走帧通道反而要为每个请求造一个 requestId、等一个回帧、
 * 处理超时与乱序。服务端已经把这批接口做成 REST，SDK 顺着用即可。
 *
 * <h2>打到哪个端口</h2>
 *
 * <p><b>不是网关</b>：网关只做长连接与 gRPC，不转发任何业务 HTTP。
 * 单体形态是 {@code im-app} 的 {@code server.port}（默认 8080），
 * 拆分形态是 {@code im-server-boot} 的（默认 8088）。打到网关端口只会 404。
 *
 * <h2>三类响应形状必须分开处理</h2>
 *
 * <ol>
 *   <li><b>带 code/message</b>（{@code /conv/{id}/read|recall|delete|flag}、{@code /group/**}）：
 *       失败也是 HTTP 200，要看 {@code code}；</li>
 *   <li><b>没有 code</b>（{@code GET /conv}、{@code GET /conv/{id}/messages}、{@code /relation/**}）：
 *       永远返回数据，用「有没有 code」判成功会误判；</li>
 *   <li><b>框架级错误</b>（少必填参数、JSON 解析失败）→ 非 200 + {@code BasicErrorController} 结构，
 *       这里统一抛 {@link ImHttpException}。</li>
 * </ol>
 *
 * <h2>翻页</h2>
 *
 * <p>当前服务端只支持「最近 N 条」（历史）与「首页 N 条」（会话列表），没有 cursor/beforeSeq。
 * SDK 不假装有翻页能力：{@link #history} 的注释直接写明这一点。
 *
 * <h2>幂等与回执</h2>
 *
 * <p>撤回/删除/已读除了 HTTP 响应，还会通过长连接推 {@code msg.update} / {@code msg.state}。
 * SDK 不在这里做去重——那是 {@link ImClient} 的活（按 {@code conversationId|seq|change} 归并）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class ImHttpClient implements AutoCloseable {

    private final String baseUrl;
    private final String userId;
    private final HttpClient http;
    private final Frames frames = new Frames();
    private final int requestTimeoutMs;

    /**
     * @param baseUrl 形如 {@code http://127.0.0.1:8088}（不加尾斜杠）
     * @param userId  默认身份：多数接口用它与 {@code operatorId}，可在单次调用里覆盖
     */
    public ImHttpClient(String baseUrl, String userId) {
        this(baseUrl, userId, 5_000, 10_000);
    }

    public ImHttpClient(String baseUrl, String userId, int connectTimeoutMs, int requestTimeoutMs) {
        if (baseUrl == null || baseUrl.isBlank()) {
            throw new IllegalArgumentException("baseUrl 不能为空");
        }
        String normalized = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
        if (!normalized.startsWith("http://") && !normalized.startsWith("https://")) {
            throw new IllegalArgumentException("baseUrl 必须以 http:// 或 https:// 开头：" + baseUrl);
        }
        this.baseUrl = normalized;
        this.userId = userId;
        this.requestTimeoutMs = requestTimeoutMs;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(Math.max(1, connectTimeoutMs)))
                .build();
    }

    // ==== 会话与消息 ====

    /**
     * 会话列表（服务端按「置顶优先 → 最后消息时间倒序」排好，未读数已算好）。
     *
     * @param limit ≤0 用服务端默认值 50，上限 200
     */
    public List<ConversationSummary> listConversations(int limit) {
        JsonNode root = execute("GET", "/conv",
                params("userId", userId, "limit", limit > 0 ? limit : null), null);
        return JsonUtils.treeToList(root.path("conversations"), ConversationSummary.class);
    }

    /**
     * 历史消息：<b>最近 N 条</b>，按 seq 升序返回。
     *
     * <p>注意服务端目前<b>没有</b> beforeSeq/cursor，所以「上滑加载更早」这件事当前做不到，
     * SDK 不提供假接口。返回的元素与长连接 {@code msg.push} 同一个结构
     * （撤回过的条目 {@code body} 为空、{@code recalled=true}）。
     */
    public List<DownlinkFrame.Push> history(String conversationId, int limit) {
        JsonNode root = execute("GET", "/conv/" + segment(conversationId) + "/messages",
                params("userId", userId, "limit", limit > 0 ? limit : null), null);
        List<DownlinkFrame.Push> messages = new ArrayList<>();
        for (JsonNode node : root.path("messages")) {
            // 走 Frames 而不是直接 treeToObject：帧形状只在一个地方定义，认不出的帧当作不存在
            if (frames.parse(node) instanceof DownlinkFrame.Push push) {
                messages.add(push);
            }
        }
        return messages;
    }

    /** 上报已读（用户级、只前进，服务端会 cap 到会话的 lastSeq） */
    public HttpResult markRead(String conversationId, long seq) {
        return result(execute("POST", "/conv/" + segment(conversationId) + "/read",
                params("userId", userId, "seq", seq), null));
    }

    /**
     * 撤回消息。
     *
     * <p>服务端限制：只有发送方能撤回（{@code 403}）、有时限（{@code im.recall.window}，默认 2 分钟，
     * 超时也是 {@code 403}）、消息不存在（已被裁剪）是 {@code 404}。
     * 成功后长连接还会推一条 {@code msg.update{change:"RECALLED"}}。
     */
    public HttpResult recall(String conversationId, long seq, String operatorId) {
        return result(execute("POST", "/conv/" + segment(conversationId) + "/messages/" + seq + "/recall",
                params("operatorId", operatorId == null ? userId : operatorId), null));
    }

    public HttpResult recall(String conversationId, long seq) {
        return recall(conversationId, seq, userId);
    }

    /**
     * 本地删除（只影响自己：服务端会推 {@code msg.update{change:"DELETED"}} 给自己其它端）。
     *
     * <p>幂等：删一条已被裁剪的消息也返回 {@code 0}。
     */
    public HttpResult deleteMessage(String conversationId, long seq) {
        return result(execute("POST", "/conv/" + segment(conversationId) + "/messages/" + seq + "/delete",
                params("userId", userId), null));
    }

    /**
     * 置顶 / 免打扰。
     *
     * @param pinned {@code null} = <b>这一项不改</b>（不是设为 false）；两者都 null 会被服务端判 400
     * @param muted  同上
     */
    public FlagResult setFlag(String conversationId, Boolean pinned, Boolean muted) {
        return JsonUtils.treeToObject(execute("POST", "/conv/" + segment(conversationId) + "/flag",
                params("userId", userId, "pinned", pinned, "muted", muted), null), FlagResult.class);
    }

    // ==== 群治理 ====

    /** 建群：{@code ownerId} 自动成为群主并加入成员 */
    public GroupInfo createGroup(String ownerId, String name, int maxMembers) {
        return groupInfo(execute("POST", "/group",
                params("ownerId", ownerId, "name", name, "maxMembers", maxMembers > 0 ? maxMembers : null),
                null));
    }

    /** 群详情（含成员数与角色/禁言表） */
    public GroupInfo group(String groupId) {
        return groupInfo(execute("GET", "/group/" + segment(groupId), params(), null));
    }

    /** 成员列表（成员 id 列表 + 角色表 + 仍在禁言期内的人） */
    public GroupInfo groupMembers(String groupId) {
        return groupInfo(execute("GET", "/group/" + segment(groupId) + "/members", params(), null));
    }

    public GroupInfo renameGroup(String groupId, String operatorId, String name) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/rename",
                params("operatorId", operatorId, "name", name), null));
    }

    public GroupInfo setAnnouncement(String groupId, String operatorId, String text) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/announcement",
                params("operatorId", operatorId, "text", text == null ? "" : text), null));
    }

    public GroupInfo muteAll(String groupId, String operatorId, boolean muted) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/mute-all",
                params("operatorId", operatorId, "muted", muted), null));
    }

    public GroupInfo setMaxMembers(String groupId, String operatorId, int max) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/max-members",
                params("operatorId", operatorId, "max", max), null));
    }

    /** 批量拉人（请求体是 JSON 数组，不是对象） */
    public GroupInfo addMembers(String groupId, String operatorId, List<String> userIds) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/members",
                params("operatorId", operatorId), userIds));
    }

    public GroupInfo removeMember(String groupId, String operatorId, String targetId) {
        return groupInfo(execute("DELETE", "/group/" + segment(groupId) + "/members/" + segment(targetId),
                params("operatorId", operatorId), null));
    }

    /** 退群；群主退群会被拒（{@code 409}，群主只能解散或先转让，转让接口尚未实现） */
    public GroupInfo leaveGroup(String groupId) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/leave",
                params("userId", userId), null));
    }

    /**
     * 设置成员角色。
     *
     * @param role {@code ADMIN} 设管理员、{@code MEMBER} 撤管理员；{@code OWNER} 会被拒（{@code 400}）
     */
    public GroupInfo setRole(String groupId, String operatorId, String targetId, String role) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/roles/" + segment(targetId),
                params("operatorId", operatorId, "role", role), null));
    }

    /**
     * 单人禁言。
     *
     * @param seconds 秒数；{@code <= 0} 表示解除禁言
     */
    public GroupInfo muteMember(String groupId, String operatorId, String targetId, int seconds) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/mute/" + segment(targetId),
                params("operatorId", operatorId, "seconds", seconds), null));
    }

    /** 解散群（逻辑删除，成员集合保留） */
    public GroupInfo disbandGroup(String groupId, String operatorId) {
        return groupInfo(execute("POST", "/group/" + segment(groupId) + "/disband",
                params("operatorId", operatorId), null));
    }

    // ==== 关系链 ====

    /** 好友列表（双向、无序） */
    public List<String> friends(String ownerId) {
        return strings(execute("GET", "/relation/" + segment(ownerId) + "/friends", params(), null), "friends");
    }

    public List<String> addFriends(String ownerId, List<String> friendIds) {
        return strings(execute("POST", "/relation/" + segment(ownerId) + "/friends", params(), friendIds), "friends");
    }

    public List<String> removeFriend(String ownerId, String friendId) {
        return strings(execute("DELETE", "/relation/" + segment(ownerId) + "/friends/" + segment(friendId),
                params(), null), "friends");
    }

    /** 黑名单（单向往、不通知对方） */
    public List<String> blocks(String ownerId) {
        return strings(execute("GET", "/relation/" + segment(ownerId) + "/blocks", params(), null), "blocked");
    }

    public List<String> addBlocks(String ownerId, List<String> blockedIds) {
        return strings(execute("POST", "/relation/" + segment(ownerId) + "/blocks", params(), blockedIds), "blocked");
    }

    public List<String> removeBlock(String ownerId, String blockedId) {
        return strings(execute("DELETE", "/relation/" + segment(ownerId) + "/blocks/" + segment(blockedId),
                params(), null), "blocked");
    }

    @Override
    public void close() {
        http.close();
    }

    // ==== HTTP 管道 ====

    /**
     * 发一次请求并解析 JSON。
     *
     * @param body {@code null} = 无请求体；{@link List}/{@link Map} = JSON；{@link String} = 原样发送
     * @throws ImHttpException 连不上、超时、非 2xx、响应不是 JSON
     */
    private JsonNode execute(String method, String path, Map<String, String> query, Object body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri(path, query))
                .timeout(Duration.ofMillis(Math.max(1, requestTimeoutMs)));
        if (body == null) {
            builder.method(method, HttpRequest.BodyPublishers.noBody());
        } else {
            String payload = body instanceof String raw ? raw : JsonUtils.toJson(body);
            builder.header("Content-Type", "application/json; charset=utf-8")
                    .method(method, HttpRequest.BodyPublishers.ofString(payload, StandardCharsets.UTF_8));
        }
        HttpResponse<String> response;
        try {
            response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new ImHttpException(0, method + ' ' + path + " 请求失败：" + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ImHttpException(0, method + ' ' + path + " 被中断", e);
        }
        String responseBody = response.body() == null ? "" : response.body();
        if (response.statusCode() / 100 != 2) {
            // 框架级错误（缺必填 query、JSON 解析失败）落在这里，形状与业务错误完全不同
            throw new ImHttpException(response.statusCode(),
                    method + ' ' + path + " 返回 " + response.statusCode() + "：" + abbreviate(responseBody));
        }
        if (responseBody.isBlank()) {
            return JsonUtils.toJsonNode("{}");
        }
        try {
            return JsonUtils.toJsonNode(responseBody);
        } catch (RuntimeException e) {
            throw new ImHttpException(response.statusCode(),
                    method + ' ' + path + " 的响应不是 JSON：" + abbreviate(responseBody), e);
        }
    }

    private URI uri(String path, Map<String, String> query) {
        StringBuilder sb = new StringBuilder(baseUrl).append(path);
        if (query != null && !query.isEmpty()) {
            sb.append('?');
            boolean first = true;
            for (Map.Entry<String, String> entry : query.entrySet()) {
                // null = 这一项不传：flag 接口用「不传」表达「不改」，传 false 会把它改掉
                if (entry.getValue() == null) {
                    continue;
                }
                if (!first) {
                    sb.append('&');
                }
                first = false;
                sb.append(encode(entry.getKey())).append('=').append(encode(entry.getValue()));
            }
        }
        return URI.create(sb.toString());
    }

    /** 路径段编码：id 里可能有 {@code :}（单聊会话）甚至 {@code /}，不编码会把路由打乱 */
    private static String segment(String value) {
        return value == null ? "" : encode(value);
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static Map<String, String> params(Object... keyValues) {
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i + 1 < keyValues.length; i += 2) {
            Object value = keyValues[i + 1];
            if (value != null) {
                map.put((String) keyValues[i], String.valueOf(value));
            }
        }
        return map;
    }

    private static HttpResult result(JsonNode root) {
        return JsonUtils.treeToObject(root, HttpResult.class);
    }

    /**
     * 群接口的返回：{@code code/message} 必有，{@code group/members/roles/mutes} 按接口只填一部分。
     *
     * <p>缺失的集合由 {@link GroupInfo} 的紧凑构造器归一成空集合，这里不再逐个字段搬运。
     */
    private static GroupInfo groupInfo(JsonNode root) {
        return JsonUtils.treeToObject(root, GroupInfo.class);
    }

    private static List<String> strings(JsonNode root, String field) {
        return JsonUtils.treeToList(root.path(field), String.class);
    }

    private static String abbreviate(String body) {
        if (body == null) {
            return "";
        }
        return body.length() <= 300 ? body : body.substring(0, 300) + "...";
    }
}
