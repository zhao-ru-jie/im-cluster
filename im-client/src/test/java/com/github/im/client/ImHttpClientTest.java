package com.github.im.client;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 短请求客户端的契约测试：<b>URL 拼装</b>、<b>三类响应形状</b>、<b>参数省略语义</b>。
 *
 * <p>这三个是这层唯一容易错的地方，而且错了都是静默的：
 * query 拼错 → 服务端按默认值处理（看起来「成功了但数据不对」）；
 * 把 {@code code != 0} 当成功 → 用户以为撤回成功；
 * 把 {@code pinned=null} 发成 {@code false} → 顺手把别人的置顶取消了。
 */
class ImHttpClientTest {

    private StubHttp stub;
    private ImHttpClient client;

    @BeforeEach
    void setUp() throws IOException {
        stub = new StubHttp();
        client = new ImHttpClient(stub.baseUrl(), "alice");
    }

    @AfterEach
    void tearDown() {
        client.close();
        stub.close();
    }

    @Test
    @DisplayName("会话列表：解析 11 个字段，limit<=0 时不传（交给服务端默认 50）")
    void listConversations() {
        stub.respond(200, """
                {"userId":"alice","limit":50,"count":1,"conversations":[
                  {"conversationId":"s:alice:bob","type":"SINGLE","lastSeq":7,"lastTime":1700000000000,
                   "lastFrom":"bob","lastDigest":"hi","lastMsgId":"srv-7","readSeq":5,"unread":2,
                   "pinned":true,"muted":false}]}
                """);

        List<ConversationSummary> conversations = client.listConversations(0);

        assertEquals(1, conversations.size());
        ConversationSummary summary = conversations.get(0);
        assertEquals("s:alice:bob", summary.conversationId());
        assertEquals("SINGLE", summary.type());
        assertEquals(7, summary.lastSeq());
        assertEquals(1_700_000_000_000L, summary.lastTime());
        assertEquals("bob", summary.lastFrom());
        assertEquals("srv-7", summary.lastMsgId());
        assertEquals(5, summary.readSeq());
        assertEquals(2, summary.unread());
        assertTrue(summary.pinned());
        assertFalse(summary.muted());
        assertEquals("GET /conv?userId=alice", stub.lastRequest(),
                "limit<=0 表示用服务端默认值，不该发出 limit=0");
    }

    @Test
    @DisplayName("历史消息：元素与 msg.push 同构，撤回条目 recalled=true 且 body 为空")
    void historyParsesPushFrames() {
        stub.respond(200, """
                {"conversationId":"s:alice:bob","limit":2,"count":2,"messages":[
                  {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE",
                   "seq":1,"serverMsgId":"srv-1","clientMsgId":"c-1","from":"bob","to":"alice",
                   "body":"hello","contentType":"TEXT","ts":1700000000000},
                  {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE",
                   "seq":2,"serverMsgId":"srv-2","clientMsgId":"c-2","from":"alice","to":"bob",
                   "body":"","contentType":"TEXT","recalled":true,"ts":1700000001000}]}
                """);

        List<DownlinkFrame.Push> messages = client.history("s:alice:bob", 20);

        assertEquals(2, messages.size());
        assertEquals(1, messages.get(0).seq());
        assertEquals("hello", messages.get(0).body());
        assertFalse(messages.get(0).recalled());
        assertTrue(messages.get(1).recalled());
        assertEquals("", messages.get(1).body(), "撤回后不能再带原文");
        assertTrue(stub.lastRequest().contains("userId=alice"),
                "带上 userId 才会过滤掉自己本地删除的消息");
    }

    @Test
    @DisplayName("历史翻页：beforeSeq 原样带上去，nextBeforeSeq / hasMore 透传给调用方")
    void historyPagingCarriesCursor() {
        stub.respond(200, """
                {"conversationId":"s:alice:bob","limit":2,"count":2,"nextBeforeSeq":11,"hasMore":true,"messages":[
                  {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE",
                   "seq":12,"serverMsgId":"srv-12","clientMsgId":"c-12","from":"bob","to":"alice",
                   "body":"m12","contentType":"TEXT","ts":1700000000000},
                  {"ver":1,"type":"msg.push","conversationId":"s:alice:bob","convType":"SINGLE",
                   "seq":13,"serverMsgId":"srv-13","clientMsgId":"c-13","from":"alice","to":"bob",
                   "body":"m13","contentType":"TEXT","ts":1700000001000}]}
                """);

        HistoryPage page = client.history("s:alice:bob", 13L, 2);

        assertEquals(2, page.messages().size());
        assertEquals(11L, page.nextBeforeSeq(), "游标要能直接用于下一次请求");
        assertTrue(page.hasMore(), "还有更早的消息，客户端应继续上滑");
        assertEquals(2, page.limit(), "服务端实际生效的条数要回显");
        assertTrue(stub.lastRequest().contains("beforeSeq=13"), "游标必须带上去：" + stub.lastRequest());
    }

    @Test
    @DisplayName("业务错误是 HTTP 200 + code：必须按 code 判成功，而不是看状态码")
    void recallPropagatesBusinessCode() {
        stub.respond(200, """
                {"conversationId":"s:alice:bob","seq":7,"code":403,
                 "message":"only the sender can recall this message"}
                """);

        HttpResult result = client.recall("s:alice:bob", 7);

        assertFalse(result.success());
        assertEquals(403, result.code());
        assertTrue(result.message().contains("sender"));
        assertEquals("POST /conv/s%3Aalice%3Abob/messages/7/recall?operatorId=alice",
                stub.lastRequest(),
                "会话 id 里的冒号必须编码，且 operatorId 默认取构造时的 userId");
    }

    @Test
    @DisplayName("置顶/免打扰：传 null 表示「这一项不改」，绝不能发成 false")
    void flagOmitsNullFields() {
        stub.respond(200, """
                {"conversationId":"s:alice:bob","code":0,"pinned":true,"muted":false}
                """);

        FlagResult result = client.setFlag("s:alice:bob", true, null);

        assertTrue(result.success());
        assertTrue(result.pinned());
        assertFalse(result.muted());
        assertTrue(stub.lastRequest().contains("pinned=true"));
        assertFalse(stub.lastRequest().contains("muted="),
                "muted=null 若被发成 muted=false，会把用户原来的免打扰顺手关掉");
    }

    @Test
    @DisplayName("群成员：成员列表 + 角色表 + 仍在禁言期内的人")
    void groupMembersParsesRolesAndMutes() {
        stub.respond(200, """
                {"code":0,"groupId":"g1","size":2,"members":["alice","bob"],
                 "roles":{"alice":"OWNER","bob":"ADMIN"},"mutes":{"bob":1700000600000}}
                """);

        GroupInfo info = client.groupMembers("g1");

        assertTrue(info.success());
        assertEquals(List.of("alice", "bob"), info.members());
        assertEquals("OWNER", info.roles().get("alice"));
        assertEquals(1_700_000_600_000L, info.mutes().get("bob"));
    }

    @Test
    @DisplayName("建群：解析 group 快照；maxMembers<=0 时不传")
    void createGroup() {
        stub.respond(200, """
                {"code":0,"group":{"groupId":"g1","name":"team","ownerId":"alice","announcement":"",
                 "maxMembers":200,"createdAt":1700000000000,"mutedAll":false,"dismissed":false}}
                """);

        GroupInfo info = client.createGroup("alice", "team", 0);

        assertTrue(info.success());
        assertEquals("g1", info.group().groupId());
        assertEquals(200, info.group().maxMembers());
        assertEquals("POST /group?ownerId=alice&name=team", stub.lastRequest());
    }

    @Test
    @DisplayName("批量拉人：请求体是 JSON 数组（服务端契约如此，不是对象）")
    void addMembersSendsJsonArray() {
        stub.respond(200, "{\"code\":0}");

        client.addMembers("g1", "alice", List.of("bob", "carol"));

        assertEquals("[\"bob\",\"carol\"]", stub.lastRequestBody());
        assertTrue(stub.lastRequest().contains("operatorId=alice"));
    }

    @Test
    @DisplayName("关系链：好友列表没有 code 字段，仍然要解析成功")
    void friendsWithoutCode() {
        stub.respond(200, """
                {"userId":"alice","size":2,"friends":["bob","carol"]}
                """);

        assertEquals(List.of("bob", "carol"), client.friends("alice"));
        assertEquals("GET /relation/alice/friends", stub.lastRequest());
    }

    @Test
    @DisplayName("非 2xx（框架级错误）抛 ImHttpException，不伪装成业务错误")
    void nonSuccessStatusThrows() {
        stub.respond(500, "{\"status\":500,\"error\":\"Internal Server Error\"}");

        ImHttpException error = assertThrows(ImHttpException.class,
                () -> client.markRead("s:alice:bob", 3));

        assertEquals(500, error.status());
    }

    /** 桩 HTTP 服务端：记录请求行/请求体，按测试设置的状态码与 JSON 回应 */
    static final class StubHttp implements AutoCloseable {

        private final HttpServer server;
        private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
        private volatile int status = 200;
        private volatile String body = "{}";
        private volatile String lastRequestBody = "";

        StubHttp() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                String query = exchange.getRequestURI().getRawQuery();
                requests.add(exchange.getRequestMethod() + ' ' + exchange.getRequestURI().getRawPath()
                        + (query == null ? "" : "?" + query));
                lastRequestBody = new String(exchange.getRequestBody().readAllBytes(),
                        StandardCharsets.UTF_8);
                byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
                exchange.sendResponseHeaders(status, payload.length);
                try (OutputStream out = exchange.getResponseBody()) {
                    out.write(payload);
                }
            });
            server.start();
        }

        String baseUrl() {
            return "http://127.0.0.1:" + server.getAddress().getPort();
        }

        void respond(int status, String body) {
            this.status = status;
            this.body = body;
        }

        String lastRequest() {
            return requests.isEmpty() ? "" : requests.get(requests.size() - 1);
        }

        String lastRequestBody() {
            return lastRequestBody;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
