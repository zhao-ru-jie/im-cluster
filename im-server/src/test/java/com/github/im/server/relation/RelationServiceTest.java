package com.github.im.server.relation;

import com.github.im.contract.PushPort;
import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;
import com.github.im.server.message.ClientFrames;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 好友申请状态机的规则。
 *
 * <p>这一组用例锁的是「<b>只有对方同意之后才互为好友</b>」这一条产品规则，
 * 以及围绕它的边界：自申请、已是好友、被拉黑、重复申请、双方同时申请、终态不可重入。
 * 每一条都对应一个能在真实客户端上复现的现象，注释里写的是「错了会怎样」。
 *
 * <h2>为什么断言用 {@link #awaitFrame} 而不是 {@code poll} + {@code clear()}</h2>
 *
 * <p>推送是异步的（virtual thread + fire-and-forget），「清空队列」与「下一条帧入队」
 * 之间没有顺序保证：本意是丢弃上一步的通知，实际经常把本步要断言的那条丢掉、
 * 或者把上一步的残帧当成本步的第一条。<b>按帧内容等到目标为止</b>才是稳定的写法——
 * 不属于本次断言的帧直接跳过，与它什么时候到无关。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class RelationServiceTest {

    /** 等一条「应该来」的帧：够宽松，避免慢机器上偶发失败 */
    private static final long WAIT_MS = 3_000;
    /** 等一条「不该来」的帧：只等一小会儿，不为它拖慢整个套件 */
    private static final long QUIET_MS = 300;

    private InMemoryRelationRepository relations;
    private InMemoryFriendRequestRepository requests;
    private BlockingQueue<PushTask> pushed;
    private RelationService service;

    @BeforeEach
    void setUp() {
        relations = new InMemoryRelationRepository();
        requests = new InMemoryFriendRequestRepository();
        pushed = new LinkedBlockingQueue<>();
        service = new RelationService(relations, requests, notifier(true));
    }

    @Test
    @DisplayName("申请之后仍然不是好友：关系只能由对方同意来建立")
    void requestAloneDoesNotCreateFriendship() {
        RelationService.Result result = service.request("alice", "bob", "我是小李");

        assertTrue(result.ok(), result.message());
        assertEquals("PENDING", result.request().status());
        assertEquals("我是小李", result.request().message());
        assertFalse(relations.areFriends("alice", "bob"),
                "发出申请就变成好友的话，「经过对方同意」这条规则等于不存在");
        assertTrue(relations.friendsOf("alice").isEmpty(), "申请方的通讯录里不该出现对方");
        assertTrue(relations.friendsOf("bob").isEmpty(), "被申请方更不该凭空多出好友");
    }

    @Test
    @DisplayName("申请会立刻通知被申请人，帧里带申请人与验证消息")
    void requestNotifiesTheOtherSide() throws Exception {
        service.request("alice", "bob", "我是产品的小李");

        PushTask task = awaitFrame("\"type\":\"relation.request\"");
        assertNotNull(task, "不推的话对方只能主动刷新通讯录才知道有人加他");
        assertEquals(List.of("bob"), task.getTargetUserIdsList(), "推给被申请人，不是申请人");
        assertTrue(task.getFrame().contains("\"fromId\":\"alice\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"message\":\"我是产品的小李\""),
                "验证消息必须带上，否则对方只看到一个 userId，无从判断该不该同意：" + task.getFrame());
    }

    @Test
    @DisplayName("同意：双方互为好友，申请人收到 ACCEPTED")
    void acceptCreatesFriendshipBothWays() throws Exception {
        service.request("alice", "bob", "");
        assertNotNull(awaitFrame("\"type\":\"relation.request\""), "申请通知先到，后面的断言才有意义");

        RelationService.Result result = service.accept("bob", "alice");

        assertTrue(result.ok(), result.message());
        assertTrue(relations.areFriends("alice", "bob"), "同意之后两边都必须是好友");
        assertTrue(result.friends().contains("alice"), "返回值里带好友列表，省掉前端一次重拉");
        assertEquals("ACCEPTED", requests.find("alice", "bob").status());

        PushTask task = awaitFrame("\"change\":\"ACCEPTED\"");
        assertNotNull(task, "申请人必须知道结果，否则他一直停在「等待验证」");
        assertEquals(List.of("alice"), task.getTargetUserIdsList(), "结果推给申请人，不是审批人");
        assertTrue(task.getFrame().contains("\"type\":\"relation.update\""), task.getFrame());
        assertTrue(task.getFrame().contains("\"peerId\":\"bob\""), task.getFrame());
    }

    @Test
    @DisplayName("拒绝：不建立关系，但申请人要知道结果")
    void rejectKeepsStrangersApart() throws Exception {
        service.request("alice", "bob", "");
        assertNotNull(awaitFrame("\"type\":\"relation.request\""));

        RelationService.Result result = service.reject("bob", "alice");

        assertTrue(result.ok(), result.message());
        assertFalse(relations.areFriends("alice", "bob"), "拒绝就是拒绝，不能顺手把关系写上");
        assertEquals("REJECTED", requests.find("alice", "bob").status());

        PushTask task = awaitFrame("\"change\":\"REJECTED\"");
        assertNotNull(task);
        assertEquals(List.of("alice"), task.getTargetUserIdsList());
    }

    @Test
    @DisplayName("同意/拒绝是终态：第二次调用返回 404，不重复通知")
    void terminalStateIsNotReentrant() throws Exception {
        service.request("alice", "bob", "");
        assertTrue(service.accept("bob", "alice").ok());
        assertNotNull(awaitFrame("\"change\":\"ACCEPTED\""), "先把同意那一条消费掉，再看有没有多余的");

        RelationService.Result again = service.accept("bob", "alice");

        assertEquals(RelationService.CODE_NOT_FOUND, again.code(),
                "连点两下「同意」的第二下必须是 404，而不是再推一次通知");
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("不能加自己 / 已是好友不能再申请")
    void invalidRequestsAreRejected() {
        assertEquals(RelationService.CODE_BAD_REQUEST, service.request("alice", "alice", "").code());

        relations.addFriends("alice", List.of("bob"));
        assertEquals(RelationService.CODE_CONFLICT, service.request("alice", "bob", "").code(),
                "已是好友还发申请，会让对方收到一条无意义的提示");
    }

    @Test
    @DisplayName("重复申请幂等：不堆记录，也不再打扰对方")
    void duplicateRequestIsIdempotent() throws Exception {
        service.request("alice", "bob", "第一次");
        assertNotNull(awaitFrame("\"type\":\"relation.request\""));

        RelationService.Result second = service.request("alice", "bob", "第二次");

        assertTrue(second.ok(), second.message());
        assertEquals(1, requests.incoming("bob").size(), "重复申请不能堆出两条记录");
        assertEquals("第二次", requests.find("alice", "bob").message(), "附言以最后一次为准");
        assertNull(pushed.poll(QUIET_MS, TimeUnit.MILLISECONDS),
                "同一个人申请两次不该给对方弹两次提示——他只是又点了一下按钮");
    }

    @Test
    @DisplayName("被拒绝后可以重新申请，并且会重新通知")
    void canReapplyAfterRejection() throws Exception {
        service.request("alice", "bob", "");
        service.reject("bob", "alice");
        assertNotNull(awaitFrame("\"change\":\"REJECTED\""));

        RelationService.Result again = service.request("alice", "bob", "再试一次");

        assertTrue(again.ok(), again.message());
        assertEquals("PENDING", requests.find("alice", "bob").status());
        assertNotNull(awaitFrame("\"type\":\"relation.request\""),
                "重新申请是一条新事件，对方的列表要从「已拒绝」变回待处理");
    }

    @Test
    @DisplayName("双方同时申请：直接互为好友，不再要求第二个同意")
    void mutualRequestsBecomeFriends() throws Exception {
        service.request("bob", "alice", "先申请的那个");
        assertNotNull(awaitFrame("\"type\":\"relation.request\""));

        RelationService.Result result = service.request("alice", "bob", "后申请的那个");

        assertTrue(result.ok(), result.message());
        assertTrue(relations.areFriends("alice", "bob"),
                "双方都想加对方还要求再点一次同意，会卡成两边都在等对方审批");
        assertEquals("ACCEPTED", requests.find("bob", "alice").status());

        PushTask task = awaitFrame("\"change\":\"ACCEPTED\"");
        assertNotNull(task, "原来那个申请人也要知道「不用再等了」");
        assertEquals(List.of("bob"), task.getTargetUserIdsList());
    }

    @Test
    @DisplayName("被对方拉黑时申请发不出去，且不泄露「你被拉黑了」")
    void blockedByTargetIsForbidden() {
        relations.block("bob", "alice");

        RelationService.Result result = service.request("alice", "bob", "");

        assertEquals(RelationService.CODE_FORBIDDEN, result.code());
        assertFalse(result.message().contains("拉黑"),
                "把「你被对方拉黑了」写进提示等于替对方通知了他，这是信息泄露");
        assertNull(requests.find("alice", "bob"), "发不出去的申请不该落库");
    }

    @Test
    @DisplayName("我自己拉黑了对方：提示先解除黑名单（这是我自己的操作，可以说清楚）")
    void blockedBySelfAsksToUnblock() {
        relations.block("alice", "bob");

        RelationService.Result result = service.request("alice", "bob", "");

        assertEquals(RelationService.CODE_FORBIDDEN, result.code());
        assertTrue(result.message().contains("黑名单"), result.message());
    }

    @Test
    @DisplayName("撤回：记录消失，被申请人收到 CANCELLED")
    void cancelRemovesAndNotifies() throws Exception {
        service.request("alice", "bob", "");
        assertNotNull(awaitFrame("\"type\":\"relation.request\""));

        RelationService.Result result = service.cancel("alice", "bob");

        assertTrue(result.ok(), result.message());
        assertNull(requests.find("alice", "bob"), "撤回后申请人列表里不该再有这条");
        assertTrue(requests.incoming("bob").isEmpty());

        PushTask task = awaitFrame("\"change\":\"CANCELLED\"");
        assertNotNull(task, "不推的话对方列表里那条申请会一直挂着");
        assertEquals(List.of("bob"), task.getTargetUserIdsList());
    }

    @Test
    @DisplayName("只能撤回自己发出的、仍待处理的申请")
    void cannotCancelOthersRequest() {
        service.request("alice", "bob", "");

        // bob 想「撤回 alice 发给自己的申请」：方向不对，查不到
        assertEquals(RelationService.CODE_NOT_FOUND, service.cancel("bob", "alice").code());
        assertNotNull(requests.find("alice", "bob"), "别人的申请不能被我删掉");
    }

    @Test
    @DisplayName("申请列表按最近变更倒序，两个视角各自只含与自己相关的条目")
    void listsAreScopedAndOrdered() {
        service.request("bob", "alice", "");
        service.request("carol", "alice", "");

        List<FriendRequest> incoming = service.incoming("alice");
        assertEquals(2, incoming.size());
        assertTrue(incoming.get(0).updatedAt() >= incoming.get(1).updatedAt(), "最近处理的排前面");

        assertEquals(1, service.outgoing("bob").size());
        assertTrue(service.outgoing("alice").isEmpty(), "alice 只是被申请人，不该有自己的发出列表");
    }

    // ==== 辅助 ====

    /** 等到第一条包含 {@code needle} 的帧；不属于本次断言的帧直接跳过 */
    private PushTask awaitFrame(String needle) throws InterruptedException {
        long deadline = System.currentTimeMillis() + WAIT_MS;
        while (System.currentTimeMillis() < deadline) {
            PushTask task = pushed.poll(100, TimeUnit.MILLISECONDS);
            if (task != null && task.getFrame().contains(needle)) {
                return task;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private RelationEventNotifier notifier(boolean enabled) {
        ObjectProvider<PushPort> provider = mock(ObjectProvider.class);
        PushPort pushPort = task -> {
            pushed.add(task);
            return PushResult.newBuilder().addAllDeliveredUserIds(task.getTargetUserIdsList()).build();
        };
        when(provider.getIfAvailable()).thenReturn(pushPort);
        return new RelationEventNotifier(new ClientFrames(new ObjectMapper()), provider, enabled);
    }
}
