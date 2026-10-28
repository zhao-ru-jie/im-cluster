package com.github.im.server.message;

import com.github.im.contract.MessageEnvelope;
import com.github.im.contract.RedisKeys;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.conversation.ConversationStore;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupService;
import com.github.im.server.metrics.MessageMetrics;
import com.github.im.server.relation.InMemoryRelationRepository;
import com.github.im.server.relation.MessageAcl;
import com.github.im.server.relation.RelationRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 投递前准入（{@code MessageAcl} + 关系链）的端到端约定。
 *
 * <p>锁住四件事：
 * <ol>
 *   <li><b>好友双向、黑名单单向</b>：存储语义错了，后面所有判定都会错；</li>
 *   <li><b>被拒的消息不占 seq、不进幂等表</b>——所以校验点必须在发号之前，
 *       否则骚扰消息虽然没投出去，也已经消耗了序号并把两者的一致性推到更脆的状态；</li>
 *   <li><b>群 ID 可枚举，非成员发言必须拦</b>（{@code g:1}、{@code g:sales} 这种命名很常见）；</li>
 *   <li><b>off 模式零查询</b>：默认部署不该为了一个没开的策略多付出 Redis 往返。</li>
 * </ol>
 *
 * <p>需要本机 Redis，不可用时自动跳过。
 */
class MessageAclTest {

    private static final Duration TTL = Duration.ofHours(1);

    private final MessageRepository repository = mock(MessageRepository.class);
    private final DeliveryDispatcher delivery = mock(DeliveryDispatcher.class);
    private final SeqAllocator seqAllocator = mock(SeqAllocator.class);
    private final GroupMemberRepository groupMembers = mock(GroupMemberRepository.class);
    private final AtomicLong seq = new AtomicLong(0);
    private final List<String> userIds = new ArrayList<>();
    private final List<String> conversationIds = new ArrayList<>();
    /** 每次 {@code service()} 都会重建 MessageService，关系链必须是同一个实例，否则刚加的好友会“丢” */
    private RelationRepository relations;

    @BeforeEach
    void stubSeq() {
        when(seqAllocator.allocate(anyString(), anyString()))
                .thenAnswer(inv -> new SeqAllocator.Allocation(false, null, seq.incrementAndGet()));
    }

    @AfterEach
    void cleanup() {
        StringRedisTemplate redis = TestRedis.template();
        for (String userId : userIds) {
            redis.delete(RedisKeys.userFriends(userId));
            redis.delete(RedisKeys.userBlocks(userId));
            // 幂等键挂在会话维度上（含随机 clientMsgId），由 24h TTL 自然回收
        }
        for (String conversationId : conversationIds) {
            redis.delete(RedisKeys.convMeta(conversationId));
        }
    }

    @Test
    @DisplayName("好友是双向的，解除也双向生效")
    void friendsAreBidirectional() {
        RelationRepository relations = relations();
        String alice = newUser();
        String bob = newUser();

        relations.addFriends(alice, List.of(bob));

        assertTrue(relations.areFriends(alice, bob));
        assertTrue(relations.areFriends(bob, alice), "只写单边，对方的消息就发不进来了");

        relations.removeFriend(bob, alice);

        assertFalse(relations.areFriends(alice, bob), "解除必须双向，否则留下一条单边残留");
        assertFalse(relations.areFriends(bob, alice));
    }

    @Test
    @DisplayName("黑名单是单向的：本人查得到，被拉黑方查不到")
    void blockIsOneWay() {
        RelationRepository relations = relations();
        String alice = newUser();
        String bob = newUser();

        relations.block(alice, bob);

        assertTrue(relations.isBlocked(alice, bob));
        assertFalse(relations.isBlocked(bob, alice), "反查得到等于告诉对方「你被拉黑了」");
        assertFalse(relations.areFriends(alice, bob), "拉黑不等于好友，两者独立存储");
    }

    @Test
    @DisplayName("被对方拉黑后消息 403，且不占 seq、不进幂等表")
    void blockedSenderIsRejected() {
        String alice = newUser();
        String bob = newUser();
        relations().block(bob, alice);

        SubmitMessageResponse response = service("enforce", false)
                .submit(singleChat(alice, bob, "hi"));

        assertEquals(MessageAcl.CODE_FORBIDDEN, response.getCode());
        assertTrue(response.getMessage().contains("blocked"), response.getMessage());
        verify(seqAllocator, never()).allocate(anyString(), anyString());
    }

    @Test
    @DisplayName("非群成员不能发言，成员可以")
    void groupOutsiderIsRejected() {
        String alice = newUser();
        String groupId = "g-" + UUID.randomUUID();
        when(groupMembers.isMember(groupId, alice)).thenReturn(false);

        assertEquals(MessageAcl.CODE_FORBIDDEN,
                service("enforce", false).submit(groupChat(groupId, alice, "hi")).getCode(),
                "群 ID 往往可枚举，不校验等于任何人可以往任何群发言");

        when(groupMembers.isMember(groupId, alice)).thenReturn(true);
        assertEquals(0, service("enforce", false).submit(groupChat(groupId, alice, "hi")).getCode(),
                "成员发言必须放行");
    }

    @Test
    @DisplayName("off 模式不校验、也不碰关系链（默认部署不付这次往返）")
    void offModeSkipsChecks() {
        String alice = newUser();
        String bob = newUser();
        RelationRepository untouched = mock(RelationRepository.class);

        assertEquals(0, service("off", false, untouched)
                .submit(singleChat(alice, bob, "hi")).getCode(), "off 模式即使被拉黑也放行");
        verifyNoInteractions(untouched);
    }

    @Test
    @DisplayName("require-friend 开启时陌生人发不出去，建立好友后放行")
    void strangerIsRejectedWhenFriendsRequired() {
        String alice = newUser();
        String bob = newUser();

        assertEquals(MessageAcl.CODE_FORBIDDEN,
                service("enforce", true).submit(singleChat(alice, bob, "hi")).getCode());

        relations().addFriends(alice, List.of(bob));

        assertEquals(0, service("enforce", true).submit(singleChat(alice, bob, "hi")).getCode(),
                "好友关系建立后必须放行");
    }

    // ==== 辅助 ====

    /**
     * 关系链用内存替身：Redis 实现已搬到 im-storage-redis，im-server 的测试不能反向依赖它
     * （会构成 reactor 环）。本类要验的是「准入判定该拦谁」，与介质无关；
     * 介质行为（双向写、pipeline、单向黑名单）由 im-storage-redis 侧守。
     */
    private RelationRepository relations() {
        if (relations == null) {
            relations = new InMemoryRelationRepository();
        }
        return relations;
    }

    private MessageService service(String mode, boolean requireFriend) {
        return service(mode, requireFriend, relations());
    }

    private MessageService service(String mode, boolean requireFriend, RelationRepository relations) {
        MessageAcl acl = new MessageAcl(providerOf(relations), groupMembers,
                emptyProvider(), mode, requireFriend);
        return new MessageService(TestRedis.template(), repository,
                new OfflineMessageStore(TestRedis.template(), TTL, 500, TTL),
                new ClientFrames(new ObjectMapper()), delivery, seqAllocator,
                new ClientWatermarkStore(TestRedis.template(), TTL),
                new ConversationStore(TestRedis.template(), TTL, 1000),
                groupMembers, null, acl, null, Duration.ofMinutes(2), new MessageMetrics(null));
    }

    @SuppressWarnings("unchecked")
    private static ObjectProvider<RelationRepository> providerOf(RelationRepository relations) {
        ObjectProvider<RelationRepository> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(relations);
        return provider;
    }

    /**
     * 不装配群治理层：ACL 会退化成「只查是不是成员」，
     * 这里的用例关心关系链与发号时机，不需要群资料。
     */
    @SuppressWarnings("unchecked")
    private static ObjectProvider<GroupService> emptyProvider() {
        ObjectProvider<GroupService> provider = mock(ObjectProvider.class);
        when(provider.getIfAvailable()).thenReturn(null);
        return provider;
    }

    private SubmitMessageRequest singleChat(String from, String to, String body) {
        String conversationId = MessageEnvelope.singleConversationId(from, to);
        conversationIds.add(conversationId);
        return request(from, to, conversationId, MessageEnvelope.CONV_SINGLE, body);
    }

    private SubmitMessageRequest groupChat(String groupId, String from, String body) {
        conversationIds.add("g:" + groupId);
        return request(from, groupId, "g:" + groupId, MessageEnvelope.CONV_GROUP, body);
    }

    private static SubmitMessageRequest request(String from, String to, String conversationId,
                                                String convType, String body) {
        return SubmitMessageRequest.newBuilder()
                .setClientMsgId("c-" + UUID.randomUUID())
                .setConversationId(conversationId)
                .setConvType(convType)
                .setFromUserId(from)
                .setReceiverId(to)
                .setPayload(body)
                .setClientTs(System.currentTimeMillis())
                .build();
    }

    private String newUser() {
        String userId = "acl-test-" + UUID.randomUUID();
        userIds.add(userId);
        return userId;
    }
}
