package com.github.im.storage.redis;

import com.github.im.contract.RedisKeys;
import com.github.im.server.group.GroupInvite;
import com.github.im.server.group.GroupJoinRequest;
import com.github.im.server.group.GroupJoinStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * 邀请码与入群申请的 Redis 实现（连本机 Redis；不可用时整类跳过）。
 *
 * <h2>为什么这个实现必须有集成测试</h2>
 *
 * <p>它有两处<b>只有真连一次才能验证</b>的东西：
 *
 * <ol>
 *   <li><b>限量脚本</b>：{@code maxUses} 的校验与自增在 Lua 里完成。脚本写错（字段名、返回码）
 *       在 Java 侧完全看不出来，症状是「限量链接被无限使用」——而这正是它存在的理由；</li>
 *   <li><b>Hash 字段映射</b>：{@code expires_at / max_uses / uses} 这类字段名在写入与读出两侧
 *       各写一遍，写错一个字母就会静默退化成「永不过期、不限次数」。</li>
 * </ol>
 *
 * <p>其余端口（群成员、群资料）不需要这类测试：它们只是 Set/Hash 的直读直写，
 * 语义在 Java 侧就能被 {@code GroupServiceTest} 用内存实现锁住。
 */
class RedisGroupJoinStoreTest {

    private static LettuceConnectionFactory factory;
    private static StringRedisTemplate redis;
    private static RedisGroupJoinStore store;
    private static boolean available;

    private String groupId;
    private final List<String> codes = new ArrayList<>();
    private final List<String> applicants = new ArrayList<>();

    @BeforeAll
    static void connect() {
        factory = new LettuceConnectionFactory("127.0.0.1", 6379);
        factory.afterPropertiesSet();
        factory.start();
        redis = new StringRedisTemplate(factory);
        try {
            redis.opsForValue().set("im:test:ping", "1", Duration.ofSeconds(5));
            available = true;
        } catch (RuntimeException e) {
            available = false;
        }
        store = new RedisGroupJoinStore(redis, new ObjectMapper(), Duration.ofDays(7), Duration.ofDays(30));
    }

    @AfterAll
    static void disconnect() {
        if (factory != null) {
            factory.destroy();
        }
    }

    @BeforeEach
    void newGroup() {
        assumeTrue(available, "本机没有可用的 Redis，跳过");
        groupId = "g" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
    }

    @AfterEach
    void cleanup() {
        if (!available) {
            return;
        }
        redis.delete(RedisKeys.groupJoinRequests(groupId));
        redis.delete(RedisKeys.groupInvites(groupId));
        codes.forEach(code -> redis.delete(RedisKeys.groupInvite(code)));
        applicants.forEach(applicant -> redis.delete(RedisKeys.groupJoinRequests(groupId)));
    }

    // ==== 邀请码 ====

    @Test
    @DisplayName("限量脚本：maxUses=1 的码只能用一次，第二次返回 EXHAUSTED")
    void inviteRespectsMaxUses() {
        GroupInvite invite = invite(0, 1, false);
        store.saveInvite(invite);

        GroupJoinStore.InviteUse first = store.useInvite(invite.code(), System.currentTimeMillis());
        assertTrue(first.ok(), "第一次必须成功，实际 " + first.status());
        assertEquals(1, first.invite().uses(), "自增后的次数要能被读回来");

        GroupJoinStore.InviteUse second = store.useInvite(invite.code(), System.currentTimeMillis());
        assertFalse(second.ok());
        assertEquals(GroupJoinStore.InviteUse.Status.EXHAUSTED, second.status());
    }

    @Test
    @DisplayName("过期与吊销：过期返回 EXPIRED，吊销后按不存在返回 NOT_FOUND")
    void expiredAndRevokedInvites() {
        GroupInvite expired = new GroupInvite(code(), groupId, "alice", System.currentTimeMillis() - 10_000,
                System.currentTimeMillis() - 1_000, 0, 0, false);
        codes.add(expired.code());
        store.saveInvite(expired);
        assertEquals(GroupJoinStore.InviteUse.Status.EXPIRED,
                store.useInvite(expired.code(), System.currentTimeMillis()).status());

        GroupInvite live = invite(0, 0, false);
        store.saveInvite(live);
        assertTrue(store.useInvite(live.code(), System.currentTimeMillis()).ok());
        store.revokeInvite(groupId, live.code());
        assertNull(store.findInvite(live.code()));
        assertEquals(GroupJoinStore.InviteUse.Status.NOT_FOUND,
                store.useInvite(live.code(), System.currentTimeMillis()).status());
    }

    @Test
    @DisplayName("不存在的码返回 NOT_FOUND：脚本返回值的解码不能把 null 当成成功")
    void unknownInviteIsNotFound() {
        String code = code();
        codes.add(code);

        GroupJoinStore.InviteUse use = store.useInvite(code, System.currentTimeMillis());

        assertEquals(GroupJoinStore.InviteUse.Status.NOT_FOUND, use.status());
        assertNull(use.invite(), "失败时不该顺手带出一个邀请码对象");
        assertNull(store.findInvite(code));
    }

    @Test
    @DisplayName("限量脚本是原子的：8 个并发请求抢 maxUses=3，只会成功 3 次")
    void concurrentUseStopsAtMaxUses() throws Exception {
        GroupInvite invite = invite(0, 3, false);
        store.saveInvite(invite);

        int threads = 8;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<GroupJoinStore.InviteUse.Status>> futures = new ArrayList<>(threads);
        try {
            for (int i = 0; i < threads; i++) {
                futures.add(pool.submit(() -> {
                    start.await();
                    return store.useInvite(invite.code(), System.currentTimeMillis()).status();
                }));
            }
            // 一起放行：串行地跑这个用例就测不出「读-判-写」的竞态
            start.countDown();
            Map<GroupJoinStore.InviteUse.Status, Long> counts = new EnumMap<>(GroupJoinStore.InviteUse.Status.class);
            for (Future<GroupJoinStore.InviteUse.Status> future : futures) {
                counts.merge(future.get(10, TimeUnit.SECONDS), 1L, Long::sum);
            }

            assertEquals(3L, counts.getOrDefault(GroupJoinStore.InviteUse.Status.OK, 0L),
                    "成功次数必须正好等于 maxUses，实际分布 " + counts);
            assertEquals(5L, counts.getOrDefault(GroupJoinStore.InviteUse.Status.EXHAUSTED, 0L),
                    "其余请求应当看到次数用尽，实际分布 " + counts);
        } finally {
            pool.shutdownNow();
        }
        assertEquals(3, store.findInvite(invite.code()).uses(), "自增次数不能多也不能少");
    }

    @Test
    @DisplayName("邀请码列表只返回群内仍有效的码，且带审批开关")
    void invitesOfFiltersExpired() {
        GroupInvite live = invite(0, 0, true);
        store.saveInvite(live);
        GroupInvite dead = new GroupInvite(code(), groupId, "alice", System.currentTimeMillis() - 10_000,
                System.currentTimeMillis() - 1_000, 0, 0, false);
        codes.add(dead.code());
        store.saveInvite(dead);

        List<GroupInvite> invites = store.invitesOf(groupId);

        assertEquals(1, invites.size(), "过期的码不该出现在管理列表里");
        assertEquals(live.code(), invites.get(0).code());
        assertTrue(invites.get(0).approvalRequired(), "approval_required 字段不能读丢");
    }

    // ==== 入群申请 ====

    @Test
    @DisplayName("申请按人覆盖、按首次申请时间排序，删除后查不到")
    void requestsRoundTrip() {
        String alice = applicant();
        String bob = applicant();
        long now = System.currentTimeMillis();
        store.saveRequest(GroupJoinRequest.pending(groupId, bob, "后来", now + 5));
        store.saveRequest(GroupJoinRequest.pending(groupId, alice, "先来", now));

        // 同一个人重复申请：覆盖，不新增
        store.saveRequest(GroupJoinRequest.pending(groupId, alice, "改了附言", now, now + 10));
        List<GroupJoinRequest> pending = store.pendingRequests(groupId);
        assertEquals(2, pending.size(), "一人一条");
        assertEquals(alice, pending.get(0).applicantId(), "先申请的排前面");
        assertEquals("改了附言", pending.get(0).message());

        store.deleteRequest(groupId, alice);
        assertNull(store.findRequest(groupId, alice));
        assertEquals(1, store.pendingRequests(groupId).size());
    }

    @Test
    @DisplayName("已处理的申请不再出现在待审列表")
    void approvedRequestsLeaveTheQueue() {
        String alice = applicant();
        store.saveRequest(GroupJoinRequest.pending(groupId, alice, "", System.currentTimeMillis()));
        store.saveRequest(store.findRequest(groupId, alice)
                .withStatus(GroupJoinRequest.STATUS_APPROVED, System.currentTimeMillis()));

        assertTrue(store.pendingRequests(groupId).isEmpty());
        assertEquals(GroupJoinRequest.STATUS_APPROVED,
                store.findRequest(groupId, alice).status(), "终态仍然查得到（申请人要能看结论）");
    }

    // ==== 辅助 ====

    private GroupInvite invite(long expiresInMillis, int maxUses, boolean approvalRequired) {
        long now = System.currentTimeMillis();
        GroupInvite invite = new GroupInvite(code(), groupId, "alice", now,
                expiresInMillis <= 0 ? 0 : now + expiresInMillis, maxUses, 0, approvalRequired);
        codes.add(invite.code());
        return invite;
    }

    private String code() {
        return UUID.randomUUID().toString().replace("-", "").substring(0, GroupInvite.CODE_LENGTH);
    }

    private String applicant() {
        String userId = "u" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        applicants.add(userId);
        return userId;
    }
}
