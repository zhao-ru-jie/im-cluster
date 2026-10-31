package com.github.im.server.message;

import com.github.im.contract.resilience.RedisDegrade;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.RedisConnectionFailureException;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link SeqAllocator} 在 <b>Redis 不可用</b>时的降级行为。
 *
 * <p>与 {@link SeqAllocatorTest} 互补：那个跑真实 Redis，验证正常态的核心承诺；
 * 这个用一个「所有命令都抛连接失败」的 Redis 模板，验证故障态的承诺：
 *
 * <ol>
 *   <li>不抛异常 —— Redis 挂不能让上行链路变成 5xx；</li>
 *   <li>号仍然单调递增、且从「已落库最大 seq」之上开始 —— 不能发一个比历史更小的号，
 *       否则客户端会把它当旧消息丢掉；</li>
 *   <li>进程内幂等表在 Redis 不可用时仍然能吃掉重复上行。</li>
 * </ol>
 */
class SeqAllocatorDegradeTest {

    private static final String CONVERSATION = "g:degraded-conv";
    private static final long STORAGE_MAX_SEQ = 42L;

    @BeforeEach
    void setUp() {
        RedisDegrade.reset();
        // 阈值 1 + 短窗口：用例只关心「失败后怎么走」，不想等熔断窗口
        RedisDegrade.configure(1, 200);
    }

    @AfterEach
    void tearDown() {
        RedisDegrade.reset();
        RedisDegrade.configure(3, 2_000);
    }

    @Test
    @DisplayName("Redis 不可用时不抛异常，从「已落库最大 seq + 1」继续发号")
    void nextDegradesToStorageFloor() {
        SeqAllocator allocator = allocator();

        assertEquals(STORAGE_MAX_SEQ + 1, allocator.next(CONVERSATION));
        assertEquals(STORAGE_MAX_SEQ + 2, allocator.next(CONVERSATION));
    }

    @Test
    @DisplayName("降级期并发取号：唯一且严格递增（先读再写会在这里发出重复号）")
    void degradedNextIsUniqueUnderConcurrency() throws Exception {
        SeqAllocator allocator = allocator();
        int threads = 8;
        int perThread = 50;
        List<Long> all = java.util.Collections.synchronizedList(new ArrayList<>());
        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            Thread worker = new Thread(() -> {
                for (int j = 0; j < perThread; j++) {
                    all.add(allocator.next(CONVERSATION));
                }
            });
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join(10_000);
        }

        Set<Long> distinct = new HashSet<>(all);
        assertEquals(threads * perThread, all.size());
        assertEquals(all.size(), distinct.size(), "降级期发出重复号，落库会撞唯一键");
        assertTrue(distinct.stream().min(Long::compareTo).orElseThrow() > STORAGE_MAX_SEQ,
                "降级号必须整体高于已落库的最大 seq");
    }

    @Test
    @DisplayName("allocate() 在 Redis 不可用时也走降级发号，而不是把异常抛给受理链路")
    void allocateDegradesInsteadOfThrowing() {
        SeqAllocator allocator = allocator();

        SeqAllocator.Allocation allocation = allocator.allocate("idem:degraded-1", CONVERSATION);

        assertFalse(allocation.duplicate());
        assertEquals(STORAGE_MAX_SEQ + 1, allocation.seq());
    }

    @Test
    @DisplayName("进程内幂等表：Redis 不可用时同一实例的重试仍然回放旧值、不再消耗 seq")
    void localIdemTableReplaysWithoutRedis() {
        SeqAllocator allocator = allocator();
        String idemKey = "idem:degraded-2";

        SeqAllocator.Allocation first = allocator.allocate(idemKey, CONVERSATION);
        // 模拟 MessageService 落库成功之后写入的幂等记录
        allocator.rememberIdem(idemKey, "msg-9|" + first.seq());

        SeqAllocator.Allocation second = allocator.allocate(idemKey, CONVERSATION);

        assertTrue(second.duplicate(), "同一实例的重试必须命中本地幂等");
        assertEquals("msg-9|" + first.seq(), second.idemValue());
        assertEquals(1, allocator.localIdemSize());
    }

    /** 所有 Redis 命令都抛「连接失败」的模板 */
    private SeqAllocator allocator() {
        return new SeqAllocator(deadRedis(), repository(), new ClientWatermarkStore());
    }

    private static MessageRepository repository() {
        MessageRepository repository = mock(MessageRepository.class);
        when(repository.maxSeq(anyString())).thenReturn(STORAGE_MAX_SEQ);
        return repository;
    }

    @SuppressWarnings("unchecked")
    private static StringRedisTemplate deadRedis() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        when(redis.execute(any(RedisCallback.class)))
                .thenThrow(new RedisConnectionFailureException("redis down"));
        return redis;
    }
}
