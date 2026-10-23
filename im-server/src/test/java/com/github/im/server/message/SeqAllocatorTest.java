package com.github.im.server.message;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * {@link SeqAllocator} 的集成测试：跑真实 Redis，验证「逐条原子递增」方案的三条核心承诺。
 *
 * <ol>
 *   <li><b>会话内单调 + 多实例一致的顺序</b>：多个 {@code SeqAllocator} 实例（模拟多实例部署）
 *       交替取号，得到的 seq 必须严格按 Redis 串行化顺序递增——号段方案正是在这里失效的；</li>
 *   <li><b>Redis 计数器丢失/回退时不回绕</b>：冷启动（key 不存在）与下界保护（key 低于下界）两条路径；</li>
 *   <li><b>开销可控</b>：DB 查询按 1/100 摊薄，稳态每条消息只有一次 Redis 调用。</li>
 * </ol>
 *
 * <p>每个用例用自己的随机会话 ID，{@link AfterEach} 只删这些 key。
 */
class SeqAllocatorTest {

    private final List<String> created = new ArrayList<>();

    @AfterEach
    void deleteOnlyOwnKeys() {
        if (created.isEmpty()) {
            return;
        }
        try {
            StringRedisTemplate redis = TestRedis.template();
            created.forEach(conversationId -> redis.delete(TestRedis.seqKey(conversationId)));
        } catch (RuntimeException ignored) {
            // Redis 不可用时用例本身已被标记 skipped，清理失败不该把 skip 变成 failure
        } finally {
            created.clear();
        }
    }

    @Test
    @DisplayName("冷启动：key 不存在时以「已落库最大 seq」为起点，不从 1 重来")
    void coldStartUsesStorageFloor() {
        String conversationId = conversation();
        SeqAllocator allocator = allocator(storageFixed(100L), new ClientWatermarkStore());

        assertEquals(101L, allocator.next(conversationId));
        assertEquals("101", redis().opsForValue().get(TestRedis.seqKey(conversationId)));
    }

    @Test
    @DisplayName("冷启动：客户端水位高于落库最大 seq 时，以水位为起点（防新消息被客户端当历史丢弃）")
    void coldStartUsesClientWatermarkWhenHigherThanStorage() {
        String conversationId = conversation();
        ClientWatermarkStore watermarks = new ClientWatermarkStore();
        watermarks.observe(Map.of(conversationId, 250L));
        SeqAllocator allocator = allocator(storageFixed(100L), watermarks);

        assertEquals(251L, allocator.next(conversationId));
    }

    @Test
    @DisplayName("冷启动：存储与 Redis 同时为空时，只有客户端水位还在（第三道防线）")
    void coldStartUsesClientWatermarkWhenStorageEmpty() {
        String conversationId = conversation();
        ClientWatermarkStore watermarks = new ClientWatermarkStore();
        watermarks.observe(Map.of(conversationId, 5L));
        SeqAllocator allocator = allocator(storageFixed(0L), watermarks);

        assertEquals(6L, allocator.next(conversationId));
    }

    @Test
    @DisplayName("多实例：两个实例交替取号，顺序严格单调且不交错（号段方案的致命缺陷）")
    void twoInstancesShareOneStrictOrder() {
        String conversationId = conversation();
        SeqAllocator instanceA = allocator(storageFixed(0L), new ClientWatermarkStore());
        SeqAllocator instanceB = allocator(storageFixed(0L), new ClientWatermarkStore());

        List<Long> issued = new ArrayList<>();
        for (int i = 1; i <= 20; i++) {
            issued.add((i % 2 == 0 ? instanceA : instanceB).next(conversationId));
        }

        assertEquals(20, issued.size());
        for (int i = 0; i < issued.size(); i++) {
            // 返回顺序必须与「Redis 串行化顺序」一致：1,2,3,...,20
            assertEquals(i + 1L, issued.get(i), "第 " + (i + 1) + " 次取号与串行化顺序不一致: " + issued);
        }
    }

    @Test
    @DisplayName("Redis 回退到旧快照：计数器推进过的号不会重新发一遍（下界保护）")
    void counterRollbackDoesNotRewind() {
        String conversationId = conversation();
        SeqAllocator allocator = allocator(storageFixed(0L), new ClientWatermarkStore());
        for (int i = 0; i < 150; i++) {
            allocator.next(conversationId);
        }

        // 模拟「Redis 从旧快照恢复」：计数器回到 3，但本进程已知最大号是 150
        redis().opsForValue().set(TestRedis.seqKey(conversationId), "3");

        assertEquals(151L, allocator.next(conversationId));
    }

    @Test
    @DisplayName("Redis 整体丢失 + 进程重启：靠客户端水位把号抬回水位之上")
    void redisLossWithProcessRestartUsesWatermark() {
        String conversationId = conversation();
        SeqAllocator beforeRestart = allocator(storageFixed(0L), new ClientWatermarkStore());
        long last = 0;
        for (int i = 0; i < 120; i++) {
            last = beforeRestart.next(conversationId);
        }
        assertEquals(120L, last);

        // 模拟：Redis 被清空（计数器与消息一起没了）且进程已重启（内存状态归零），客户端已消费到 120
        redis().delete(TestRedis.seqKey(conversationId));
        ClientWatermarkStore watermarks = new ClientWatermarkStore();
        watermarks.observe(Map.of(conversationId, last));
        SeqAllocator afterRestart = allocator(storageFixed(0L), watermarks);

        assertEquals(121L, afterRestart.next(conversationId));
    }

    @Test
    @DisplayName("并发：8 线程 × 200 条跨两个实例取号，结果唯一且恰好覆盖 1..1600，无重复无遗漏")
    void concurrentAllocationIsUnique() throws Exception {
        String conversationId = conversation();
        SeqAllocator instanceA = allocator(storageFixed(0L), new ClientWatermarkStore());
        SeqAllocator instanceB = allocator(storageFixed(0L), new ClientWatermarkStore());
        int threads = 8;
        int perThread = 200;

        List<Long> all = Collections.synchronizedList(new ArrayList<>(threads * perThread));
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch go = new CountDownLatch(1);
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        try {
            for (int t = 0; t < threads; t++) {
                SeqAllocator allocator = t % 2 == 0 ? instanceA : instanceB;
                pool.submit(() -> {
                    ready.countDown();
                    awaitOrFail(go);
                    for (int i = 0; i < perThread; i++) {
                        all.add(allocator.next(conversationId));
                    }
                });
            }
            ready.await(10, TimeUnit.SECONDS);
            go.countDown();
            pool.shutdown();
            assertTrue(pool.awaitTermination(60, TimeUnit.SECONDS), "取号线程未在 60s 内结束");
        } finally {
            pool.shutdownNow();
        }

        Set<Long> distinct = new HashSet<>(all);
        assertEquals(threads * perThread, all.size());
        assertEquals(all.size(), distinct.size(), "出现重复 seq，唯一索引会直接爆掉");
        assertEquals(1L, distinct.stream().min(Long::compareTo).orElseThrow());
        assertEquals((long) threads * perThread, distinct.stream().max(Long::compareTo).orElseThrow());
    }

    @Test
    @DisplayName("开销：500 条消息只查库 ~6 次（冷启动 1 次 + 每 100 条校准），不是每条都查")
    void storageFloorRefreshIsAmortized() {
        String conversationId = conversation();
        AtomicInteger storageQueries = new AtomicInteger();
        MessageRepository repository = mock(MessageRepository.class);
        when(repository.maxSeq(anyString())).thenAnswer(invocation -> {
            storageQueries.incrementAndGet();
            return 0L;
        });
        SeqAllocator allocator = allocator(repository, new ClientWatermarkStore());

        for (int i = 0; i < 500; i++) {
            allocator.next(conversationId);
        }

        int queries = storageQueries.get();
        assertTrue(queries > 0, "下界从未校准过");
        assertTrue(queries <= 10, "DB 下界查询没有被摊薄，500 条查了 " + queries + " 次");
    }

    // ==== 辅助 ====

    private String conversation() {
        String conversationId = TestRedis.conversationId();
        created.add(conversationId);
        return conversationId;
    }

    private static StringRedisTemplate redis() {
        return TestRedis.template();
    }

    private static SeqAllocator allocator(MessageRepository repository, ClientWatermarkStore watermarks) {
        return new SeqAllocator(redis(), repository, watermarks);
    }

    private static MessageRepository storageFixed(long maxSeq) {
        AtomicLong current = new AtomicLong(maxSeq);
        MessageRepository repository = mock(MessageRepository.class);
        when(repository.maxSeq(anyString())).thenAnswer(invocation -> current.get());
        return repository;
    }

    private static void awaitOrFail(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("并发起点等待超时");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("并发测试被中断", e);
        }
    }
}
