package com.github.im.storage.jdbc;

import com.github.im.server.message.StoredMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * group commit 的三条行为约定：<b>并发时合并</b>、<b>稀疏时零额外延迟</b>、<b>失败必传达给调用方</b>。
 *
 * <p>用 mock 的 {@link JdbcTemplate} 就够：这里要验证的是「合批策略与失败语义」，
 * 不是 MySQL 的事务行为（后者靠压测与 {@code innodb_flush_log_at_trx_commit} 调参）。
 */
class JdbcBatchAppenderTest {

    private final JdbcTemplate jdbc = mock(JdbcTemplate.class);

    @Test
    @DisplayName("并发到达的消息合并成一个事务提交，批次数远少于消息数")
    void mergesConcurrentAppendsIntoBatches() throws Exception {
        AtomicInteger batchCalls = new AtomicInteger();
        AtomicInteger maxBatchSize = new AtomicInteger();
        when(jdbc.batchUpdate(anyString(), anyList())).thenAnswer(invocation -> {
            List<?> args = invocation.getArgument(1);
            batchCalls.incrementAndGet();
            maxBatchSize.accumulateAndGet(args.size(), Math::max);
            return new int[args.size()];
        });

        JdbcBatchAppender appender = new JdbcBatchAppender(jdbc, true);
        int threads = 8;
        int perThread = 20;
        CountDownLatch ready = new CountDownLatch(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(threads * perThread);

        for (int t = 0; t < threads; t++) {
            Thread.ofVirtual().start(() -> {
                ready.countDown();
                try {
                    start.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                }
                for (int i = 0; i < perThread; i++) {
                    appender.append(message());
                    finished.countDown();
                }
            });
        }

        assertTrue(ready.await(2, TimeUnit.SECONDS), "线程应全部就绪");
        start.countDown();
        assertTrue(finished.await(5, TimeUnit.SECONDS), "所有消息都应该完成落库");
        appender.close();

        assertTrue(maxBatchSize.get() > 1,
                "并发到达时必须发生合并，实际最大批大小 = " + maxBatchSize.get());
        assertTrue(batchCalls.get() < threads * perThread,
                "批次数 " + batchCalls.get() + " 必须少于消息数 " + (threads * perThread));
    }

    @Test
    @DisplayName("稀疏写入零等待：append 返回时该条已经提交，不额外攒批窗口")
    void singleMessageIsCommittedBeforeReturn() {
        when(jdbc.batchUpdate(anyString(), anyList())).thenReturn(new int[]{1});
        JdbcBatchAppender appender = new JdbcBatchAppender(jdbc, true);

        appender.append(message());

        // 关键点：append 一返回就必须已经提交过，说明没有「等 2ms 再 flush」的固定延迟
        verify(jdbc, times(1)).batchUpdate(anyString(), anyList());
        appender.close();
    }

    @Test
    @DisplayName("整批失败时异常必须传回调用方（由它让客户端重试），不能静默吞掉")
    void failureIsPropagatedToCaller() {
        when(jdbc.batchUpdate(anyString(), anyList()))
                .thenThrow(new DataAccessResourceFailureException("db down"));
        JdbcBatchAppender appender = new JdbcBatchAppender(jdbc, true);

        assertThrows(DataAccessResourceFailureException.class, () -> appender.append(message()));
        appender.close();
    }

    @Test
    @DisplayName("关闭后（批线程已退出）仍能落库，退化为同步写而不是丢消息")
    void writesSynchronouslyAfterClose() {
        JdbcBatchAppender appender = new JdbcBatchAppender(jdbc, true);
        appender.close();

        appender.append(message());

        verify(jdbc, times(1)).update(anyString(),
                any(), any(), any(), any(), any(), any(), any(), any(), any());
    }

    private static StoredMessage message() {
        return new StoredMessage("c1", 1L, "01a16bb1-d2fb-7000-a29a-eb81064be351",
                "alice", "bob", "SINGLE", "m1", "hello", 1_700_000_000_000L);
    }
}
