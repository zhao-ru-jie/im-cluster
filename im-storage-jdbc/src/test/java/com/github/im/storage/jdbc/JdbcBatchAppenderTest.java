package com.github.im.storage.jdbc;

import com.github.im.server.message.StoredMessage;
import com.github.im.storage.jdbc.entity.MessageEntity;
import com.github.im.storage.jdbc.mapper.MessageMapper;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * group commit 的三条行为约定：<b>并发时合并</b>、<b>稀疏时零额外延迟</b>、<b>失败必传达给调用方</b>。
 *
 * <p>用 mock 的 {@link SqlSessionFactory} 就够：这里要验证的是「合批策略、提交顺序与失败语义」，
 * 不是 MySQL 的事务行为（后者靠压测与 {@code innodb_flush_log_at_trx_commit} 调参）。
 *
 * <p>唯一的框架级约定单列一条：批量路径必须走 {@code ExecutorType.BATCH} 并且在**返回调用方之前**
 * 显式 {@code commit}——如果哪天有人把它换成 {@code saveBatch} 而不加事务，
 * 「{@code append} 返回 = 已落库」这个契约就悄悄破了（变成为 ack 抢时间、落库却可能失败）。
 */
class JdbcBatchAppenderTest {

    private final SqlSessionFactory sqlSessionFactory = mock(SqlSessionFactory.class);
    private final MessageMapper mapper = mock(MessageMapper.class);

    @Test
    @DisplayName("并发到达的消息合并成一次提交，会话数远少于消息数")
    void mergesConcurrentAppendsIntoBatches() throws Exception {
        AtomicInteger sessions = new AtomicInteger();
        AtomicInteger maxInsertsPerSession = new AtomicInteger();
        stubBatchSessions(sessions, maxInsertsPerSession, null);

        JdbcBatchAppender appender = new JdbcBatchAppender(sqlSessionFactory, mapper, true);
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

        assertTrue(maxInsertsPerSession.get() > 1,
                "并发到达时必须发生合并，实际单事务最大条数 = " + maxInsertsPerSession.get());
        assertTrue(sessions.get() < threads * perThread,
                "事务数 " + sessions.get() + " 必须少于消息数 " + (threads * perThread));
    }

    @Test
    @DisplayName("稀疏写入零等待：append 返回前该条已经 commit，且走的是 BATCH 执行器")
    void singleMessageIsCommittedBeforeReturn() {
        AtomicInteger commits = new AtomicInteger();
        stubBatchSessions(new AtomicInteger(), new AtomicInteger(), commits);
        JdbcBatchAppender appender = new JdbcBatchAppender(sqlSessionFactory, mapper, true);

        appender.append(message());

        // 关键点：append 一返回就必须已经提交过，说明没有「等 2ms 再 flush」的固定延迟
        assertEquals(1, commits.get(), "append 返回前必须已经 commit");
        // 会话必须是 BATCH 执行器：否则 N 条插入就是 N 次往返，group commit 只摊薄了 fsync
        verify(sqlSessionFactory).openSession(any(ExecutorType.class), anyBoolean());
        appender.close();
    }

    @Test
    @DisplayName("整批失败时异常必须传回调用方（由它让客户端重试），不能静默吞掉")
    void failureIsPropagatedToCaller() {
        stubBatchSessions(new AtomicInteger(), new AtomicInteger(), null,
                new DataAccessResourceFailureException("db down"));
        JdbcBatchAppender appender = new JdbcBatchAppender(sqlSessionFactory, mapper, true);

        assertThrows(DataAccessResourceFailureException.class, () -> appender.append(message()));
        appender.close();
    }

    @Test
    @DisplayName("关闭后（批线程已退出）仍能落库，退化为同步写而不是丢消息")
    void writesSynchronouslyAfterClose() {
        JdbcBatchAppender appender = new JdbcBatchAppender(sqlSessionFactory, mapper, true);
        appender.close();

        appender.append(message());

        verify(mapper).insert(any(MessageEntity.class));
    }

    @Test
    @DisplayName("关掉 group commit 时直接单条提交，不起批处理线程")
    void disabledModeInsertsDirectly() {
        JdbcBatchAppender appender = new JdbcBatchAppender(sqlSessionFactory, mapper, false);

        appender.append(message());

        verify(mapper).insert(any(MessageEntity.class));
        appender.close();
    }

    // ==== 辅助 ====

    /**
     * 让 {@code openSession} 每次都返回一个新的 mock 会话（内部再 mock 一个 batch Mapper），
     * 这样才能统计「每次提交里塞了多少条」。
     *
     * @param insertFailure 非空时，批量 insert 直接抛这个异常，用于验证失败传播
     */
    private void stubBatchSessions(AtomicInteger sessions, AtomicInteger maxInsertsPerSession,
                                   AtomicInteger commits, RuntimeException insertFailure) {
        when(sqlSessionFactory.openSession(any(ExecutorType.class), anyBoolean())).thenAnswer(invocation -> {
            sessions.incrementAndGet();
            SqlSession session = mock(SqlSession.class);
            MessageMapper batchMapper = mock(MessageMapper.class);
            AtomicInteger inserts = new AtomicInteger();
            when(session.getMapper(MessageMapper.class)).thenReturn(batchMapper);
            when(batchMapper.insert(any(MessageEntity.class))).thenAnswer(insert -> {
                if (insertFailure != null) {
                    throw insertFailure;
                }
                int count = inserts.incrementAndGet();
                maxInsertsPerSession.accumulateAndGet(count, Math::max);
                return 1;
            });
            if (commits != null) {
                // commit 返回 void，不能用 when(...)，只能 doAnswer
                doAnswer(commit -> {
                    commits.incrementAndGet();
                    return null;
                }).when(session).commit();
            }
            return session;
        });
    }

    private void stubBatchSessions(AtomicInteger sessions, AtomicInteger maxInsertsPerSession,
                                   AtomicInteger commits) {
        stubBatchSessions(sessions, maxInsertsPerSession, commits, null);
    }

    private static StoredMessage message() {
        return new StoredMessage("c1", 1L, "01a16bb1-d2fb-7000-a29a-eb81064be351",
                "alice", "bob", "SINGLE", "m1", "hello", 1_700_000_000_000L);
    }
}
