package com.github.im.storage.jdbc;

import com.github.im.server.message.StoredMessage;
import com.github.im.storage.jdbc.entity.MessageEntity;
import com.github.im.storage.jdbc.mapper.MessageMapper;
import org.apache.ibatis.session.ExecutorType;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * MySQL 落库的 group commit：把「几乎同时到达的多条插入」合并成一个事务提交。
 *
 * <h2>为什么需要</h2>
 *
 * <p>单条 {@code INSERT} 自动提交 = 每条消息一次 fsync。在 {@code innodb_flush_log_at_trx_commit=1}
 * 的默认配置下，一次 fsync 就是 0.3~3ms（取决于盘），单个库的写入天花板因此被压在
 * <b>几百到三千 QPS</b>，且和连接池大小无关——加连接只会让更多事务争抢同一个 redo 刷盘。
 *
 * <p>把 N 条插入放进一个事务，fsync 摊薄成 1/N。N=64 时同样的盘能到几万 QPS。
 *
 * <h2>为什么不用 {@code saveBatch}</h2>
 *
 * <p>MyBatis-Plus 的 {@code saveBatch} 是「开一个 {@code ExecutorType.BATCH} 的 SqlSession，
 * 循环 insert，最后 `flushStatements`」，但它<b>不负责提交</b>——提交时机由 Spring 事务决定。
 * 本类的契约是「{@code append} 返回 = 已提交」，所以这里显式地自己开 BATCH 会话并
 * {@code commit()}，不把「是否已落库」这个判断交给外层有没有开事务。
 *
 * <h2>为什么不在「攒批窗口」上浪费延迟</h2>
 *
 * <p>常见写法是「等满 2ms 再提交」，低负载时每条消息平白多 2ms。这里用的是
 * 「<b>只在已经排队时才攒批</b>」：
 * <ol>
 *   <li>调用方把消息放进队列并等自己的 future；</li>
 *   <li>后台线程阻塞等第一条，醒来后<b>非阻塞地</b>把队列里现有的全部取出；</li>
 *   <li>一个 BATCH 会话提交，complete 所有 future。</li>
 * </ol>
 * 于是：消息稀疏时队列里只有一条 → 立即提交（<b>零额外延迟</b>）；
 * 消息密集时队列里本来就有很多条 → 自然合并成大事务。Nagle 算法的同一个思路。
 *
 * <h2>失败与边界</h2>
 *
 * <ul>
 *   <li>整批失败（如唯一索引冲突、连接断）：整批 completeExceptionally，调用方收到失败并让客户端重试。
 *       一条坏消息会拖累同批的其它消息，但重试时 Redis 幂等表会让它们直接回放，代价可接受；</li>
 *   <li>队列满 / 已关闭：<b>就地同步写</b>，绝不丢消息，也不让调用方无限等；</li>
 *   <li>关闭时把队列里剩下的刷完再退出。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
final class JdbcBatchAppender implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(JdbcBatchAppender.class);

    /** 一个事务里最多合并多少条：太大则失败时回滚面变大，太小则 fsync 摊不薄 */
    private static final int MAX_BATCH = 64;

    /** 队列容量：真正的上限是「同时等待落库的消息数」，超了就走同步写 */
    private static final int QUEUE_CAPACITY = 8_192;

    private record Pending(StoredMessage message, CompletableFuture<Void> done) {
    }

    private final SqlSessionFactory sqlSessionFactory;
    /** 同步写路径用：Spring 注入的 Mapper 代理每条语句自动提交 */
    private final MessageMapper mapper;
    private final BlockingQueue<Pending> queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);
    private final Thread flusher;
    private final boolean enabled;
    private volatile boolean running = true;

    JdbcBatchAppender(SqlSessionFactory sqlSessionFactory, MessageMapper mapper, boolean enabled) {
        this.sqlSessionFactory = sqlSessionFactory;
        this.mapper = mapper;
        this.enabled = enabled;
        this.flusher = Thread.ofPlatform()
                .name("im-db-batcher")
                .daemon(true)
                .unstarted(this::loop);
        if (enabled) {
            flusher.start();
            log.info("[storage] 消息落库已启用 group commit：单事务最多合并 {} 条", MAX_BATCH);
        } else {
            log.info("[storage] 消息落库为单条提交（im.db.batch-enabled=false）");
        }
    }

    /**
     * 落一条消息，<b>返回即已确认落库</b>（成功或抛出异常）。
     *
     * @throws RuntimeException 落库失败（调用方据此让客户端重试）
     */
    void append(StoredMessage message) {
        if (!enabled || !running) {
            insertNow(message);
            return;
        }
        CompletableFuture<Void> done = new CompletableFuture<>();
        if (!queue.offer(new Pending(message, done))) {
            // 队列满：说明 DB 已经跟不上，同步写把压力直接反馈给调用方（变慢而不是 OOM）
            insertNow(message);
            return;
        }
        await(done, message);
    }

    private void await(CompletableFuture<Void> done, StoredMessage message) {
        try {
            done.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted while waiting for message " + message.serverMsgId(), e);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause() == null ? e : e.getCause();
            if (cause instanceof RuntimeException runtime) {
                throw runtime;
            }
            throw new IllegalStateException(cause.getMessage(), cause);
        }
    }

    private void insertNow(StoredMessage message) {
        mapper.insert(MessageEntity.from(message));
    }

    /** 后台线程：阻塞等第一条 → 非阻塞抽干队列 → 一个事务提交 */
    private void loop() {
        while (running) {
            Pending first;
            try {
                first = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
            List<Pending> batch = new ArrayList<>(Math.min(MAX_BATCH, 16));
            batch.add(first);
            queue.drainTo(batch, MAX_BATCH - 1);
            flush(batch);
        }
        // 关闭：把剩余任务刷完，避免「已 ack 但没落库」
        List<Pending> rest = new ArrayList<>();
        queue.drainTo(rest);
        if (!rest.isEmpty()) {
            flush(rest);
        }
    }

    /**
     * 一个 BATCH 会话提交整批。
     *
     * <p>顺序是「先 commit 再 complete future」：反过来会让调用方在事务提交前就回 ack，
     * 一旦提交失败就变成「客户端以为落库了、实际没有」——这正是本类要防的场景。
     */
    private void flush(List<Pending> batch) {
        if (batch.isEmpty()) {
            return;
        }
        try (SqlSession session = sqlSessionFactory.openSession(ExecutorType.BATCH, false)) {
            MessageMapper batchMapper = session.getMapper(MessageMapper.class);
            for (Pending pending : batch) {
                batchMapper.insert(MessageEntity.from(pending.message()));
            }
            session.commit();
            batch.forEach(pending -> pending.done().complete(null));
            if (batch.size() > 1) {
                log.debug("[storage] group commit 合并 {} 条消息", batch.size());
            }
        } catch (RuntimeException e) {
            log.warn("[storage] 批量落库失败（{} 条），整批交给客户端重试: {}", batch.size(), e.getMessage());
            batch.forEach(pending -> pending.done().completeExceptionally(e));
        }
    }

    @Override
    public void close() {
        running = false;
        flusher.interrupt();
        try {
            flusher.join(3_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
