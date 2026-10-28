package com.github.im.storage.mongodb;

import com.github.im.server.message.StoredMessage;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * {@link MongoMessageRepository#before} 的翻页语义（<b>需要真实 MongoDB</b>，连不上自动跳过）。
 *
 * <p>为什么要单独一条：游标翻页是「用户往上滚」的唯一数据来源，而 MongoDB 这边的查询条件
 * （{@code conversationId + seq < 游标} 的倒序 + limit）一旦写成正序、或者忘了加
 * {@code seq} 上界，表现都是「消息对了但顺序反了 / 上滑一直重复同一页」——
 * 这两种错误都不会报错，只有翻过才知道。
 *
 * <p>用临时库（{@code im_cluster_it_*}），结束后删除，不碰业务库。
 */
class MongoMessageRepositoryPagingTest {

    private static final String URI = "mongodb://127.0.0.1:27017/?serverSelectionTimeoutMS=800&connectTimeoutMS=800";
    private static final String CONV = "s:alice:bob";

    @Test
    @DisplayName("before：seq 排他、升序返回、按会话隔离")
    void beforeSlicesBySeq() {
        assumeTrue(mongoReachable(), "本机没有 MongoDB（127.0.0.1:27017），跳过这条集成用例");

        String database = "im_cluster_it_" + System.nanoTime();
        try (MongoClient client = MongoClients.create(URI)) {
            MongoTemplate template = new MongoTemplate(client, database);
            MongoMessageRepository repository = new MongoMessageRepository(template);

            for (long seq = 1; seq <= 5; seq++) {
                repository.append(message(CONV, seq));
            }
            // 另一个会话：翻页不能串台
            repository.append(message("s:alice:carol", 99));

            assertEquals(List.of(4L, 5L), seqs(repository.before(CONV, 0L, 2)),
                    "beforeSeq=0 = 第一页，最新两条，且返回升序");
            assertEquals(List.of(2L, 3L), seqs(repository.before(CONV, 4L, 2)),
                    "seq < 4 的两条（游标排他）");
            assertEquals(List.of(1L), seqs(repository.before(CONV, 2L, 2)),
                    "到头了：返回条数少于 limit");
            assertTrue(repository.before(CONV, 1L, 2).isEmpty(), "最早一条之前什么都没有");
        } finally {
            try (MongoClient client = MongoClients.create(URI)) {
                client.getDatabase(database).drop();
            }
        }
    }

    private static StoredMessage message(String conversationId, long seq) {
        return new StoredMessage(conversationId, seq, "srv-" + conversationId + "-" + seq, "alice", "bob",
                "SINGLE", "c-" + conversationId + "-" + seq, "hi-" + seq, 1_750_000_000_000L + seq);
    }

    private static List<Long> seqs(List<StoredMessage> messages) {
        List<Long> seqs = new ArrayList<>(messages.size());
        for (StoredMessage message : messages) {
            seqs.add(message.seq());
        }
        return seqs;
    }

    private static boolean mongoReachable() {
        try (MongoClient client = MongoClients.create(URI)) {
            client.listDatabaseNames().first();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }
}
