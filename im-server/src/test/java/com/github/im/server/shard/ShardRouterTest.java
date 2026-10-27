package com.github.im.server.shard;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 会话分片路由的约定。
 *
 * <p>这些是「加维度」时最容易悄悄坏掉的性质：切分函数一改，历史数据就散在两处，
 * 而症状通常只是「某个会话的消息莫名其妙丢了一半」。所以把三条性质钉死：
 * <ol>
 *   <li><b>稳定</b>：同一 conversationId 永远同一分片；</li>
 *   <li><b>跨语言可复现</b>：CRC32 的取值与 {@code zlib.crc32} 一致（下面用硬编码值锁住）；</li>
 *   <li><b>单分片时完全退化</b>：表名就是 {@code im_message}、分片号恒 0，老部署零变化。</li>
 * </ol>
 */
class ShardRouterTest {

    @Test
    @DisplayName("单分片：恒 0，表名就是 im_message（未分片部署零变化）")
    void singleShardDegrades() {
        ShardRouter router = new ShardRouter(1);

        assertEquals(1, router.shardCount());
        assertEquals(0, router.shardOf("s:alice:bob"));
        assertEquals(ShardRouter.MESSAGE_TABLE, router.messageTableOf("s:alice:bob"));
    }

    @Test
    @DisplayName("CRC32 取值与 zlib.crc32 一致（离线脚本能算出同一个分片）")
    void matchesZlibCrc32() {
        // 基准值：zlib.crc32("s:alice:bob") = 3947514772，"g:sales" = 3678342791
        assertEquals(0, new ShardRouter(4).shardOf("s:alice:bob"));
        assertEquals(3, new ShardRouter(4).shardOf("g:sales"));
        assertEquals(3, new ShardRouter(4).shardOf("s:bob:carol"));
    }

    @Test
    @DisplayName("同一会话永远同一分片：扩缩容前后不能漂移")
    void stableForSameConversation() {
        ShardRouter router = new ShardRouter(16);
        for (int i = 0; i < 100; i++) {
            String conversationId = "s:user" + i + ":peer" + i;
            int first = router.shardOf(conversationId);
            assertEquals(first, router.shardOf(conversationId));
            assertEquals(ShardRouter.MESSAGE_TABLE + "_" + first, router.messageTableOf(conversationId));
        }
    }

    @Test
    @DisplayName("分布均匀：不能有哪个分片明显偏空（否则扩容等于白扩）")
    void distributesEvenly() {
        int shards = 8;
        ShardRouter router = new ShardRouter(shards);
        Map<Integer, Integer> counts = new HashMap<>();
        int total = 8_000;
        for (int i = 0; i < total; i++) {
            String conversationId = "s:" + UUID.randomUUID();
            counts.merge(router.shardOf(conversationId), 1, Integer::sum);
        }
        double expected = (double) total / shards;
        for (int shard = 0; shard < shards; shard++) {
            int count = counts.getOrDefault(shard, 0);
            // ±30% 足够宽，CRC32 在 8000 个样本上的偏差远小于这个量级
            assertTrue(count > expected * 0.7 && count < expected * 1.3,
                    "分片 " + shard + " 落了 " + count + " 个，期望约 " + expected);
        }
    }

    @Test
    @DisplayName("不同会话会落到不同分片（否则等于没分）")
    void spreadsAcrossShards() {
        ShardRouter router = new ShardRouter(4);
        assertNotEquals(router.shardOf("s:alice:bob"), router.shardOf("g:sales"));
    }

    @Test
    @DisplayName("分片数配 0 / 负数时退化成 1，而不是除零")
    void clampsInvalidShardCount() {
        assertEquals(1, new ShardRouter(0).shardCount());
        assertEquals(1, new ShardRouter(-3).shardCount());
        assertEquals(ShardRouter.MESSAGE_TABLE, new ShardRouter(0).messageTableOf("s:alice:bob"));
    }
}
