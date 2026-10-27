package com.github.im.server.shard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.zip.CRC32;

/**
 * 会话维度的分片路由：<b>{@code conversationId} 是唯一的分片键</b>。
 *
 * <h2>为什么现在是「只有函数，没有分片」</h2>
 *
 * <p>当前所有部署都是 {@code im.shard.count=1}，`shardOf` 恒返回 0，行为与不分片完全一致。
 * 先把它落地成<b>一个集中的、可测的、稳定的函数</b>，是因为分片这件事一旦要加，
 * 最先出错的永远是「同一份数据在两处被算到不同分片」——
 * 而各个模块各自写 {@code Math.abs(id.hashCode()) % n} 的那种实现，几乎必然会出现。
 *
 * <p>切分函数的价值在于约束：
 * <ul>
 *   <li><b>稳定</b>：同一 conversationId 永远落在同一分片（换实例、重启、扩缩容后仍是）；</li>
 *   <li><b>跨语言可复现</b>：用 CRC32（{@code java.util.zip.CRC32} 与 {@code zlib.crc32} 一致），
 *       方便运维脚本/离线任务算出同一个分片；</li>
 *   <li><b>单点定义</b>：改分片数只改配置，不散落在各模块。</li>
 * </ul>
 *
 * <h2>为什么以 conversationId 而不是 userId</h2>
 *
 * <p>消息的读写全部是「按会话 + seq」：seq 发号、历史范围扫、幂等键、撤回集合都是这个形态
 * （{@code im_message} 的唯一键与主查询也是 {@code (conversation_id, seq)} 打头）。
 * 以 userId 分片会让「一个群里 N 个人的消息」分散到 N 个分片，群聊扇出变成跨分片查询。
 *
 * <h2>真正的分片在哪一层</h2>
 *
 * <p>Redis 侧已经落地：所有会话 key 共用 {@code {conversationId}} hash tag
 * （见 {@code RedisKeys}），Cluster 的 slot 天然就是按会话分的。
 * MySQL 侧的分区脚本见 {@code db/schema-shard.sql}（按 {@code HASH(conversation_id)} 分区），
 * 默认不启用：单机部署没有收益，而分区表对主键有额外约束。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class ShardRouter {

    private static final Logger log = LoggerFactory.getLogger(ShardRouter.class);

    /** 未分片时的物理表名 */
    public static final String MESSAGE_TABLE = "im_message";

    private final int shardCount;

    public ShardRouter(@Value("${im.shard.count:1}") int shardCount) {
        this.shardCount = Math.max(shardCount, 1);
        log.info("[shard] 会话分片数 = {}{}", this.shardCount,
                this.shardCount == 1 ? "（未分片）" : "，消息表 " + MESSAGE_TABLE + "_{0.." + (this.shardCount - 1) + "}");
    }

    public int shardCount() {
        return shardCount;
    }

    /**
     * 会话所属分片。
     *
     * <p>空会话 ID 落到 0 号分片：这条路径只会在参数校验失效时出现，
     * 让它抛异常会把「一个坏请求」升级成「一次 500」，而路由本身没有正确性风险。
     */
    public int shardOf(String conversationId) {
        if (shardCount == 1 || conversationId == null || conversationId.isEmpty()) {
            return 0;
        }
        CRC32 crc = new CRC32();
        crc.update(conversationId.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return (int) Math.floorMod(crc.getValue(), shardCount);
    }

    /**
     * 消息表名。
     *
     * <p>单分片时返回 {@link #MESSAGE_TABLE} 本身——这就是「零分片也先有路由」的落点：
     * 分片数改成 N 之后，调用方不需要任何改动就能拿到 {@code im_message_3}。
     */
    public String messageTableOf(String conversationId) {
        return shardCount == 1 ? MESSAGE_TABLE : MESSAGE_TABLE + '_' + shardOf(conversationId);
    }
}
