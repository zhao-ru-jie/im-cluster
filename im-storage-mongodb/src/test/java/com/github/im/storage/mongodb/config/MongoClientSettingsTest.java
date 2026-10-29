package com.github.im.storage.mongodb.config;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 客户端设置的构造与启动期握手策略。
 *
 * <p>这些参数是「Mongo 相关故障」里唯一能靠读代码定性的部分：报错只告诉你
 * 「等不到可用服务器」，至于它等了多久、恢复后多久能重新发现节点，全在这里。
 * 三条断言分别对应三个真实后果：
 *
 * <ul>
 *   <li><b>心跳间隔</b>决定抖动过后的恢复盲区（默认 10s 太长）；</li>
 *   <li><b>超时三件套</b>决定一次失败要占住调用线程多久（{@code socketTimeout=0} 会永久挂住）；</li>
 *   <li><b>握手重试</b>决定「mongod 抖一下」是「起不来」还是「多等一秒」。不连库即可测。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class MongoClientSettingsTest {

    private static final String DEFAULT_URI =
            "mongodb://127.0.0.1:27017/?connectTimeoutMS=3000&serverSelectionTimeoutMS=5000&socketTimeoutMS=30000";

    @Test
    @DisplayName("超时三件套按 uri 生效，心跳取默认 5s（比驱动的 10s 短一半恢复盲区）")
    void appliesTimeoutsFromUriAndShortensHeartbeat() {
        MongoClientSettings settings = MongoStorageConfig.buildSettings(DEFAULT_URI, 5_000);

        assertEquals(3_000, settings.getSocketSettings().getConnectTimeout(TimeUnit.MILLISECONDS));
        assertEquals(30_000, settings.getSocketSettings().getReadTimeout(TimeUnit.MILLISECONDS),
                "socketTimeout 必须是有界值：0 意味着存储假死时读线程永远等");
        assertEquals(5_000, settings.getClusterSettings().getServerSelectionTimeout(TimeUnit.MILLISECONDS));
        assertEquals(5_000, settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS),
                "心跳没被缩短的话，mongod 恢复后驱动最多还要 10s 才发现，这段盲区里请求照旧全失败");
    }

    @Test
    @DisplayName("uri 里显式写了 heartbeatFrequencyMS 时以 uri 为准（配置文件写了就得生效）")
    void uriHeartbeatWinsOverDefault() {
        MongoClientSettings settings = MongoStorageConfig.buildSettings(
                "mongodb://127.0.0.1:27017/?heartbeatFrequencyMS=8000", 5_000);

        assertEquals(8_000, settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS));
    }

    @Test
    @DisplayName("心跳传 0 表示不干预，交给 uri / 驱动默认")
    void zeroHeartbeatLeavesDriverDefault() {
        MongoClientSettings settings = MongoStorageConfig.buildSettings(DEFAULT_URI, 0);

        assertEquals(10_000, settings.getServerSettings().getHeartbeatFrequency(TimeUnit.MILLISECONDS),
                "驱动默认就是 10s；传 0 时不该被我们改成别的值");
    }

    @Test
    @DisplayName("启动期握手失败两次后成功：不再让整个进程起不来")
    void pingRetriesThroughTransientFailure() {
        MongoDatabase database = mock(MongoDatabase.class);
        when(database.runCommand(any(Document.class)))
                .thenThrow(new MongoTimeoutException("Timed out while waiting for a server"))
                .thenThrow(new MongoTimeoutException("Timed out while waiting for a server"))
                .thenReturn(new Document("ok", 1));
        MongoClient client = clientReturning(database);

        assertDoesNotThrow(() -> MongoStorageConfig.pingWithRetry(client, MongoStorageConfig.buildSettings(DEFAULT_URI, 5_000)));

        verify(database, times(3)).runCommand(any(Document.class));
    }

    @Test
    @DisplayName("重试用尽后抛错，且错误信息能直接指向真实原因")
    void pingGivesUpWithActionableMessage() {
        MongoDatabase database = mock(MongoDatabase.class);
        when(database.runCommand(any(Document.class)))
                .thenThrow(new MongoSocketReadTimeoutExceptionStub());
        MongoClient client = clientReturning(database);
        MongoClientSettings settings = MongoStorageConfig.buildSettings(DEFAULT_URI, 5_000);

        IllegalStateException error = assertThrows(IllegalStateException.class,
                () -> MongoStorageConfig.pingWithRetry(client, settings));

        assertTrue(error.getMessage().contains("127.0.0.1:27017"), error.getMessage());
        assertTrue(error.getMessage().contains("QuickEdit"),
                "Windows 上前台 mongod 被控制台选中是最常见的假死原因，报错里要直接点出来：" + error.getMessage());
        assertTrue(error.getMessage().contains("不是查询慢"), error.getMessage());
        assertTrue(error.getCause() != null, "原始异常要保留，否则看不到驱动的真实报文");
        verify(database, times(3)).runCommand(any(Document.class));
    }

    private static MongoClient clientReturning(MongoDatabase database) {
        MongoClient client = mock(MongoClient.class);
        when(client.getDatabase("admin")).thenReturn(database);
        return client;
    }

    /**
     * 读超时样本。
     *
     * <p>不用真实的 {@code MongoSocketReadTimeoutException}：它要 {@code ServerAddress} 等
     * 连接态参数才能构造，而这里要验的只是「任意运行时异常都会被重试、且原异常被保留」。
     */
    private static final class MongoSocketReadTimeoutExceptionStub extends RuntimeException {
        MongoSocketReadTimeoutExceptionStub() {
            super("Timeout while receiving message");
        }
    }
}
