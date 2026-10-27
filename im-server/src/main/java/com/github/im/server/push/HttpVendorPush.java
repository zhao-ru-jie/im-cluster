package com.github.im.server.push;

import com.github.im.contract.VendorPushPort;
import com.github.im.contract.VendorPushRequest;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * 厂商推送的 HTTP 中转实现：把离线通知 POST 给一个统一的中转地址。
 *
 * <h2>为什么是「中转」而不是直接对接 APNs / FCM</h2>
 *
 * <p>各家的协议与鉴权差别太大，不是同一个问题的不同写法：
 * <ul>
 *   <li>APNs：HTTP/2 + 每次连接用 ES256 签发的 JWT，且 token 有刷新节奏；</li>
 *   <li>FCM：OAuth2 服务账号，token 有效期一小时，需要提前刷新；</li>
 *   <li>国内厂商（华为/小米/OPPO/vivo）：各有一套 AppKey + AppSecret 签名，字段名都不一样。</li>
 * </ul>
 *
 * <p>把这四套实现塞进消息服务只有一种结果：消息服务的发布节奏被推送 SDK 的升级绑死，
 * 而且其中任何一家的连接抖动都能影响投递线程池。所以边界放在这里——
 * IM 服务只声明「谁、推什么」，由中转服务负责「怎么推」。
 *
 * <p>默认关闭（{@code im.vendor-push.enabled=false}）：没有中转服务时不该有任何网络调用，
 * 也不该在启动日志里出现一个「连不上」的告警。
 *
 * <h2>失败只记日志</h2>
 *
 * <p>限流、token 过期、目标设备注销都是厂商侧的常态。通知丢了，用户下次打开 App
 * 依然能从离线队列拿到消息（那才是消息的权威通道）；为一条通知去做重试与积压，
 * 只会换来一个需要自己运维的队列。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
@ConditionalOnProperty(name = "im.vendor-push.enabled", havingValue = "true")
public class HttpVendorPush implements VendorPushPort {

    private static final Logger log = LoggerFactory.getLogger(HttpVendorPush.class);

    private final ObjectMapper objectMapper;
    private final HttpClient client;
    private final String endpoint;
    private final String token;
    private final Duration timeout;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public HttpVendorPush(ObjectMapper objectMapper,
                          @Value("${im.vendor-push.endpoint:}") String endpoint,
                          @Value("${im.vendor-push.token:}") String token,
                          @Value("${im.vendor-push.timeout:3s}") Duration timeout) {
        this.objectMapper = objectMapper;
        this.endpoint = endpoint;
        this.token = token;
        this.timeout = timeout;
        this.client = HttpClient.newBuilder()
                .connectTimeout(timeout)
                .build();
        if (endpoint == null || endpoint.isBlank()) {
            log.warn("[vendor-push] 已开启但未配置 im.vendor-push.endpoint，通知将直接丢弃");
        } else {
            log.info("[vendor-push] 厂商推送中转就绪：{}（超时 {}）", endpoint, timeout);
        }
    }

    @Override
    public void push(VendorPushRequest request) {
        if (request == null || request.userId() == null || endpoint == null || endpoint.isBlank()) {
            return;
        }
        // 异步：调用点在投递路径上，同步等第三方 HTTP 会吃掉下行推送的并发许可
        executor.execute(() -> send(request));
    }

    private void send(VendorPushRequest request) {
        try {
            Map<String, Object> payload = new LinkedHashMap<>();
            payload.put("userId", request.userId());
            payload.put("title", request.title());
            payload.put("body", request.body());
            payload.put("conversationId", request.conversationId());
            payload.put("seq", request.seq());
            payload.put("contentType", request.contentType());

            HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(endpoint))
                    .timeout(timeout)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(objectMapper.writeValueAsString(payload)));
            if (token != null && !token.isBlank()) {
                builder.header("Authorization", "Bearer " + token);
            }
            HttpResponse<Void> response = client.send(builder.build(), HttpResponse.BodyHandlers.discarding());
            if (response.statusCode() >= 300) {
                log.warn("[vendor-push] 中转返回 {} user={} conv={}",
                        response.statusCode(), request.userId(), request.conversationId());
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            // 通知丢失不影响消息本身：离线队列才是权威通道
            log.debug("[vendor-push] 推送失败 user={} conv={} err={}",
                    request.userId(), request.conversationId(), e.getMessage());
        }
    }

    @PreDestroy
    public void shutdown() {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(2, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }
}
