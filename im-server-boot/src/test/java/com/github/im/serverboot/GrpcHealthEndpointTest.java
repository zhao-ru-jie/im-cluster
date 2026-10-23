package com.github.im.serverboot;

import io.grpc.ManagedChannel;
import io.grpc.health.v1.HealthCheckRequest;
import io.grpc.health.v1.HealthCheckResponse;
import io.grpc.health.v1.HealthGrpc;
import io.grpc.netty.NettyChannelBuilder;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * 验证 IM 服务真的把 {@code grpc.health.v1.Health} 暴露出来了。
 *
 * <p>为什么值得起一个真实上下文：服务端这一半完全由 Spring Boot 的
 * {@code GrpcServerHealthAutoConfiguration} 装配，它有一串隐式前提（classpath 上要有
 * {@code io.grpc:grpc-services}、要存在 {@code BindableService}、要有 Actuator 的
 * {@code HealthContributorRegistry}）。任何一条不满足，健康服务就**静默不生效**——
 * 网关那边照样能跑（对端没有健康服务时 gRPC 视为可用），但「排除未就绪实例」这个能力就悄悄丢了。
 * 用测试把它钉住，比在 yml 里写一行 enabled 更可靠。
 *
 * <p>状态是定时从 Actuator 健康聚合同步过来的（默认 5s，这里调小到 200ms），所以断言用轮询。
 */
@SpringBootTest(classes = ImServerApplication.class, properties = {
        "spring.grpc.server.port=19291",
        "im.node.grpc-port=19291",
        "server.port=0",
        "im.nacos.enabled=false",
        "spring.grpc.server.health.schedule.delay=100ms",
        "spring.grpc.server.health.schedule.period=200ms"
})
class GrpcHealthEndpointTest {

    private static final int GRPC_PORT = 19291;

    @Test
    @DisplayName("IM 服务暴露 grpc.health.v1.Health，整体状态最终为 SERVING")
    void exposesOverallHealth() throws InterruptedException {
        ManagedChannel channel = NettyChannelBuilder.forAddress("127.0.0.1", GRPC_PORT)
                .usePlaintext()
                .build();
        try {
            HealthCheckResponse.ServingStatus status = pollUntilServing(channel);
            assertEquals(HealthCheckResponse.ServingStatus.SERVING, status);
        } finally {
            channel.shutdownNow();
        }
    }

    private static HealthCheckResponse.ServingStatus pollUntilServing(ManagedChannel channel)
            throws InterruptedException {
        HealthGrpc.HealthBlockingStub stub = HealthGrpc.newBlockingStub(channel);
        HealthCheckResponse.ServingStatus last = null;
        for (int i = 0; i < 50; i++) {
            last = stub.withDeadlineAfter(1, TimeUnit.SECONDS)
                    .check(HealthCheckRequest.newBuilder().setService("").build())
                    .getStatus();
            if (last == HealthCheckResponse.ServingStatus.SERVING) {
                return last;
            }
            Thread.sleep(200L);
        }
        return fail("健康状态 10s 内没有变成 SERVING，最后状态=" + last);
    }
}
