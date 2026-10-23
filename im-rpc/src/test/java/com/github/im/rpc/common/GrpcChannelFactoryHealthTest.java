package com.github.im.rpc.common;

import com.github.im.proto.ingress.v1.MessageIngressGrpc;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.rpc.ingress.RemoteMessageIngress;
import io.grpc.Attributes;
import io.grpc.BindableService;
import io.grpc.EquivalentAddressGroup;
import io.grpc.ManagedChannel;
import io.grpc.NameResolver;
import io.grpc.NameResolverProvider;
import io.grpc.NameResolverRegistry;
import io.grpc.Server;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.HealthStatusManager;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 健康检查的端到端验证：<b>连得上但不健康的实例不参与 round_robin 轮询</b>。
 *
 * <p>为什么不 mock：要验证的正是 gRPC 内部「健康状态 → subchannel 状态 → picker 是否包含它」这条链路，
 * 用 mock 只能验证「我们往 serviceConfig 里塞了 healthCheckConfig」，等于没测。所以这里起真实的
 * Netty gRPC 服务端（与生产同一套 {@code HealthStatusManager}），让客户端走真实的
 * {@link GrpcChannelFactory}。
 *
 * <p>地址来自一个测试专用的 {@code healthtest://} scheme：生产用 {@code ingress://}（Nacos 订阅）
 * 提供多地址，这里用固定两地址，其余（round_robin、健康检查、picker）完全一致。
 */
class GrpcChannelFactoryHealthTest {

    private static final String SCHEME = "healthtest";
    private static final String TARGET = SCHEME + ":///both";

    /** 各服务端返回的 seq 标记，用来判断这次调用落到了哪个实例 */
    private static final long MARKER_SERVING = 1L;
    private static final long MARKER_NOT_SERVING = 2L;

    private static final NameResolverRegistry REGISTRY = NameResolverRegistry.getDefaultRegistry();
    private static StaticResolverProvider resolverProvider;

    private final List<Server> servers = new ArrayList<>();
    private final List<GrpcChannelFactory> factories = new ArrayList<>();

    @BeforeAll
    static void registerScheme() {
        resolverProvider = new StaticResolverProvider();
        REGISTRY.register(resolverProvider);
    }

    @AfterAll
    static void deregisterScheme() {
        REGISTRY.deregister(resolverProvider);
    }

    @AfterEach
    void shutdown() throws InterruptedException {
        factories.forEach(GrpcChannelFactory::close);
        factories.clear();
        for (Server server : servers) {
            server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
        }
        servers.clear();
    }

    @Test
    @DisplayName("health=SERVING 与 health=NOT_SERVING 各一台：所有请求只落到健康的那台")
    void notServingInstanceIsExcludedFromRoundRobin() throws IOException {
        MarkerService serving = new MarkerService(MARKER_SERVING);
        MarkerService notServing = new MarkerService(MARKER_NOT_SERVING);
        HealthStatusManager servingHealth = new HealthStatusManager();
        HealthStatusManager notServingHealth = new HealthStatusManager();

        int servingPort = start(serving, servingHealth);
        int notServingPort = start(notServing, notServingHealth);
        servingHealth.setStatus("", ServingStatus.SERVING);
        notServingHealth.setStatus("", ServingStatus.NOT_SERVING);

        useAddresses(servingPort, notServingPort);
        RemoteMessageIngress ingress = ingress(true, 2_000L);

        // 健康状态是异步订阅的（Watch 流建立 → 首帧状态 → picker 重算）。要求「连续 3 次都落到
        // 健康实例」才算稳定，避免偶发的一次命中就当成已生效。
        awaitTrue(() -> allRespond(ingress, MARKER_SERVING, 3), 5_000L, "健康实例始终没被稳定选中");
        int notServingCallsBefore = notServing.calls.get();

        for (int i = 0; i < 20; i++) {
            assertEquals(MARKER_SERVING, ingress.submit(request()).getSeq(),
                    "第 " + (i + 1) + " 次请求落到了 NOT_SERVING 的实例上");
        }
        assertEquals(notServingCallsBefore, notServing.calls.get(),
                "NOT_SERVING 的实例被排除后仍在接收流量");

        // 证明它一直是「可达的」：把状态翻成 SERVING，流量立刻回到两台（否则上面的断言可能是
        // 因为「这台其实连不上」而通过，那就是个假阳性）
        notServingHealth.setStatus("", ServingStatus.SERVING);
        awaitTrue(() -> {
            for (int i = 0; i < 10; i++) {
                ingress.submit(request());
            }
            return notServing.calls.get() > notServingCallsBefore;
        }, 5_000L, "翻成 SERVING 后流量没有回到该实例，说明之前被排除的原因不是健康状态");
    }

    @Test
    @DisplayName("对端没有 grpc.health.v1.Health 时不算不健康：请求照常成功（滚动升级没有硬顺序要求）")
    void missingHealthServiceIsTreatedAsUsable() throws IOException {
        MarkerService plain = new MarkerService(MARKER_SERVING);
        int port = start(plain, null);
        useAddresses(port);

        // 这是实测结论，也是这个用例存在的意义：Watch 返回 UNIMPLEMENTED 时 grpc-java
        // 不会把 subchannel 摘掉（原以为会），所以「新版客户端 + 老版服务端」的滚动升级是安全的。
        // 注意这只影响可用性，不影响正确性——老服务端永远不会有 NOT_SERVING 状态可订阅。
        RemoteMessageIngress ingress = ingress(true, 1_000L);

        assertEquals(MARKER_SERVING, ingress.submit(request()).getSeq());
        assertTrue(plain.calls.get() > 0, "未实现健康服务的实例应该照常收到请求");
    }

    @Test
    @DisplayName("waitForReady：没有可达实例时排队等到 deadline，而不是立刻 UNAVAILABLE")
    void waitForReadyWaitsUntilDeadlineWhenNoInstanceIsReachable() throws IOException {
        // 占一个端口再释放，得到一个「没人监听」的地址（连接被拒 = 没有 READY 的 subchannel）
        int deadPort;
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            deadPort = socket.getLocalPort();
        }
        useAddresses(deadPort);

        RemoteMessageIngress fastFail = ingress(false, 1_000L);
        StatusRuntimeException immediate = assertThrows(StatusRuntimeException.class,
                () -> fastFail.submit(request()));
        assertEquals(Status.Code.UNAVAILABLE, immediate.getStatus().getCode(),
                "不等待时应快速失败，实际状态: " + immediate.getStatus());

        RemoteMessageIngress waiting = ingress(true, 400L);
        long start = System.nanoTime();
        StatusRuntimeException deadline = assertThrows(StatusRuntimeException.class,
                () -> waiting.submit(request()));
        long waitedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        assertEquals(Status.Code.DEADLINE_EXCEEDED, deadline.getStatus().getCode(),
                "waitForReady 应等到 deadline 结束，实际状态: " + deadline.getStatus());
        assertTrue(waitedMs >= 300, "waitForReady 没有真的等待，仅用了 " + waitedMs + "ms");
    }

    @Test
    @DisplayName("关闭健康检查时不做额外判定：NOT_SERVING 的实例照常参与轮询")
    void healthCheckDisabledKeepsLegacyBehavior() throws IOException {
        MarkerService notServing = new MarkerService(MARKER_NOT_SERVING);
        HealthStatusManager health = new HealthStatusManager();
        int port = start(notServing, health);
        health.setStatus("", ServingStatus.NOT_SERVING);
        useAddresses(port);

        RemoteMessageIngress ingress = ingress(false, 1_000L, false);

        assertEquals(MARKER_NOT_SERVING, ingress.submit(request()).getSeq(),
                "关掉健康检查后不应再过滤 NOT_SERVING 的实例");
    }

    // ==== 辅助：服务端 ====

    /** 记录调用次数并回一个固定 seq 标记的最小 MessageIngress 实现 */
    private static final class MarkerService extends MessageIngressGrpc.MessageIngressImplBase {

        private final long marker;
        private final AtomicInteger calls = new AtomicInteger();

        private MarkerService(long marker) {
            this.marker = marker;
        }

        @Override
        public void submitMessage(SubmitMessageRequest request,
                                  StreamObserver<SubmitMessageResponse> responseObserver) {
            calls.incrementAndGet();
            responseObserver.onNext(SubmitMessageResponse.newBuilder()
                    .setCode(0)
                    .setSeq(marker)
                    .setClientMsgId(request.getClientMsgId())
                    .build());
            responseObserver.onCompleted();
        }
    }

    /**
     * @param health {@code null} 表示这个服务端<b>没有</b>实现 grpc.health.v1.Health
     *               （模拟「老版本 IM 服务」）
     */
    private int start(BindableService service, HealthStatusManager health) throws IOException {
        NettyServerBuilder builder = NettyServerBuilder.forPort(0).addService(service);
        if (health != null) {
            builder.addService(health.getHealthService());
        }
        Server server = builder.build().start();
        servers.add(server);
        return server.getPort();
    }

    // ==== 辅助：客户端 ====

    private void useAddresses(int... ports) {
        List<EquivalentAddressGroup> addresses = new ArrayList<>(ports.length);
        for (int port : ports) {
            addresses.add(new EquivalentAddressGroup(new InetSocketAddress("127.0.0.1", port)));
        }
        resolverProvider.setAddresses(addresses);
    }

    private RemoteMessageIngress ingress(boolean waitForReady, long timeoutMs) {
        return ingress(waitForReady, timeoutMs, true);
    }

    private RemoteMessageIngress ingress(boolean waitForReady, long timeoutMs, boolean healthCheck) {
        GrpcChannelFactory factory = new GrpcChannelFactory(false, "", healthCheck);
        factories.add(factory);
        ManagedChannel channel = factory.channelFor(TARGET);
        channel.getState(true);
        return new RemoteMessageIngress(factory, TARGET, timeoutMs, waitForReady);
    }

    private static SubmitMessageRequest request() {
        return SubmitMessageRequest.newBuilder()
                .setFromUserId("alice")
                .setReceiverId("bob")
                .setPayload("hi")
                .setClientMsgId("c-" + System.nanoTime())
                .build();
    }

    /** 连续 {@code times} 次都落到指定实例 */
    private static boolean allRespond(RemoteMessageIngress ingress, long marker, int times) {
        for (int i = 0; i < times; i++) {
            try {
                if (ingress.submit(request()).getSeq() != marker) {
                    return false;
                }
            } catch (RuntimeException e) {
                return false;
            }
        }
        return true;
    }

    private static void awaitTrue(BooleanSupplier condition, long timeoutMs, String message) {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("等待被中断", e);
            }
        }
        throw new AssertionError(message + "（等待 " + timeoutMs + "ms 后仍不满足）");
    }

    // ==== 辅助：固定的多地址 NameResolver（生产由 ingress:// 订阅 Nacos 提供） ====

    private static final class StaticResolverProvider extends NameResolverProvider {

        private volatile List<EquivalentAddressGroup> addresses = List.of();

        void setAddresses(List<EquivalentAddressGroup> addresses) {
            this.addresses = addresses;
        }

        @Override
        public NameResolver newNameResolver(URI targetUri, NameResolver.Args args) {
            List<EquivalentAddressGroup> current = addresses;
            return new NameResolver() {
                @Override
                public String getServiceAuthority() {
                    return SCHEME;
                }

                @Override
                public void start(Listener2 listener) {
                    listener.onResult(ResolutionResult.newBuilder()
                            .setAddresses(current)
                            .setAttributes(Attributes.EMPTY)
                            .build());
                }

                @Override
                public void shutdown() {
                }
            };
        }

        @Override
        public String getDefaultScheme() {
            return SCHEME;
        }

        @Override
        protected boolean isAvailable() {
            return true;
        }

        @Override
        protected int priority() {
            // 高于内置的 dns（5），避免被别的 resolver 抢走
            return 10;
        }
    }
}
