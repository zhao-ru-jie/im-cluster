package com.github.im.server.grpc;

import com.github.im.proto.presence.v1.GetPresenceRequest;
import com.github.im.proto.presence.v1.GetPresenceResponse;
import com.github.im.proto.presence.v1.PresenceIngressGrpc;
import com.github.im.proto.presence.v1.PresenceReport;
import com.github.im.proto.presence.v1.PresenceReportAck;
import com.github.im.proto.presence.v1.TypingRequest;
import com.github.im.proto.presence.v1.TypingResponse;
import com.github.im.server.metrics.MessageMetrics;
import com.github.im.server.presence.PresenceService;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.grpc.server.service.GrpcService;

import java.util.function.Supplier;

/**
 * 状态上报（presence / typing）的 gRPC 入口。
 *
 * <p>与 {@link MessageIngressGrpcService} 同一套约定：
 * <ul>
 *   <li>依赖注入具体类型 {@link PresenceService} 而不是端口接口——服务端永远只把请求
 *       交给本地业务实现，单体装配下也不会因为同类型多 bean 而歧义；</li>
 *   <li>业务异常在这里就地捕获并映射状态码：不接的话网关只会看到一个 {@code UNKNOWN}，
 *       两端的日志里都不会留下业务堆栈。</li>
 * </ul>
 *
 * <p><b>typing / presence 的异常一律不升级成调用方的故障</b>：网关那侧是 fire-and-forget
 * （丢了不影响任何正确性），这里把「参数错」映射成 {@code INVALID_ARGUMENT} 只是为了让
 * 灰度期的新客户端能在网关日志里看到原因，而不是让人以为网关坏了。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@GrpcService
public class PresenceGrpcService extends PresenceIngressGrpc.PresenceIngressImplBase {

    private static final Logger log = LoggerFactory.getLogger(PresenceGrpcService.class);

    private final PresenceService presence;
    private final MessageMetrics metrics;

    /** 单测用：不接 registry */
    public PresenceGrpcService(PresenceService presence) {
        this(presence, new MessageMetrics(null));
    }

    @Autowired
    public PresenceGrpcService(PresenceService presence, MessageMetrics metrics) {
        this.presence = presence;
        this.metrics = metrics;
    }

    @Override
    public void reportTyping(TypingRequest request, StreamObserver<TypingResponse> responseObserver) {
        handle("reportTyping", request.getUserId(), responseObserver,
                () -> presence.reportTyping(request));
    }

    @Override
    public void getPresence(GetPresenceRequest request,
                            StreamObserver<GetPresenceResponse> responseObserver) {
        // 批量查询没有单一主体：用人数当 subject，日志里能看出「查了多少人」
        handle("getPresence", request.getUserIdsCount() + " users", responseObserver,
                () -> presence.presence(request));
    }

    @Override
    public void reportPresence(PresenceReport request,
                               StreamObserver<PresenceReportAck> responseObserver) {
        handle("reportPresence", request.getUserId(), responseObserver,
                () -> presence.reportPresence(request));
    }

    private <T> void handle(String rpc, String subject, StreamObserver<T> observer, Supplier<T> call) {
        long startedAt = System.nanoTime();
        try {
            T response = call.get();
            metrics.ingress(rpc, System.nanoTime() - startedAt, MessageMetrics.RPC_OK);
            observer.onNext(response);
            observer.onCompleted();
        } catch (IllegalArgumentException e) {
            metrics.ingress(rpc, System.nanoTime() - startedAt, MessageMetrics.RPC_INVALID);
            log.warn("[presence] {} 参数不合法 subject={} err={}", rpc, subject, e.getMessage());
            observer.onError(Status.INVALID_ARGUMENT
                    .withDescription(rpc + ": " + e.getMessage())
                    .asRuntimeException());
        } catch (RuntimeException e) {
            metrics.ingress(rpc, System.nanoTime() - startedAt, MessageMetrics.RPC_ERROR);
            log.error("[presence] {} 处理失败 subject={} err={}", rpc, subject, e.getMessage(), e);
            observer.onError(Status.INTERNAL
                    .withDescription(rpc + " failed: " + e.getMessage())
                    .asRuntimeException());
        }
    }
}
