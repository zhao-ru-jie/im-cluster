package com.github.im.server.grpc;

import com.github.im.proto.ingress.v1.PullPendingRequest;
import com.github.im.proto.ingress.v1.PullPendingResponse;
import com.github.im.proto.ingress.v1.SubmitMessageRequest;
import com.github.im.proto.ingress.v1.SubmitMessageResponse;
import com.github.im.server.message.MessageService;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 上行 gRPC 入口的错误语义。
 *
 * <p>这里的重点是<b>异常不能被 gRPC 兜底成 UNKNOWN</b>：不处理时业务堆栈只留在框架内部，
 * 网关侧只有一句「UNKNOWN」，两端都查不到原因（分离部署下排查过一轮才定位到）。
 * 所以三类结果必须分开：
 * <ul>
 *   <li>正常 → {@code onNext} + {@code onCompleted}</li>
 *   <li>参数错 → {@code INVALID_ARGUMENT}（调用方问题，不该盲目重试）</li>
 *   <li>服务端故障 → {@code INTERNAL}（带 description，便于网关日志直接看出来）</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
class MessageIngressGrpcServiceTest {

    private final MessageService messageIngress = mock(MessageService.class);
    private final MessageIngressGrpcService service = new MessageIngressGrpcService(messageIngress);

    @Test
    @DisplayName("正常提交：回一次 onNext 后 onCompleted")
    void submitReturnsResponse() {
        SubmitMessageResponse expected = SubmitMessageResponse.newBuilder().setSeq(7L).build();
        when(messageIngress.submit(any())).thenReturn(expected);
        RecordingObserver<SubmitMessageResponse> observer = new RecordingObserver<>();

        service.submitMessage(SubmitMessageRequest.newBuilder().setTraceId("t-1").build(), observer);

        assertEquals(1, observer.values.size(), "必须恰好回一帧");
        assertSame(expected, observer.values.get(0));
        assertTrue(observer.completed, "不回 onCompleted 客户端会一直等下一次响应");
        assertNull(observer.error);
    }

    @Test
    @DisplayName("参数不合法 → INVALID_ARGUMENT，而不是笼统的 UNKNOWN")
    void illegalArgumentBecomesInvalidArgument() {
        when(messageIngress.submit(any())).thenThrow(new IllegalArgumentException("body too long"));
        RecordingObserver<SubmitMessageResponse> observer = new RecordingObserver<>();

        service.submitMessage(SubmitMessageRequest.newBuilder().setTraceId("t-2").build(), observer);

        StatusRuntimeException error = observer.statusError();
        assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains("body too long"),
                "description 要带原始原因：" + error.getStatus().getDescription());
        assertTrue(observer.values.isEmpty(), "出错后不该再有 onNext");
    }

    @Test
    @DisplayName("服务端故障 → INTERNAL 且 description 带方法名与原因")
    void runtimeFailureBecomesInternal() {
        when(messageIngress.submit(any())).thenThrow(new IllegalStateException("db down"));
        RecordingObserver<SubmitMessageResponse> observer = new RecordingObserver<>();

        service.submitMessage(SubmitMessageRequest.newBuilder().setTraceId("t-3").build(), observer);

        StatusRuntimeException error = observer.statusError();
        assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().startsWith("submitMessage failed"),
                "description 要能看出是哪个 RPC 挂的：" + error.getStatus().getDescription());
        assertTrue(error.getStatus().getDescription().contains("db down"));
    }

    @Test
    @DisplayName("补发路径同样不会被兜底成 UNKNOWN")
    void pullPendingFailureIsMapped() {
        when(messageIngress.pullPending(any())).thenThrow(new IllegalStateException("redis timeout"));
        RecordingObserver<PullPendingResponse> observer = new RecordingObserver<>();

        service.pullPending(PullPendingRequest.newBuilder().setUserId("alice").build(), observer);

        StatusRuntimeException error = observer.statusError();
        assertEquals(Status.Code.INTERNAL, error.getStatus().getCode());
        assertTrue(error.getStatus().getDescription().contains("redis timeout"));
    }

    /** 收集式 observer：把 onNext / onCompleted / onError 的最终状态摊平成一个对象 */
    private static final class RecordingObserver<T> implements StreamObserver<T> {

        private final List<T> values = new ArrayList<>();
        private Throwable error;
        private boolean completed;

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable throwable) {
            this.error = throwable;
        }

        @Override
        public void onCompleted() {
            this.completed = true;
        }

        StatusRuntimeException statusError() {
            assertNotNull(error, "应当以 onError 结束");
            assertTrue(error instanceof StatusRuntimeException,
                    "onError 必须是 gRPC 状态异常，否则网关拿不到状态码：" + error.getClass());
            return (StatusRuntimeException) error;
        }
    }
}
