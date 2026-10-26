package com.github.im.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

/**
 * 压测指标汇总：计数器全部用 {@link LongAdder}（分散热点，十万级连接同时自增也不打架），
 * 延迟用 {@link LatencyHistogram}。
 *
 * <p>指标分三类，缺一类就没法判断压测结论：
 * <ol>
 *   <li><b>能否连上</b>：建连成功/失败/被拒，失败原因分类（拒绝、超时、401/503）；</li>
 *   <li><b>能否处理</b>：上行条数、ack 成功/失败、失败码分布（400 不可重试、503 可退避）；</li>
 *   <li><b>处理多快</b>：ack RTT 分位数、下行 push 的端到端延迟分位数。</li>
 * </ol>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class BenchMetrics {

    private final LongAdder connectAttempted = new LongAdder();
    private final LongAdder connectOpened = new LongAdder();
    private final LongAdder connectFailed = new LongAdder();
    private final LongAdder connectionClosed = new LongAdder();
    private final LongAdder handshakeRejected = new LongAdder();
    private final LongAdder sent = new LongAdder();
    private final LongAdder ackOk = new LongAdder();
    private final LongAdder ackError = new LongAdder();
    private final LongAdder push = new LongAdder();
    private final LongAdder serverPing = new LongAdder();
    private final LongAdder serverPong = new LongAdder();
    private final LongAdder migrate = new LongAdder();
    private final LongAdder parseError = new LongAdder();
    private final LongAdder backpressure = new LongAdder();
    private final LongAdder inFlight = new LongAdder();
    private final LongAdder unknownFrame = new LongAdder();

    private final Map<Integer, LongAdder> errorCodes = new ConcurrentHashMap<>();
    private final Map<String, LongAdder> connectFailureReasons = new ConcurrentHashMap<>();
    private final Map<String, Boolean> announcedProblems = new ConcurrentHashMap<>();

    private final LatencyHistogram ackRtt = new LatencyHistogram();
    private final LatencyHistogram endToEnd = new LatencyHistogram();

    // ==== 建连 ====

    public void onConnectAttempt() {
        connectAttempted.increment();
    }

    public void onConnectOpened() {
        connectOpened.increment();
    }

    public void onConnectFailure(String reason) {
        connectFailed.increment();
        connectFailureReasons.computeIfAbsent(reason, key -> new LongAdder()).increment();
        if (reason.startsWith("WebSocketHandshakeException")) {
            handshakeRejected.increment();
        }
    }

    public void onConnectionClosed() {
        connectionClosed.increment();
    }

    // ==== 上行 / 下行 ====

    public void onSent() {
        sent.increment();
        inFlight.increment();
    }

    /** @param rttNanos 从发出到收到 ack 的耗时；{@code < 0} 表示这条 ack 无法配对 */
    public void onAck(long rttNanos) {
        ackOk.increment();
        releaseInFlight();
        if (rttNanos >= 0) {
            ackRtt.record(rttNanos);
        }
    }

    public void onAckError(int code) {
        ackError.increment();
        releaseInFlight();
        errorCodes.computeIfAbsent(code, key -> new LongAdder()).increment();
    }

    /** 服务端可能在客户端没发消息时也下 error（比如迁移/超时），在途数不能因此变成负数。 */
    private void releaseInFlight() {
        if (inFlight.sum() > 0) {
            inFlight.decrement();
        }
    }

    /** @param e2eNanos 发送端写入消息体到接收端收到 push 的耗时；{@code < 0} 表示无法计算 */
    public void onPush(long e2eNanos) {
        push.increment();
        if (e2eNanos >= 0) {
            endToEnd.record(e2eNanos);
        }
    }

    public void onServerPing() {
        serverPing.increment();
    }

    public void onServerPong() {
        serverPong.increment();
    }

    /** 收到 {@code session.migrate}：服务端在缩容/发版/摘流，要求客户端换实例重连。 */
    public void onMigrate() {
        migrate.increment();
    }

    public void onParseError() {
        parseError.increment();
    }

    /** 单连接未 ack 达到上限时跳过发送：说明服务端受理速度跟不上客户端的发压速度。 */
    public void onBackpressure() {
        backpressure.increment();
    }

    public void onUnknownFrame() {
        unknownFrame.increment();
    }

    /**
     * 只打印一次的问题（例如一开压就 Connection refused，一万个连接会刷一万行同样的话）。
     * 压测端最怕的不是报错，是错误把自己刷到看不见。
     */
    public void announceOnce(String problem) {
        if (announcedProblems.putIfAbsent(problem, Boolean.TRUE) == null) {
            System.err.println("[压测端] " + problem);
        }
    }

    // ==== 读取 ====

    public long connectAttempted() {
        return connectAttempted.sum();
    }

    public long connectOpened() {
        return connectOpened.sum();
    }

    public long connectFailed() {
        return connectFailed.sum();
    }

    public long connectionClosed() {
        return connectionClosed.sum();
    }

    public long handshakeRejected() {
        return handshakeRejected.sum();
    }

    public long sent() {
        return sent.sum();
    }

    public long ackOk() {
        return ackOk.sum();
    }

    public long ackError() {
        return ackError.sum();
    }

    public long push() {
        return push.sum();
    }

    public long serverPing() {
        return serverPing.sum();
    }

    public long serverPong() {
        return serverPong.sum();
    }

    public long migrate() {
        return migrate.sum();
    }

    public long parseError() {
        return parseError.sum();
    }

    public long backpressure() {
        return backpressure.sum();
    }

    public long inFlight() {
        return inFlight.sum();
    }

    public long unknownFrame() {
        return unknownFrame.sum();
    }

    public LatencyHistogram.Snapshot ackRttSnapshot() {
        return ackRtt.snapshot();
    }

    public LatencyHistogram.Snapshot endToEndSnapshot() {
        return endToEnd.snapshot();
    }

    /** 过程报告的窗口快照：用两次快照相减算速率，避免累计值把「当前已经打不动」掩盖掉。 */
    public Window window() {
        return new Window(sent.sum(), ackOk.sum(), ackError.sum(), push.sum(), inFlight.sum());
    }

    public record Window(long sent, long ackOk, long ackError, long push, long inFlight) {
    }

    // ==== 输出 ====

    public void printSummary(BenchmarkOptions options, double elapsedSeconds) {
        LatencyHistogram.Snapshot rtt = ackRtt.snapshot();
        LatencyHistogram.Snapshot e2e = endToEnd.snapshot();
        long totalAck = ackOk.sum() + ackError.sum();

        System.out.println();
        System.out.println("==================== 压测结果 ====================");
        System.out.printf("目标            %s://%s:%d%s%n", options.tls() ? "wss" : "ws",
                options.host(), options.port(), options.path());
        System.out.printf("参数            连接=%d 每连接发送=%d/s 单连接在途上限=%d 消息体=%dB 目标=%s%n",
                options.connections(), options.sendRatePerSec(), options.maxInFlight(),
                options.bodyBytes(), options.target());
        System.out.printf("压测时长        %.1fs%n", elapsedSeconds);

        System.out.println("---- 连接 ----");
        System.out.printf("建连            尝试 %s / 成功 %s / 失败 %s / 期间断开 %s%n",
                num(connectAttempted.sum()), num(connectOpened.sum()),
                num(connectFailed.sum()), num(connectionClosed.sum()));
        if (handshakeRejected.sum() > 0) {
            System.out.printf("握手被拒        %s（401=token 无效、503=实例在摘流、404=路径不对）%n",
                    num(handshakeRejected.sum()));
        }
        printReasons(connectFailureReasons);

        System.out.println("---- 吞吐 ----");
        System.out.printf("上行 msg.send   %s 条（%.0f/s）%n", num(sent.sum()), perSecond(sent.sum(), elapsedSeconds));
        System.out.printf("下行 msg.ack    %s 条（成功 %s / 失败 %s）%n",
                num(totalAck), num(ackOk.sum()), num(ackError.sum()));
        System.out.printf("下行 msg.push   %s 条（%.0f/s）%n", num(push.sum()), perSecond(push.sum(), elapsedSeconds));
        System.out.printf("心跳            server ping %s / pong %s%n", num(serverPing.sum()), num(serverPong.sum()));
        if (migrate.sum() > 0) {
            System.out.printf("摘流通知        session.migrate %s 次（服务端在缩容或发版）%n", num(migrate.sum()));
        }
        if (backpressure.sum() > 0) {
            System.out.printf("背压跳过        %s 次（单连接在途已达 %d，服务端受理跟不上发压速度）%n",
                    num(backpressure.sum()), options.maxInFlight());
        }
        if (parseError.sum() > 0 || unknownFrame.sum() > 0) {
            System.out.printf("异常帧          解析失败 %s / 未知类型 %s%n",
                    num(parseError.sum()), num(unknownFrame.sum()));
        }
        printErrorCodes();

        System.out.println("---- 延迟 ----");
        System.out.println("RTT(msg.ack)      " + rtt.percentileLine());
        System.out.println("端到端(msg.push)  " + e2e.percentileLine());

        long lostAck = Math.max(0, sent.sum() - totalAck);
        if (lostAck > 0) {
            System.out.printf("未回 ack        %s 条（含压测结束时仍在途的 %s 条）%n",
                    num(lostAck), num(inFlight.sum()));
        }
        System.out.println("=================================================");
    }

    private void printReasons(Map<String, LongAdder> reasons) {
        if (reasons.isEmpty()) {
            return;
        }
        List<Map.Entry<String, LongAdder>> entries = new ArrayList<>(reasons.entrySet());
        entries.sort((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()));
        for (Map.Entry<String, LongAdder> entry : entries) {
            System.out.printf("  - %-40s %s%n", entry.getKey(), num(entry.getValue().sum()));
        }
    }

    private void printErrorCodes() {
        if (errorCodes.isEmpty()) {
            return;
        }
        List<Map.Entry<Integer, LongAdder>> entries = new ArrayList<>(errorCodes.entrySet());
        entries.sort((a, b) -> Long.compare(b.getValue().sum(), a.getValue().sum()));
        System.out.print("上行失败码      ");
        for (Map.Entry<Integer, LongAdder> entry : entries) {
            System.out.printf("%d=%s ", entry.getKey(), num(entry.getValue().sum()));
        }
        System.out.println();
    }

    static String num(long value) {
        return String.format("%,d", value);
    }

    static double perSecond(long total, double seconds) {
        return seconds <= 0 ? 0 : total / seconds;
    }
}
