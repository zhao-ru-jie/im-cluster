package com.github.im.benchmark;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.nio.NioIoHandler;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * 压测端入口，按「建连 → 加压 → 收尾 → 汇总」四个阶段编排。
 *
 * <p>为什么分阶段而不是「一边连一边发」：连接建立阶段和稳定压测阶段的负载特征完全不同，
 * 混在一起测出来的 P99 全是建连尖刺。这里先把连接铺满（可按速率限速），
 * 再统一打开发送开关，得到的延迟分位数才是稳态值。
 *
 * <p>另外压测本身是「用一台机器模拟很多客户端」，所以：
 * <ul>
 *   <li>不引入 Spring / 日志框架，减少压测机自身的开销和干扰；</li>
 *   <li>连接数、速率、时长全部可配，便于做阶梯加压；</li>
 *   <li>结束时主动发 close 帧，让服务端连接数指标回落，不影响下一轮压测。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class BenchmarkRunner {

    /** 建连阶段的兜底超时：限速建连 + 服务端排队，正常不会触到 */
    private static final long CONNECT_PHASE_TIMEOUT_SECONDS = 60;

    private BenchmarkRunner() {
    }

    public static void main(String[] args) throws Exception {
        BenchmarkOptions options = BenchmarkOptions.parse(args);
        if (options == null) {
            System.out.println(BenchmarkOptions.USAGE);
            return;
        }
        run(options);
    }

    private static void run(BenchmarkOptions options) throws Exception {
        BenchMetrics metrics = new BenchMetrics();
        Frames frames = new Frames();
        AtomicBoolean trafficEnabled = new AtomicBoolean();
        // 只升不降：加压一旦开始，过程报告就固定用压测口径（即使压测已结束、正在收尾）
        AtomicBoolean trafficStarted = new AtomicBoolean();
        // 当前阶段的起点：建连阶段用它算相对时间，进入加压阶段后换成加压起点，
        // 这样过程中打印的 [xx.xs] 就是「加压开始后的第几秒」，和压测报告的时长口径一致。
        AtomicLong phaseStartNanos = new AtomicLong(System.nanoTime());
        AtomicLong measuredMillis = new AtomicLong();
        AtomicBoolean summaryPrinted = new AtomicBoolean();

        printHeader(options);

        SslContext sslContext = options.tls()
                ? SslContextBuilder.forClient().trustManager(InsecureTrustManagerFactory.INSTANCE).build()
                : null;

        MultiThreadIoEventLoopGroup group =
                new MultiThreadIoEventLoopGroup(options.resolvedIoThreads(), NioIoHandler.newFactory());
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, (int) TimeUnit.SECONDS.toMillis(10));

        List<VirtualClient> clients = new ArrayList<>(options.connections());
        AtomicBoolean reporterStop = new AtomicBoolean();

        Thread reporter = Thread.ofPlatform().daemon().name("bench-report")
                .start(() -> reportLoop(metrics, reporterStop, trafficStarted, phaseStartNanos,
                        options.reportIntervalMs()));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            // Ctrl+C 提前结束：仍然要把已经跑出来的数据打出来，否则一轮压测白做
            trafficEnabled.set(false);
            reporterStop.set(true);
            printSummaryOnce(metrics, options, phaseStartNanos, measuredMillis, summaryPrinted);
            closeAll(clients);
            group.shutdownGracefully(0, 1, TimeUnit.SECONDS);
        }, "bench-shutdown"));

        // ==== 阶段 1：建连 ====
        long connectPhaseStart = System.nanoTime();
        for (int i = 0; i < options.connections(); i++) {
            paceConnect(i, options, connectPhaseStart);
            metrics.onConnectAttempt();
            VirtualClient client = new VirtualClient(i, options, metrics, frames, trafficEnabled);
            clients.add(client);
            bootstrap.clone().handler(client.initializer(sslContext))
                    .connect(options.host(), options.port())
                    .addListener(future -> {
                        if (!future.isSuccess()) {
                            // 连接被拒/超时：handler 还没进 pipeline，失败只能在这里记
                            client.onConnectFailed(future.cause());
                        }
                    });
        }
        long connectMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - connectPhaseStart);
        awaitConnections(metrics, options);
        System.out.printf("[建连完成] 尝试 %s 条，用时 %.1fs；成功 %s / 失败 %s%n",
                BenchMetrics.num(metrics.connectAttempted()), connectMillis / 1000.0,
                BenchMetrics.num(metrics.connectOpened()), BenchMetrics.num(metrics.connectFailed()));

        // ==== 阶段 2：加压 ====
        long trafficStart = System.nanoTime();
        phaseStartNanos.set(trafficStart);
        trafficStarted.set(true);
        trafficEnabled.set(true);
        long durationMillis = options.duration().toMillis();
        sleepUntil(trafficStart + TimeUnit.MILLISECONDS.toNanos(durationMillis));
        trafficEnabled.set(false);
        measuredMillis.set(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - trafficStart));

        // 收尾等待：让最后一批在途消息的 ack 回来，否则「未回 ack」会被自己算成丢消息
        Thread.sleep(Math.min(2_000, Math.max(200, options.reportIntervalMs())));

        // ==== 阶段 3：收尾 ====
        reporterStop.set(true);
        reporter.interrupt();
        closeAll(clients);
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).await(5, TimeUnit.SECONDS);

        // ==== 阶段 4：汇总 ====
        printSummaryOnce(metrics, options, phaseStartNanos, measuredMillis, summaryPrinted);
    }

    private static void printHeader(BenchmarkOptions options) {
        System.out.printf("压测目标        %s://%s:%d%s%n",
                options.tls() ? "wss" : "ws", options.host(), options.port(), options.path());
        System.out.printf("压测参数        连接=%d 建连速率=%s 每连接发送=%d/s 单连接在途上限=%d 消息体=%dB%n",
                options.connections(),
                options.connectRatePerSec() <= 0 ? "不限" : options.connectRatePerSec() + "/s",
                options.sendRatePerSec(), options.maxInFlight(), options.bodyBytes());
        System.out.printf("时长            %.0fs 目标=%s eventLoop=%d 线程%n",
                options.duration().toMillis() / 1000.0, options.target(), options.resolvedIoThreads());
        System.out.println("提示：网关侧可同时观察 /actuator/prometheus 的 gateway_connections_active 与 /admin/status");
        System.out.println();
    }

    /**
     * 限速建连：把第 i 条连接的发起时刻摊到 {@code i / rate} 秒上。
     *
     * <p>不限速时十万连接会在几十毫秒里全部 SYN 出去，服务端 accept 队列一溢出，
     * 测出来的「建连失败率」测的其实是内核 backlog，而不是网关能力。
     */
    private static void paceConnect(int index, BenchmarkOptions options, long phaseStartNanos) {
        int rate = options.connectRatePerSec();
        if (rate <= 0 || index == 0) {
            return;
        }
        long dueNanos = phaseStartNanos + (long) index * 1_000_000_000L / rate;
        long now;
        while ((now = System.nanoTime()) < dueNanos) {
            LockSupport.parkNanos(Math.min(dueNanos - now, TimeUnit.MILLISECONDS.toNanos(5)));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }

    /** 等建连阶段收敛：成功的进 pipeline，失败的会在回调里记账，两者之和到齐即可开始加压。 */
    private static void awaitConnections(BenchMetrics metrics, BenchmarkOptions options) throws InterruptedException {
        long deadline = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(CONNECT_PHASE_TIMEOUT_SECONDS)
                + TimeUnit.SECONDS.toNanos(options.connections() / Math.max(1, options.connectRatePerSec()));
        while (System.nanoTime() < deadline) {
            if (metrics.connectOpened() + metrics.connectFailed() >= options.connections()) {
                return;
            }
            Thread.sleep(50);
        }
        System.out.printf("[建连收敛超时] 仍在等待 %s 条连接返回结果%n",
                BenchMetrics.num(options.connections() - metrics.connectOpened() - metrics.connectFailed()));
    }

    private static void reportLoop(BenchMetrics metrics, AtomicBoolean stop, AtomicBoolean trafficStarted,
                                   AtomicLong phaseStartNanos, int intervalMillis) {
        BenchMetrics.Window previous = metrics.window();
        long previousNanos = System.nanoTime();
        while (!stop.get()) {
            try {
                Thread.sleep(intervalMillis);
            } catch (InterruptedException e) {
                return;
            }
            long now = System.nanoTime();
            BenchMetrics.Window current = metrics.window();
            double seconds = Math.max(0.001, (now - previousNanos) / 1_000_000_000.0);
            double elapsedSeconds = (now - phaseStartNanos.get()) / 1_000_000_000.0;
            if (!trafficStarted.get()) {
                // 建连阶段用另一套口径：这时还没开始发消息，报吞吐只会是一串 0
                System.out.printf("[建连 %5.1fs] 尝试 %s | 成功 %s | 失败 %s%n",
                        elapsedSeconds,
                        BenchMetrics.num(metrics.connectAttempted()),
                        BenchMetrics.num(metrics.connectOpened()),
                        BenchMetrics.num(metrics.connectFailed()));
            } else {
                System.out.printf("[%6.1fs] 连接 %s/%s | 发送 %s/s 累计 %s | ack %s/s 失败 %s | push %s/s | 在途 %s | RTT %s%n",
                        elapsedSeconds,
                        BenchMetrics.num(metrics.connectOpened()),
                        BenchMetrics.num(metrics.connectFailed()),
                        BenchMetrics.num(delta(current.sent(), previous.sent(), seconds)),
                        BenchMetrics.num(current.sent()),
                        BenchMetrics.num(delta(current.ackOk(), previous.ackOk(), seconds)),
                        BenchMetrics.num(current.ackError()),
                        BenchMetrics.num(delta(current.push(), previous.push(), seconds)),
                        BenchMetrics.num(current.inFlight()),
                        metrics.ackRttSnapshot().shortLine());
            }
            previous = current;
            previousNanos = now;
        }
    }

    private static long delta(long current, long previous, double seconds) {
        return Math.max(0, Math.round((current - previous) / seconds));
    }

    private static void closeAll(List<VirtualClient> clients) {
        for (VirtualClient client : clients) {
            client.close();
        }
    }

    private static void printSummaryOnce(BenchMetrics metrics, BenchmarkOptions options,
                                         AtomicLong phaseStartNanos, AtomicLong measuredMillis,
                                         AtomicBoolean printed) {
        if (!printed.compareAndSet(false, true)) {
            return;
        }
        long elapsedMillis = measuredMillis.get() > 0
                ? measuredMillis.get()
                : Math.max(1, (System.nanoTime() - phaseStartNanos.get()) / 1_000_000L);
        metrics.printSummary(options, elapsedMillis / 1000.0);
    }

    private static void sleepUntil(long targetNanos) {
        long now;
        while ((now = System.nanoTime()) < targetNanos) {
            LockSupport.parkNanos(Math.min(targetNanos - now, TimeUnit.MILLISECONDS.toNanos(50)));
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
        }
    }
}
