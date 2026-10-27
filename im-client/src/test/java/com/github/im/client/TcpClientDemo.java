package com.github.im.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.Duration;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * TCP 长连接客户端的<b>手动测试程序</b>：连上就一直发、一直收，直到你让它停。
 *
 * <h2>干什么</h2>
 *
 * <ul>
 *   <li>连指定 {@code host:port}（TCP 承载，首帧 {@code connect} 认证）；</li>
 *   <li>连上后每 {@code --interval} 毫秒发一条消息给 {@code --to}，并把「发出的内容 / 收到的内容 / 受理回执」打到控制台；</li>
 *   <li>收到对方消息后自动回一个 {@code msg.read}（所以对端能看到已读）；</li>
 *   <li>断线由 SDK 自动按指数退避重连，本程序只负责打印「为什么断、多久重连」；</li>
 *   <li>三种停止方式：控制台输入 {@code q} 回车、{@code Ctrl+C}、{@code --run-seconds} 到点（后者用于冒烟）。</li>
 * </ul>
 *
 * <h2>怎么跑</h2>
 *
 * <p>IDEA：右键本类 → Run，然后在 Run 窗口（Program arguments）里填参数。命令行：
 *
 * <pre>{@code
 * # 1) 编译（顺带把 test 源码也编出来）
 * ./mvnw.cmd -q -pl im-client test-compile
 *
 * # 2) 拼出运行期 classpath（写到文件，避免手抄 jar 路径）
 * ./mvnw.cmd -q -pl im-client dependency:build-classpath "-Dmdep.outputFile=im-client/target/cp.txt"
 *
 * # 3) 跑（PowerShell）
 * $cp = "im-client/target/classes;im-client/target/test-classes;" + (Get-Content im-client/target/cp.txt)
 * java -cp $cp com.github.im.client.TcpClientDemo --port 7004 --user alice --to bob
 * }</pre>
 *
 * <p>常用参数（全部可选，见 {@link Options#printUsage}）：{@code --host --port --user --device --token
 * --to --interval --text --run-seconds}。
 *
 * <h2>两个容易踩的点</h2>
 *
 * <ol>
 *   <li><b>要发给人，对端得在线</b>：单聊消息发给不在线的用户，服务端照常受理（你会收到 ack），
 *       但没人接收。想看「收到」的效果，就开两个终端、各跑一个实例（{@code --user alice --to bob}
 *       和 {@code --user bob --to alice}）。</li>
 *   <li><b>同一 userId 会顶号</b>：默认 userId 是随机的（{@code demo-xxxx}），就是为了不把你正在调试的
 *       实例挤下线；要指定 {@code --user} 时自己确认没有同名连接在跑。</li>
 * </ol>
 *
 * <p>本类刻意<b>不含任何断言</b>，也不属于测试用例（名字不以 {@code Test} 结尾，surefire 不会捡它）：
 * 它是拿来对着真实服务观察行为的。要自动化的端到端用例见 {@code TcpSdkExampleTest}。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class TcpClientDemo {

    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private TcpClientDemo() {
    }

    public static void main(String[] args) throws Exception {
        Options options = Options.parse(args);
        if (options == null) {
            Options.printUsage();
            return;
        }

        Stats stats = new Stats();
        Printer printer = new Printer(stats);
        ImClientConfig config = ImClientConfig.builder()
                .host("127.0.0.1")
                .port(7004)
                .transport(ImClientConfig.Transport.TCP)
                .userId("xiaotian")
                .deviceId("device-1")
                .token(options.token())
                // 其余用 SDK 默认值：心跳 30s（网关 45s 才发 ping、90s 无上行才断）、
                // 重连退避 0.5s→30s、recv 攒批 100ms
                .build();
        ImClient client = new ImClient(config, printer);
        printer.bind(client);

        CountDownLatch stopped = new CountDownLatch(1);
        CountDownLatch finished = new CountDownLatch(1);
        AtomicReference<String> reason = new AtomicReference<>("unknown");

        // 停止方式 1：Ctrl+C / kill（JVM 关停钩子）。注意钩子里不要做耗时操作
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            reason.compareAndSet("unknown", "signal (Ctrl+C / kill)");
            stopped.countDown();
            try {
                // 钩子跑完 JVM 就 halt，不等一下的话下面的汇总可能来不及打印
                finished.await(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "demo-shutdown-hook"));

        // 停止方式 2：控制台输入 q / quit / exit / 空行
        Thread.ofVirtual().name("demo-stdin").start(() -> watchStdin(stopped, reason));

        // 停止方式 3（可选）：--run-seconds，到点自动停，便于脚本冒烟
        if (options.runSeconds() > 0) {
            Thread.ofVirtual().name("demo-timebox").start(() -> {
                try {
                    Thread.sleep(Duration.ofSeconds(options.runSeconds()));
                } catch (InterruptedException e) {
                    return;
                }
                reason.compareAndSet("unknown", "--run-seconds=" + options.runSeconds());
                stopped.countDown();
            });
        }

        System.out.printf("[BOOT] target=%s:%d user=%s device=%s to=%s interval=%dms token=%s%n",
                options.host(), options.port(), options.userId(), options.deviceId(),
                options.to(), options.intervalMs(), options.token() == null ? "(none)" : "(set)");
        System.out.println("[BOOT] stop: type \"q\" + Enter, or press Ctrl+C");

        client.connect();

        Thread sender = Thread.ofVirtual().name("demo-sender")
                .start(() -> sendLoop(client, options, stats, stopped));

        // 主线程只等停止信号：这样程序在「没人让它停」之前一直是运行状态
        stopped.await();
        sender.interrupt();

        System.out.printf("%n[STOP] reason=%s%n", reason.get());
        // 先打汇总再关：close() 之后本地表也关了
        printSummary(stats, client);
        client.close();
        System.out.println("[STOP] client closed, bye");
        finished.countDown();
    }

    // ==== 发送循环 ====

    /**
     * 每 {@code intervalMs} 发一条，直到收到停止信号。
     *
     * <p>用 {@code latch.await(interval)} 代替 {@code Thread.sleep}：停止信号一到就能立刻退出，
     * 不用等这一轮睡完。
     *
     * <p>没连上时<b>跳过</b>本轮而不是先写本地表：断线几分钟的情况下，后者会在发件箱里堆一堆
     * 重连后一次性重投的消息（SDK 支持这么做，但手动测试时刷屏很难看）。
     */
    private static void sendLoop(ImClient client, Options options, Stats stats, CountDownLatch stopped) {
        long index = 0;
        ImClient.State lastState = null;
        while (stopped.getCount() > 0) {
            try {
                ImClient.State state = client.state();
                if (state == ImClient.State.CONNECTED) {
                    index++;
                    String body = String.format("%s #%d %s", options.text(), index,
                            LocalTime.now().format(TIME));
                    long startNs = System.nanoTime();
                    String clientMsgId = client.sendText(options.to(), body);
                    stats.lastSendNs.set(startNs);
                    stats.sent.incrementAndGet();
                    System.out.printf("[SEND] #%d to=%s id=%s body=%s%n",
                            index, options.to(), shortId(clientMsgId), body);
                } else if (state != lastState) {
                    // 只在状态变化时打一次：断线一小时、每 500ms 刷一行 WAIT 会把真正的日志冲掉
                    System.out.printf("[WAIT] not connected (state=%s): pause sending, waiting for reconnect%n", state);
                }
                lastState = state;
                if (stopped.await(options.intervalMs(), TimeUnit.MILLISECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (RuntimeException e) {
                // 发送本身不抛（SDK 会先落表再上网），这里兜的是 closed 之后误调之类的情况
                System.out.println("[SEND] failed: " + e);
            }
        }
    }

    // ==== 控制台停止 ====

    private static void watchStdin(CountDownLatch stopped, AtomicReference<String> reason) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            String line;
            while ((line = reader.readLine()) != null) {
                // 去掉 BOM 和不可见控制字符：`echo q | java ...`、PowerShell 管道、
                // 以及某些终端会把行首带上 \uFEFF，不处理的话 "q" 会被认成 "ï»¿q"
                String cmd = line.replace("\uFEFF", "").strip().toLowerCase(Locale.ROOT);
                if (cmd.isEmpty() || cmd.equals("q") || cmd.equals("quit") || cmd.equals("exit")) {
                    reason.compareAndSet("unknown", "stdin: " + (cmd.isEmpty() ? "<Enter>" : cmd));
                    stopped.countDown();
                    return;
                }
                System.out.println("[CLI ] unknown command \"" + line.strip() + "\"; use q to stop");
            }
            // 读到 EOF：stdin 被重定向或控制台不可用（例如部分 IDE 的无控制台运行）
            System.out.println("[CLI ] stdin closed; stop with Ctrl+C");
        } catch (Exception e) {
            System.out.println("[CLI ] stdin unavailable (" + e + "); stop with Ctrl+C");
        }
    }

    // ==== 回调打印 ====

    /** 把 SDK 的每个回调原样打出来，不做任何判断——这就是「观察行为」的全部 */
    static final class Printer implements ImClientListener {

        private final Stats stats;
        private volatile ImClient client;

        Printer(Stats stats) {
            this.stats = stats;
        }

        void bind(ImClient client) {
            this.client = client;
        }

        @Override
        public void onConnected() {
            stats.disconnects.set(0);
            System.out.printf("[CONN] connected (transport=TCP, user=%s)%n",
                    client == null ? "?" : client.config().userId());
        }

        @Override
        public void onDisconnected(Throwable cause, long retryInMs) {
            stats.disconnects.incrementAndGet();
            String why = cause == null ? "(no cause)" : cause.getClass().getSimpleName() + ": " + cause.getMessage();
            if (retryInMs < 0) {
                System.out.printf("[CONN] disconnected: %s; will NOT retry%n", why);
            } else {
                System.out.printf("[CONN] disconnected: %s; retry in %dms%n", why, retryInMs);
            }
        }

        @Override
        public void onMessage(DownlinkFrame.Push push) {
            stats.received.incrementAndGet();
            System.out.printf("[RECV] conv=%s seq=%d from=%s content=%s body=%s%s%n",
                    push.conversationId(), push.seq(), push.from(), push.contentType(), push.body(),
                    push.recalled() ? " (recalled)" : "");
            // 收到就回已读：让对端也能看到 msg.state{READ}，手动测双端时效果更完整
            ImClient current = client;
            if (current != null && push.seq() > 0) {
                try {
                    current.markRead(push.conversationId(), push.seq());
                } catch (RuntimeException e) {
                    System.out.println("[RECV] markRead failed: " + e);
                }
            }
        }

        @Override
        public void onAck(DownlinkFrame.Ack ack, LocalMessage local) {
            if (ack.ok()) {
                stats.acked.incrementAndGet();
            } else {
                stats.errors.incrementAndGet();
            }
            long costMs = (System.nanoTime() - stats.lastSendNs.get()) / 1_000_000;
            System.out.printf("[ACK ] id=%s serverMsgId=%s seq=%d code=%d latency=%dms%n",
                    shortId(ack.clientMsgId()), ack.serverMsgId(), ack.seq(), ack.code(), costMs);
        }

        @Override
        public void onState(DownlinkFrame.State state) {
            System.out.printf("[STAT] conv=%s seq=%d state=%s by=%s%n",
                    state.conversationId(), state.seq(), state.state(), state.userId());
        }

        @Override
        public void onUpdate(DownlinkFrame.Update update) {
            System.out.printf("[UPDT] conv=%s seq=%d change=%s actor=%s%n",
                    update.conversationId(), update.seq(), update.change(), update.actorId());
        }

        @Override
        public void onGroupUpdate(DownlinkFrame.GroupUpdate update) {
            System.out.printf("[GRP ] group=%s change=%s actor=%s target=%s%n",
                    update.groupId(), update.change(), update.actorId(), update.targetId());
        }

        @Override
        public void onError(DownlinkFrame.Error error) {
            stats.errors.incrementAndGet();
            System.out.printf("[ERR ] code=%d message=%s id=%s%n",
                    error.code(), error.message(), shortId(error.clientMsgId()));
        }

        @Override
        public void onGap(String conversationId, long expectedFrom, long actualSeq) {
            System.out.printf("[GAP ] conv=%s expected=%d actual=%d (pull history to fill)%n",
                    conversationId, expectedFrom, actualSeq);
        }

        @Override
        public void onAuthRejected(String message) {
            stats.errors.incrementAndGet();
            System.out.printf("[AUTH] rejected: %s (SDK will not reconnect; check --token)%n", message);
        }
    }

    // ==== 统计与汇总 ====

    static final class Stats {

        final AtomicLong sent = new AtomicLong();
        final AtomicLong acked = new AtomicLong();
        final AtomicLong received = new AtomicLong();
        final AtomicLong errors = new AtomicLong();
        final AtomicLong disconnects = new AtomicLong();
        /** 最后一次 send 的时刻，用来算 ack 往返（单线程发送，不需要队列） */
        final AtomicLong lastSendNs = new AtomicLong(System.nanoTime());
    }

    private static void printSummary(Stats stats, ImClient client) {
        System.out.println("---- summary ----");
        System.out.printf("sent=%d acked=%d received=%d errors=%d disconnects=%d%n",
                stats.sent.get(), stats.acked.get(), stats.received.get(),
                stats.errors.get(), stats.disconnects.get());
        System.out.println("watermarks=" + client.watermarks());
    }

    // ==== 命令行参数 ====

    record Options(String host, int port, String userId, String deviceId, String token,
                   String to, long intervalMs, String text, long runSeconds) {

        /**
         * @return 解析结果；{@code null} 表示参数有问题或要求打印帮助
         */
        static Options parse(String[] args) {
            String host = "127.0.0.1";
            int port = 9092;
            String user = "demo-" + Integer.toHexString(ThreadLocalRandom.current().nextInt(0x1000, 0xFFFF));
            String device = "demo-device";
            String token = null;
            String to = "xiaozhao";
            long interval = 2_000;
            String text = "hello-from-sdk";
            long runSeconds = 0;
            try {
                for (int i = 0; i < args.length; i++) {
                    String arg = args[i];
                    switch (arg) {
                        case "-h", "--help" -> {
                            return null;
                        }
                        case "--host" -> host = args[++i];
                        case "--port" -> port = Integer.parseInt(args[++i]);
                        case "--user" -> user = args[++i];
                        case "--device" -> device = args[++i];
                        case "--token" -> token = args[++i];
                        case "--to" -> to = args[++i];
                        case "--interval" -> interval = Long.parseLong(args[++i]);
                        case "--text" -> text = args[++i];
                        case "--run-seconds" -> runSeconds = Long.parseLong(args[++i]);
                        default -> {
                            System.out.println("[CLI ] unknown option: " + arg);
                            return null;
                        }
                    }
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                System.out.println("[CLI ] missing value for " + args[args.length - 1]);
                return null;
            } catch (NumberFormatException e) {
                System.out.println("[CLI ] not a number: " + e.getMessage());
                return null;
            }
            if (port <= 0 || port > 65535) {
                System.out.println("[CLI ] invalid --port: " + port);
                return null;
            }
            if (interval < 100) {
                System.out.println("[CLI ] --interval must be >= 100ms (got " + interval + ")");
                return null;
            }
            return new Options(host, port, user, device, token, to, interval, text, runSeconds);
        }

        static void printUsage() {
            System.out.println("""
                    TcpClientDemo - connect to an IM server over TCP, send & print continuously

                    Usage:
                      TcpClientDemo [options]

                    Options:
                      --host <addr>         server address            (default 127.0.0.1)
                      --port <port>         TCP port, NOT the HTTP one (default 9092; im-gateway-boot often 7004)
                      --user <userId>       login identity            (default demo-<random hex>)
                      --device <deviceId>   device slot; keep it stable across restarts (default demo-device)
                      --token <token>       required only when im.auth.mode=enforce (default none)
                      --to <userId>         receiver of every message  (default bob)
                      --interval <ms>       send interval, >=100    (default 2000)
                      --text <prefix>       body prefix; "-N <time>" is appended (default hello-from-sdk)
                      --run-seconds <n>     auto stop after n seconds, 0 = run until stopped (default 0)
                      -h, --help            print this help

                    Stop:
                      type "q" + Enter, or press Ctrl+C (or use --run-seconds)

                    Two instances, talking to each other:
                      java ... TcpClientDemo --port 7004 --user alice --to bob
                      java ... TcpClientDemo --port 7004 --user bob   --to alice
                    """);
        }
    }

    /** clientMsgId 是 UUID，日志里打前 8 位够定位了 */
    private static String shortId(String value) {
        if (value == null) {
            return "-";
        }
        return value.length() <= 8 ? value : value.substring(0, 8);
    }
}
