package com.github.im.benchmark;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * 压测端命令行参数。
 *
 * <p>为什么不用配置文件：压测的每次运行都是一次实验，参数必须跟着命令行（以及脚本里的那一行）
 * 一起进版本库、一起进压测报告，配置文件容易让「报告里的数字对应哪套参数」说不清。
 *
 * <p>用法：{@code java -jar im-benchmark.jar --connections=10000 --duration=5m}
 * （不带参数或 {@code --help} 打印 {@link #USAGE}）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record BenchmarkOptions(
        String host,
        int port,
        String path,
        boolean tls,
        String userIdPrefix,
        String deviceId,
        String token,
        int connections,
        int connectRatePerSec,
        int sendRatePerSec,
        int maxInFlight,
        int bodyBytes,
        Duration duration,
        int reportIntervalMs,
        Target target,
        int ioThreads) {

    /** 消息发给谁：决定压测能覆盖到「下行推送」还是只覆盖「上行受理 + ack」。 */
    public enum Target {
        /** 环形：第 i 个连接发给第 i+1 个连接（最后一个发回第 0 个），上下行链路都被压到 */
        RING,
        /** 发给自己：只压上行受理与 ack，下行推送量最小 */
        SELF
    }

    public static final String USAGE = """
            压测端：模拟大量 WebSocket 长连接，压 im-gateway 的建连 / 心跳 / 上行受理与下行推送。

            用法：
              java -jar im-benchmark.jar [选项...]

            连接（握手）：网关的 userId/token 只从握手 query 取，不支持 header、不支持子协议
              --host=localhost        目标主机
              --port=8081             目标端口（Netty WS 端口，不是 Tomcat 的 server.port）
              --path=/ws              WebSocket 路径
              --tls=false             true 则用 wss（压测场景跳过证书校验）
              --user-prefix=bench-    虚拟用户 ID 前缀，第 i 个连接用 <前缀><i>
              --device=bench          设备 ID；注意网关同一 userId 最多 5 个设备，
                                      本压测端每个连接独占一个 userId，因此不受该限制影响
              --token=                握手 token（im.auth.mode=enforce 时必填，所有连接共用）

            负载：
              --connections=1000      虚拟连接数
              --connect-rate=0        建连速率（个/秒），0 = 不限速；大连接数时建议限速，
                                      否则会被 TCP 连接拒绝/半开队列打乱结论
              --send-rate=1           每连接每秒发送的消息数，0 = 只建连不发消息
              --max-inflight=8        单连接未收到 ack 的消息上限（背压阈值，满了就跳过）
              --body-bytes=64         消息体字节数（消息体前缀会带发送时刻，用于算端到端延迟）
              --duration=60s          压测时长，支持 500ms / 60s / 5m / 1h
              --target=ring           消息发给谁：ring | self
              --report=1000           过程中报告的间隔（毫秒）

            线程：
              --io-threads=0          Netty event loop 线程数，0 = 按 CPU 核数

            示例：
              # 只验建连与心跳（不发业务消息）
              java -jar im-benchmark.jar --connections=5000 --send-rate=0 --duration=2m

              # 1 万连接、每连接每秒 2 条单聊消息、边建连边限速 1000/s
              java -jar im-benchmark.jar --connections=10000 --connect-rate=1000 \\
                   --send-rate=2 --duration=5m --target=ring
            """;

    /** 参数名全集：写错参数名直接报错，而不是被静默忽略（压测时静默忽略代价很高）。 */
    private static final Set<String> KNOWN_KEYS = Set.of(
            "host", "port", "path", "tls", "user-prefix", "device", "token",
            "connections", "connect-rate", "send-rate", "max-inflight", "body-bytes",
            "duration", "target", "report", "io-threads", "help");

    /**
     * @return 解析结果；若命令行要求打印用法则返回 {@code null}
     */
    public static BenchmarkOptions parse(String[] args) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (String arg : args) {
            if ("-h".equals(arg) || "--help".equals(arg)) {
                return null;
            }
            if (!arg.startsWith("--")) {
                throw new IllegalArgumentException("参数必须以 -- 开头且形如 --key=value，收到：" + arg);
            }
            int eq = arg.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("参数缺少取值，应写成 --key=value，收到：" + arg);
            }
            String key = arg.substring(2, eq);
            if (!KNOWN_KEYS.contains(key)) {
                throw new IllegalArgumentException("未知参数 --" + key + "，可用参数见 --help");
            }
            kv.put(key, arg.substring(eq + 1));
        }

        String path = text(kv, "path", "/ws");
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        BenchmarkOptions options = new BenchmarkOptions(
                text(kv, "host", "localhost"),
                number(kv, "port", 8081),
                path,
                bool(kv, "tls", false),
                text(kv, "user-prefix", "bench-"),
                text(kv, "device", "bench"),
                blankToNull(text(kv, "token", "")),
                number(kv, "connections", 1000),
                number(kv, "connect-rate", 0),
                number(kv, "send-rate", 1),
                number(kv, "max-inflight", 8),
                number(kv, "body-bytes", 64),
                duration(kv, "duration", Duration.ofSeconds(60)),
                number(kv, "report", 1000),
                target(kv, "target", Target.RING),
                number(kv, "io-threads", 0));

        if (options.connections() <= 0) {
            throw new IllegalArgumentException("--connections 必须大于 0");
        }
        if (options.port() <= 0 || options.port() > 65535) {
            throw new IllegalArgumentException("--port 超出范围：" + options.port());
        }
        if (options.sendRatePerSec() < 0 || options.connectRatePerSec() < 0) {
            throw new IllegalArgumentException("--send-rate / --connect-rate 不能为负");
        }
        if (options.maxInFlight() <= 0) {
            throw new IllegalArgumentException("--max-inflight 必须大于 0");
        }
        if (options.bodyBytes() < 0) {
            throw new IllegalArgumentException("--body-bytes 不能为负");
        }
        if (options.duration().isNegative() || options.duration().isZero()) {
            throw new IllegalArgumentException("--duration 必须大于 0");
        }
        return options;
    }

    /** 第 {@code index} 个虚拟连接的握手 URI（query 里带 userId/deviceId/token）。 */
    public String uri(int index) {
        StringBuilder sb = new StringBuilder(tls ? "wss://" : "ws://")
                .append(host).append(':').append(port).append(path)
                .append('?')
                .append("userId=").append(encode(userId(index)))
                .append("&deviceId=").append(encode(deviceId));
        if (token != null) {
            sb.append("&token=").append(encode(token));
        }
        return sb.toString();
    }

    public String userId(int index) {
        return userIdPrefix + index;
    }

    /** 消息收件人：每个连接独占一个 userId，所以 RING 天然保证每条消息都能被另一个连接收到。 */
    public String targetUserId(int index) {
        return switch (target) {
            case SELF -> userId(index);
            case RING -> userId((index + 1) % connections);
        };
    }

    /** event loop 线程数：0 表示按核数，且不超过连接数（连接很少时开一堆线程纯属浪费）。 */
    public int resolvedIoThreads() {
        if (ioThreads > 0) {
            return ioThreads;
        }
        return Math.max(1, Math.min(Runtime.getRuntime().availableProcessors(), connections));
    }

    /** 单连接发送间隔（纳秒）。 */
    public long sendIntervalNanos() {
        return sendRatePerSec <= 0 ? 0 : 1_000_000_000L / sendRatePerSec;
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    private static String text(Map<String, String> kv, String key, String defaultValue) {
        String value = kv.get(key);
        return value == null || value.isBlank() ? defaultValue : value.trim();
    }

    private static String blankToNull(String value) {
        return value == null || value.isBlank() ? null : value;
    }

    private static int number(Map<String, String> kv, String key, int defaultValue) {
        String value = kv.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " 需要整数，收到：" + value);
        }
    }

    private static boolean bool(Map<String, String> kv, String key, boolean defaultValue) {
        String value = kv.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return switch (value.trim().toLowerCase()) {
            case "true", "1", "yes" -> true;
            case "false", "0", "no" -> false;
            default -> throw new IllegalArgumentException("--" + key + " 需要 true/false，收到：" + value);
        };
    }

    private static Target target(Map<String, String> kv, String key, Target defaultValue) {
        String value = kv.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return switch (value.trim().toLowerCase()) {
            case "ring" -> Target.RING;
            case "self" -> Target.SELF;
            default -> throw new IllegalArgumentException("--" + key + " 只能是 ring 或 self，收到：" + value);
        };
    }

    /**
     * 时长解析：纯数字按秒处理，后缀支持 ms/s/m/h。
     * 用自定义格式而不是 {@code Duration.parse}，是因为压测时手上写的一定是 {@code 5m} 而不是 {@code PT5M}。
     */
    private static Duration duration(Map<String, String> kv, String key, Duration defaultValue) {
        String value = kv.get(key);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        String text = value.trim().toLowerCase();
        try {
            if (text.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(text.substring(0, text.length() - 2)));
            }
            if (text.endsWith("s")) {
                return Duration.ofSeconds(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            if (text.endsWith("m")) {
                return Duration.ofMinutes(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            if (text.endsWith("h")) {
                return Duration.ofHours(Long.parseLong(text.substring(0, text.length() - 1)));
            }
            return Duration.ofSeconds(Long.parseLong(text));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("--" + key + " 需要形如 500ms / 60s / 5m / 1h，收到：" + value);
        }
    }
}
