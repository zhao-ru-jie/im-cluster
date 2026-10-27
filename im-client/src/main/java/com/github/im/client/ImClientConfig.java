package com.github.im.client;

/**
 * SDK 配置。不可变，全部字段由 builder 补齐缺省值。
 *
 * <p>默认值都按「移动端弱网」选，不是为了本机联调好看：
 * 连接超时 5s（弱网握手常常 1~2s）、认证窗口 10s（与网关 {im.gateway.transport.tcp-auth-timeout-ms}）。
 *
 * <h2>为什么 deviceId 必须稳定</h2>
 *
 * <p>服务端用 {@code userId + deviceId} 定位会话槽位：同一 deviceId 重连会<b>踢掉旧连接</b>，
 * 不同 deviceId 才并存（上限 5）。如果每次启动随机生成 deviceId，
 * 服务端会认为「又多了一台新设备」，多开几次就把自己的槽位占满、把真机挤下线。
 *
 * <h2>为什么 keepAlive 默认 30s</h2>
 *
 * <p>网关 45s 无上行就发 ping、90s 无任何上行直接断开。客户端主动 30s 一次 ping
 * 有双重作用：让 NAT 表项不过期，以及在「网关还没判超时」之前就发现半开连接
 * （写成功但没 pong，说明链路已经单向断了，早重连比等 90s 更划算）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public record ImClientConfig(
        String host,
        int port,
        Transport transport,
        String wsPath,
        boolean wsTls,
        String userId,
        String deviceId,
        String token,
        int connectTimeoutMs,
        int authTimeoutMs,
        int maxFrameBytes,
        int recvBatchMax,
        int recvFlushIntervalMs,
        long keepAliveIntervalMs,
        long outboxScanIntervalMs,
        int outboxBatch,
        long sendRetryBaseMs,
        long sendRetryMaxMs,
        ReconnectPolicy reconnectPolicy) {

    /**
     * 传输承载。
     *
     * <p>{@link #TCP} 走原生 socket（App/桌面长连接，端口默认 9092），身份靠首帧 {@code connect}；
     * {@link #WEBSOCKET} 走 HTTP 升级（浏览器/小程序/受限于代理的环境，端口默认 8081），
     * 身份在握手 query 里。
     *
     * <p>两者<b>帧内容完全一致</b>，差别只有分帧方式与身份声明位置——
     * 所以业务层（重连、重投、去重、水位）不需要知道用的是哪个。
     */
    public enum Transport {
        TCP,
        WEBSOCKET
    }

    public ImClientConfig {
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("host 不能为空");
        }
        if (port <= 0 || port > 65535) {
            throw new IllegalArgumentException("port 非法：" + port);
        }
        if (transport == null) {
            transport = Transport.TCP;
        }
        if (wsPath == null || wsPath.isBlank()) {
            wsPath = "/ws";
        } else if (!wsPath.startsWith("/")) {
            // 少一个斜杠会让握手 URI 变成 ws://host:8081ws?...，报出来的错和「路径写错」毫无关系
            throw new IllegalArgumentException("wsPath 必须以 / 开头：" + wsPath);
        }
        if (userId == null || userId.isBlank()) {
            // 网关握手缺 userId 直接 400，这里提前失败比让用户看到一个连接反复被拒更清楚
            throw new IllegalArgumentException("userId 不能为空");
        }
        if (deviceId == null || deviceId.isBlank()) {
            deviceId = "default";
        }
        if (reconnectPolicy == null) {
            reconnectPolicy = new ReconnectPolicy();
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String host = "127.0.0.1";
        private int port = 9092;
        private Transport transport = Transport.TCP;
        private String wsPath = "/ws";
        private boolean wsTls;
        private String userId;
        private String deviceId = "default";
        private String token;
        private int connectTimeoutMs = 5_000;
        private int authTimeoutMs = 10_000;
        private int maxFrameBytes = 1024 * 1024;
        private int recvBatchMax = 50;
        private int recvFlushIntervalMs = 100;
        private long keepAliveIntervalMs = 30_000;
        private long outboxScanIntervalMs = 500;
        private int outboxBatch = 100;
        private long sendRetryBaseMs = 500;
        private long sendRetryMaxMs = 30_000;
        private ReconnectPolicy reconnectPolicy = new ReconnectPolicy();

        public Builder host(String v) {
            this.host = v;
            return this;
        }

        public Builder port(int v) {
            this.port = v;
            return this;
        }

        public Builder transport(Transport v) {
            this.transport = v;
            return this;
        }

        /** 仅 WebSocket：网关的 {@code im.gateway.transport.ws-path}，默认 {@code /ws} */
        public Builder wsPath(String v) {
            this.wsPath = v;
            return this;
        }

        /** 仅 WebSocket：是否用 {@code wss://}（TLS 通常由反向代理终结，SDK 直连时用不到） */
        public Builder wsTls(boolean v) {
            this.wsTls = v;
            return this;
        }

        public Builder userId(String v) {
            this.userId = v;
            return this;
        }

        public Builder deviceId(String v) {
            this.deviceId = v;
            return this;
        }

        public Builder token(String v) {
            this.token = v;
            return this;
        }

        public Builder connectTimeoutMs(int v) {
            this.connectTimeoutMs = v;
            return this;
        }

        public Builder authTimeoutMs(int v) {
            this.authTimeoutMs = v;
            return this;
        }

        public Builder maxFrameBytes(int v) {
            this.maxFrameBytes = v;
            return this;
        }

        public Builder recvBatchMax(int v) {
            this.recvBatchMax = v;
            return this;
        }

        public Builder recvFlushIntervalMs(int v) {
            this.recvFlushIntervalMs = v;
            return this;
        }

        /** 0 = 关闭主动保活（只被动应答服务端 ping） */
        public Builder keepAliveIntervalMs(long v) {
            this.keepAliveIntervalMs = v;
            return this;
        }

        public Builder outboxScanIntervalMs(long v) {
            this.outboxScanIntervalMs = v;
            return this;
        }

        public Builder outboxBatch(int v) {
            this.outboxBatch = v;
            return this;
        }

        public Builder sendRetryBaseMs(long v) {
            this.sendRetryBaseMs = v;
            return this;
        }

        public Builder sendRetryMaxMs(long v) {
            this.sendRetryMaxMs = v;
            return this;
        }

        public Builder reconnectPolicy(ReconnectPolicy v) {
            this.reconnectPolicy = v;
            return this;
        }

        public ImClientConfig build() {
            return new ImClientConfig(host, port, transport, wsPath, wsTls, userId, deviceId, token,
                    connectTimeoutMs, authTimeoutMs, maxFrameBytes, recvBatchMax, recvFlushIntervalMs,
                    keepAliveIntervalMs, outboxScanIntervalMs, outboxBatch,
                    sendRetryBaseMs, sendRetryMaxMs, reconnectPolicy);
        }
    }
}
