package com.example.im.connection;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * 解析「本节点对外可达地址」，用于生成节点 ID 并写入 Redis 路由表。
 *
 * <p>这是 K8s / 容器化部署的关键：pod IP 是运行时才分配的，无法预先写死。
 * 本类按以下优先级自动探测，无需人工指定：
 * <ol>
 *   <li>显式环境变量 {@code IM_GRPC_HOST} / {@code GRPC_HOST}（手动覆盖，物理机跨机部署用）</li>
 *   <li>K8s Downward API 注入的 {@code POD_IP}（容器真实 IP，集群内其他 pod 可直接 dial）</li>
 *   <li>K8s Downward API 注入的 {@code POD_NAME}（StatefulSet + Headless Service 的 DNS 名，可跨节点解析）</li>
 *   <li>本机第一个非回环 IPv4 地址（best-effort，多网卡/容器网桥可能不准）</li>
 *   <li>{@code 127.0.0.1}（纯本地开发兜底）</li>
 * </ol>
 *
 * <p>解析结果与 gRPC 端口拼接成节点 ID（host:port），既是路由表的值，也是其他节点转发的目标地址。
 */
@Component
public class NodeAddressResolver {

    private final String host;
    private final int grpcPort;

    public NodeAddressResolver(@Value("${im.node.grpc-host:}") String configuredHost,
                               @Value("${im.node.grpc-port}") int grpcPort) {
        this.grpcPort = grpcPort;
        this.host = resolve(configuredHost);
    }

    private String resolve(String configured) {
        // 1) 手动覆盖
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        // 2) K8s 容器真实 IP
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isBlank()) {
            return podIp;
        }
        // 3) K8s StatefulSet DNS 名（headless service 可解析）
        String podName = System.getenv("POD_NAME");
        if (podName != null && !podName.isBlank()) {
            return podName;
        }
        // 4) best-effort 本机非回环 IP
        String nic = firstNonLoopbackIp();
        if (nic != null) {
            return nic;
        }
        // 5) 兜底
        return "127.0.0.1";
    }

    private String firstNonLoopbackIp() {
        try {
            Enumeration<NetworkInterface> nets = NetworkInterface.getNetworkInterfaces();
            while (nets.hasMoreElements()) {
                NetworkInterface ni = nets.nextElement();
                if (ni.isLoopback() || !ni.isUp()) {
                    continue;
                }
                Enumeration<InetAddress> addrs = ni.getInetAddresses();
                while (addrs.hasMoreElements()) {
                    InetAddress a = addrs.nextElement();
                    if (a instanceof Inet4Address && !a.isLoopbackAddress()) {
                        return a.getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            // 探测失败则回退到更后面的兜底项
        }
        return null;
    }

    /** 本节点对外可达地址（host:port），即节点 ID */
    public String localNodeId() {
        return host + ":" + grpcPort;
    }

    public String getHost() {
        return host;
    }

    /**
     * 注册到 Nacos / 供其健康探活使用的真实 IP。
     * Nacos 探活需要「能直连的 IP」，不能用 DNS 名（POD_NAME）或 127.0.0.1：
     * 优先 POD_IP → 本机非回环 IPv4 → 127.0.0.1 兜底。
     */
    public String getRegisterIp() {
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isBlank()) {
            return podIp;
        }
        if (isInet4Address(host)) {
            return host;
        }
        String nic = firstNonLoopbackIp();
        return nic != null ? nic : "127.0.0.1";
    }

    private boolean isInet4Address(String s) {
        if (s == null || s.isBlank()) {
            return false;
        }
        String[] parts = s.split("\\.");
        if (parts.length != 4) {
            return false;
        }
        try {
            for (String p : parts) {
                int v = Integer.parseInt(p);
                if (v < 0 || v > 255) {
                    return false;
                }
            }
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    public int getPort() {
        return grpcPort;
    }
}
