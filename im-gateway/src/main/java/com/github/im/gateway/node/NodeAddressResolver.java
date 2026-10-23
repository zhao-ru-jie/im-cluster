package com.github.im.gateway.node;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * 解析「本网关实例对外可达地址」，用于生成实例 ID 并写入在线路由表。
 *
 * <p>拆分后该组件的语义变化：它标识的是<b>网关实例</b>（连接持有者），
 * 不再是「IM 业务节点」。因此 Redis 里保存的是「userId → 网关实例」，
 * IM 服务据此决定把下行帧推到哪个网关。
 *
 * <p>探测优先级（K8s 下 pod IP 运行时才分配，不能写死）：
 * <ol>
 *   <li>{@code IM_GRPC_HOST} / {@code GRPC_HOST} 手动覆盖（物理机跨机部署）</li>
 *   <li>K8s Downward API 的 {@code POD_IP}</li>
 *   <li>{@code POD_NAME}（StatefulSet + Headless Service 的稳定 DNS）</li>
 *   <li>本机第一个非回环 IPv4</li>
 *   <li>{@code 127.0.0.1} 兜底</li>
 * </ol>
 *
 * @author zhaorujie
 * @since 1.0.0
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
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        String podIp = System.getenv("POD_IP");
        if (podIp != null && !podIp.isBlank()) {
            return podIp;
        }
        String podName = System.getenv("POD_NAME");
        if (podName != null && !podName.isBlank()) {
            return podName;
        }
        String nic = firstNonLoopbackIp();
        if (nic != null) {
            return nic;
        }
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

    /** 本网关实例 ID（host:port），既是路由表的值，也是下行推送的 gRPC 目标 */
    public String localInstanceId() {
        return host + ":" + grpcPort;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return grpcPort;
    }

    /**
     * 注册到 Nacos / 供其健康探活使用的真实 IP。
     * Nacos 探活需要可直连的 IP，不能用 DNS 名或 127.0.0.1。
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
}
