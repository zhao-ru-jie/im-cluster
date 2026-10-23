package com.github.im.gateway.controller;

import com.github.im.gateway.drain.GatewayDrainService;
import com.github.im.gateway.session.ClientSession;
import com.github.im.gateway.session.ConnectionManager;
import com.github.im.gateway.transport.NettyGatewayServer;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 网关运维入口（供 K8s preStop hook 与人工摘流使用）。
 *
 * <pre>
 * POST /admin/drain?reason=scale-in   触发优雅摘流（幂等，不退出进程）
 * GET  /admin/status                  查看当前连接数 / 是否在摘流
 * </pre>
 *
 * <p><b>安全边界</b>：本入口不做鉴权，必须只在内网/集群内可达（K8s 用 NetworkPolicy
 * 或独立 management 端口 + Service 限制访问），不要暴露到公网 Ingress。
 */
@RestController
@RequestMapping("/admin")
public class GatewayAdminController {

    private final GatewayDrainService drainService;
    private final ConnectionManager connectionManager;
    private final NettyGatewayServer nettyServer;

    public GatewayAdminController(GatewayDrainService drainService,
                                  ConnectionManager connectionManager,
                                  NettyGatewayServer nettyServer) {
        this.drainService = drainService;
        this.connectionManager = connectionManager;
        this.nettyServer = nettyServer;
    }

    @PostMapping("/drain")
    public Map<String, Object> drain(@RequestParam(defaultValue = "manual") String reason) {
        GatewayDrainService.DrainResult result = drainService.drain(reason);
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("instanceId", result.instanceId());
        body.put("alreadyDraining", result.alreadyDraining());
        body.put("notified", result.notified());
        body.put("forcedClosed", result.forcedClosed());
        return body;
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("draining", drainService.isDraining());
        body.put("connections", connectionManager.connectionCount());
        body.put("transports", nettyServer.describe());
        // 双通道拆分看一眼占比：WS 客户端迁移到 TCP 的进度靠这两个数
        body.put(ClientSession.TRANSPORT_WS, connectionManager.connectionCount(ClientSession.TRANSPORT_WS));
        body.put(ClientSession.TRANSPORT_TCP, connectionManager.connectionCount(ClientSession.TRANSPORT_TCP));
        return body;
    }
}
