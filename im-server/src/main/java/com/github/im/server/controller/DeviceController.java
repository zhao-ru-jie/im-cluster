package com.github.im.server.controller;

import com.github.im.server.device.DeviceService;
import com.github.im.server.device.DeviceStore;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 多端设备管理入口。
 *
 * <pre>
 * GET    /device/{userId}                          设备列表（在线优先、最近登录优先）
 * POST   /device/{userId}/kick?deviceId=&reason=   踢指定设备下线
 * POST   /device/{userId}/kick-others?keepDeviceId= 退出其他设备（设置页常用）
 * DELETE /device/{userId}/{deviceId}               从账号下移除这台设备
 * </pre>
 *
 * <p>三个写操作都会真实关闭 TCP 连接（而不是「推一帧让客户端自觉退出」），
 * 关闭前会给客户端发一条 {@code session.kick} 作为退出理由。
 *
 * <p><b>鉴权仍然留给接入层</b>：这些接口把 URL 里的 {@code userId} 当成已认证身份，
 * 对外暴露时必须校验调用者就是这个用户——否则任何人拿着 userId 就能把别人踢下线。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
@RequestMapping("/device")
public class DeviceController {

    private final DeviceService deviceService;

    public DeviceController(DeviceService deviceService) {
        this.deviceService = deviceService;
    }

    @GetMapping("/{userId}")
    public Map<String, Object> list(@PathVariable String userId) {
        List<DeviceStore.DeviceRow> devices = deviceService.list(userId);
        List<Map<String, Object>> items = new ArrayList<>(devices.size());
        for (DeviceStore.DeviceRow device : devices) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("deviceId", device.deviceId());
            item.put("online", device.online());
            item.put("transport", device.transport());
            item.put("gateway", device.gateway());
            item.put("loginAt", device.loginAt());
            items.add(item);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("userId", userId);
        result.put("count", items.size());
        result.put("onlineCount", items.stream().filter(i -> Boolean.TRUE.equals(i.get("online"))).count());
        result.put("devices", items);
        return result;
    }

    @PostMapping("/{userId}/kick")
    public Map<String, Object> kick(@PathVariable String userId,
                                    @RequestParam(name = "deviceId") String deviceId,
                                    @RequestParam(name = "reason", defaultValue = "kicked-by-server")
                                    String reason) {
        return outcome(userId, deviceService.kick(userId, deviceId, null, reason));
    }

    /**
     * 退出其他设备（保留 {@code keepDeviceId} 这一台）。
     *
     * <p>为什么不复用 {@code kick}：设备管理的默认路径上不该存在「一次踢光所有设备」的入口，
     * 那种动作应该由封号/改密这类业务触发，而不是列表页上的一个按钮。
     */
    @PostMapping("/{userId}/kick-others")
    public Map<String, Object> kickOthers(@PathVariable String userId,
                                         @RequestParam(name = "keepDeviceId") String keepDeviceId,
                                         @RequestParam(name = "reason", defaultValue = "logout-other-devices")
                                         String reason) {
        return outcome(userId, deviceService.kick(userId, null, keepDeviceId, reason));
    }

    @DeleteMapping("/{userId}/{deviceId}")
    public Map<String, Object> forget(@PathVariable String userId, @PathVariable String deviceId) {
        boolean removed = deviceService.forget(userId, deviceId);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", removed ? 0 : 400);
        result.put("message", removed ? "removed" : "userId and deviceId are required");
        result.put("userId", userId);
        result.put("deviceId", deviceId);
        return result;
    }

    private Map<String, Object> outcome(String userId, DeviceService.KickOutcome outcome) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("code", outcome.code());
        result.put("message", outcome.message());
        result.put("userId", userId);
        result.put("kicked", outcome.kicked());
        result.put("deviceIds", outcome.deviceIds());
        // controllable=false 表示这个部署没有连接控制通道（踢下线根本没执行），
        // 与「执行了但设备不在线（kicked=0, controllable=true）」是两件完全不同的事
        result.put("controllable", outcome.controllable());
        return result;
    }
}
