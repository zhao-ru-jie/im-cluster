package com.github.im.app.controller;

import com.github.im.app.shadow.ShadowMessageIngress;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Shadow 双跑观测入口（仅单体装配进程提供）：{@code GET /shadow/stats}。
 *
 * <p>灰度切流前的判断依据：{@code matchRate} 应稳定为 1.0 且 {@code shadowErrors} 不增长，
 * 否则说明新传输通道（序列化/超时/幂等键透传）存在偏差，不能进入下一步。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@RestController
public class ShadowReportController {

    private final ObjectProvider<ShadowMessageIngress> shadowProvider;

    public ShadowReportController(ObjectProvider<ShadowMessageIngress> shadowProvider) {
        this.shadowProvider = shadowProvider;
    }

    @GetMapping("/shadow/stats")
    public Map<String, Object> stats() {
        ShadowMessageIngress shadow = shadowProvider.getIfAvailable();
        if (shadow == null) {
            Map<String, Object> disabled = new LinkedHashMap<>();
            disabled.put("enabled", false);
            return disabled;
        }
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("enabled", true);
        stats.putAll(shadow.stats());
        return stats;
    }
}
