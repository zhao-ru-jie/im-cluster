package com.github.im.server.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.util.Arrays;
import java.util.List;

/**
 * REST 接口的跨源（CORS）开关，默认对 {@code im-app} 自带的测试客户端开放。
 *
 * <h2>为什么需要它</h2>
 *
 * <p>{@code im-app/src/main/resources/static/index.html} 这个测试客户端只在<b>由 IM 服务自己
 * 提供</b>时是同源的。一旦通过 IDE 内置服务器（{@code localhost:63342/...}）、前端 dev server
 * 之类的静态服务器打开，页面里的 {@code fetch('/conv?...')} 就会打到那台静态服务器上：
 * 同源请求回它自己的 404 HTML，跨源请求则被浏览器直接拦下。两种表现看起来都像「接口没实现」，
 * 而真正的信息（CORS 缺头）只在浏览器控制台里。
 *
 * <p>WebSocket 不受同源策略约束，所以现象经常是「长连接能通、REST 全挂」——这个组合最容易被
 * 误判成后端挂了，因此这里把开关做出来，而不是让人去猜。
 *
 * <h2>默认值的取舍</h2>
 *
 * <p>默认 {@code *}：被开放的这些接口（{@code /conv}、{@code /group}、{@code /relation}、
 * {@code /presence}、{@code /send}、{@code /admin}、{@code /shadow}）本来就<b>不带任何鉴权</b>
 * ——它们把 URL 里的 {@code userId}/{@code operatorId} 当作已认证身份，注释里也写明了
 * 「对外暴露时必须由接入层校验」。也就是说 CORS 在这里不是最后一道防线，
 * 放开它不会让攻击面比现在更大；真正的防线是别把这些端口直接暴露到公网。
 *
 * <p>要收窄或关闭：
 * <pre>
 * im.http.cors.allowed-origins=http://localhost:63342,http://localhost:5173   # 白名单
 * im.http.cors.allowed-origins=                                              # 完全关闭
 * </pre>
 *
 * <p>只对上面这几个路径生效，静态资源（{@code /index.html}）与 WebSocket 握手不在其中：
 * 它们不需要 CORS，也不该被这条配置牵连。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Configuration
public class RestCorsConfig implements WebMvcConfigurer {

    private static final Logger log = LoggerFactory.getLogger(RestCorsConfig.class);

    /** 需要跨源的 REST 前缀；{@code /send} 没有子路径，单独列出 */
    private static final List<String> MAPPINGS = List.of(
            "/conv/**",
            "/group/**",
            "/relation/**",
            "/presence/**",
            "/device/**",
            "/send",
            "/admin/**",
            "/shadow/**");

    private final List<String> allowedOriginPatterns;

    public RestCorsConfig(@Value("${im.http.cors.allowed-origins:*}") String allowedOrigins) {
        this.allowedOriginPatterns = allowedOrigins == null ? List.of()
                : Arrays.stream(allowedOrigins.split(","))
                        .map(String::trim)
                        .filter(origin -> !origin.isEmpty())
                        .toList();
        if (allowedOriginPatterns.isEmpty()) {
            log.info("[cors] 未配置 im.http.cors.allowed-origins，REST 跨源已关闭");
        } else {
            log.info("[cors] REST 跨源已开启：allowedOriginPatterns={}，路径={}",
                    allowedOriginPatterns, MAPPINGS);
        }
    }

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        if (allowedOriginPatterns.isEmpty()) {
            return;
        }
        String[] patterns = allowedOriginPatterns.toArray(String[]::new);
        for (String mapping : MAPPINGS) {
            registry.addMapping(mapping)
                    // 用 OriginPatterns 而不是 Origins：后者在带凭据时不允许 *，且不支持通配后缀，
                    // 将来要开 cookie 认证时这里不用改
                    .allowedOriginPatterns(patterns)
                    .allowedMethods("GET", "POST", "DELETE", "OPTIONS")
                    .allowedHeaders("*")
                    // 预检结果缓存 30 分钟：测试客户端点一次按钮就是一批请求，别每次都预检
                    .maxAge(1800);
        }
    }
}
