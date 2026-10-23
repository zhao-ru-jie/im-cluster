package com.github.im.gateway.auth;

import com.github.im.gateway.metrics.GatewayMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 鉴权闸门：把「开关档位」和「具体校验实现」粘在一起，传输层只调 {@link #check}。
 *
 * <p>它同时承担两个职责，都是为了「上线不出事」：
 * <ul>
 *   <li>{@link AuthMode#WARN} 档下把拒绝原因记进指标和日志，但照常放行——先观测再阻断；</li>
 *   <li>{@link AuthMode#ENFORCE} 档下把结论原样返回给传输层去拒绝。</li>
 * </ul>
 *
 * <p>配置错误（{@code mode=warn/enforce} 但没配 secret）在<b>启动时</b>就抛异常：
 * 否则会出现「以为开了鉴权、其实一直在放行」这种最危险的状态。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class AuthGuard {

    private static final Logger log = LoggerFactory.getLogger(AuthGuard.class);

    private final AuthMode mode;
    private final SessionAuthenticator authenticator;
    private final GatewayMetrics metrics;

    public AuthGuard(@Value("${im.auth.mode:off}") String mode,
                     @Value("${im.auth.secret:}") String secret,
                     GatewayMetrics metrics) {
        this.mode = AuthMode.parse(mode);
        this.metrics = metrics;
        if (this.mode == AuthMode.OFF) {
            this.authenticator = null;
            log.info("[auth] token 校验未启用（im.auth.mode=off）：客户端只需带 userId，仅适用于本地联调/内网");
            return;
        }
        if (secret == null || secret.isBlank()) {
            throw new IllegalStateException("im.auth.mode=" + this.mode.name().toLowerCase()
                    + " 必须配置 im.auth.secret（与 token 签发方共用同一密钥）；"
                    + "本地联调可显式设 im.auth.mode=off");
        }
        this.authenticator = new HmacTokenAuthenticator(secret);
        log.info("[auth] token 校验已启用：mode={}（{}）",
                this.mode.name().toLowerCase(),
                this.mode == AuthMode.WARN ? "只记录不阻断，用于上线前观察" : "校验失败即拒绝连接");
    }

    /**
     * 校验一条连接的身份。
     *
     * @return {@code accepted=false} 时调用方必须拒绝连接并断开；{@code reason} 可直接回给客户端
     */
    public AuthVerdict check(String userId, String token) {
        if (authenticator == null) {
            return AuthVerdict.ok();
        }
        AuthVerdict verdict = authenticator.authenticate(userId, token);
        if (verdict.accepted()) {
            return verdict;
        }
        if (metrics != null) {
            metrics.authRejected(verdict.reason());
        }
        if (mode == AuthMode.ENFORCE) {
            log.warn("[auth] 拒绝连接：userId={} reason={}", userId, verdict.reason());
            return verdict;
        }
        log.warn("[auth] warn 档放行（切 enforce 后会拒绝）：userId={} reason={}", userId, verdict.reason());
        return AuthVerdict.ok();
    }

    /** 当前档位（启动日志/健康检查/排障用） */
    public AuthMode mode() {
        return mode;
    }
}
