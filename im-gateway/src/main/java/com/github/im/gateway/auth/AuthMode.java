package com.github.im.gateway.auth;

import java.util.Locale;

/**
 * 接入鉴权的三档开关（{@code im.auth.mode}）。
 *
 * <p>为什么要三档而不是一个布尔：token 校验一旦上线，出错就是「全量客户端连不上」。
 * 主流做法都是先只观测、再阻断：
 * <ol>
 *   <li>{@link #OFF}：完全不校验。默认值——本地联调、内网部署不需要额外准备 token；</li>
 *   <li>{@link #WARN}：<b>校验并记录</b>，但一律放行。上线前跑一段时间，用
 *       {@code gateway_auth_rejected_total} 指标和日志确认「该被拒的都拒了、正常的没被误伤」；</li>
 *   <li>{@link #ENFORCE}：校验失败即拒绝（WebSocket 握手回 401、TCP 回 error 帧并断开）。</li>
 * </ol>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public enum AuthMode {

    /** 不校验（默认） */
    OFF,
    /** 校验但只记录，不阻断 */
    WARN,
    /** 校验并阻断 */
    ENFORCE;

    /**
     * 解析配置值。宽容一些：{@code off/none/false} → OFF，{@code on/true} → ENFORCE，
     * 其余非法值直接报错（配错了在启动时就炸，比上线后才发现「鉴权其实没生效」好）。
     */
    public static AuthMode parse(String raw) {
        if (raw == null || raw.isBlank()) {
            return OFF;
        }
        return switch (raw.trim().toLowerCase(Locale.ROOT)) {
            case "off", "none", "false" -> OFF;
            case "warn", "observe" -> WARN;
            case "enforce", "on", "true" -> ENFORCE;
            default -> throw new IllegalArgumentException(
                    "im.auth.mode 只支持 off / warn / enforce，实际配置: " + raw);
        };
    }
}
