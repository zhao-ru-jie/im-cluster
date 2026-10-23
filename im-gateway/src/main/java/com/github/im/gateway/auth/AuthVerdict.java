package com.github.im.gateway.auth;

/**
 * 一次身份校验的结论。
 *
 * <p>{@code reason} 会被当作指标 tag（{@code gateway_auth_rejected_total{reason=...}}），
 * 因此必须是<b>固定的小枚举值</b>，不要把 userId / token 片段拼进去——那会让 tag 基数爆炸，
 * 直接把 Prometheus 打挂。
 *
 * @param accepted 是否通过
 * @param reason   通过时为 {@code ok}；不通过时为固定原因码
 * @author zhaorujie
 * @since 1.0.0
 */
public record AuthVerdict(boolean accepted, String reason) {

    /** 通过 */
    public static final String OK = "ok";
    /** 没带 token */
    public static final String MISSING_TOKEN = "missing_token";
    /** 没带 userId（理论上更早被拦，兜底用） */
    public static final String MISSING_USER = "missing_user";
    /** token 格式不对 / 版本不认识 */
    public static final String MALFORMED_TOKEN = "malformed_token";
    /** 已过期 */
    public static final String EXPIRED_TOKEN = "expired_token";
    /** 签名不匹配（篡改、或与签发方密钥不一致） */
    public static final String BAD_SIGNATURE = "bad_signature";

    private static final AuthVerdict PASS = new AuthVerdict(true, OK);

    public static AuthVerdict ok() {
        return PASS;
    }

    public static AuthVerdict deny(String reason) {
        return new AuthVerdict(false, reason);
    }
}
