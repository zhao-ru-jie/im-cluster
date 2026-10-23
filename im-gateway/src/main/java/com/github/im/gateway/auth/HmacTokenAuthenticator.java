package com.github.im.gateway.auth;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.Base64;

/**
 * 自签 HMAC-SHA256 token 的校验器（不引入 JWT 依赖，够用且没有算法混淆风险）。
 *
 * <h2>token 格式</h2>
 * <pre>
 * v1.&lt;过期时间(epoch 秒)&gt;.&lt;base64url(HMAC-SHA256(secret, "v1|userId|expire"))&gt;
 * 例：v1.1761264000.qQ7m1s...（无 padding）
 * </pre>
 *
 * <p>签名材料里带上 {@code userId} 是<b>关键</b>：不带的话，任何人拿到自己的 token 都能改
 * query 里的 {@code userId} 冒充别人。带上 {@code expire} 才能让同一个签名只能用到过期为止。
 *
 * <h2>为什么不用 JWT</h2>
 * <p>JWT 的价值主要在「跨语言、多方互认、可带自定义 claim」；本项目 token 只给网关自己校验，
 * 引入 jjwt 反而多一个依赖和一类坑（{@code alg=none}、算法混淆、时钟漂移）。真接认证中心时，
 * 实现另一个 {@link SessionAuthenticator} 即可，传输层不用动。
 *
 * <h2>签发</h2>
 * <p>token 由业务认证中心用<b>同一个 secret</b> 签发，规则见 {@link #issue(String, long, String)}。
 * 本地联调可以直接跑 {@code main}：
 * <pre>
 * java -cp ... com.github.im.gateway.auth.HmacTokenAuthenticator &lt;secret&gt; &lt;userId&gt; [ttlSeconds=3600]
 * </pre>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class HmacTokenAuthenticator implements SessionAuthenticator {

    /** 版本前缀，为将来换算法/换格式留出兼容位 */
    public static final String VERSION = "v1";

    private static final String ALGORITHM = "HmacSHA256";
    /** 签名输入里的分隔符：用不可出现在 userId 里的字符，避免 "a|b" 拼接歧义 */
    private static final char SEP = '|';

    private final byte[] secret;

    public HmacTokenAuthenticator(String secret) {
        if (secret == null || secret.isBlank()) {
            throw new IllegalArgumentException("HMAC 鉴权必须配置 im.auth.secret");
        }
        this.secret = secret.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public AuthVerdict authenticate(String userId, String token) {
        if (userId == null || userId.isBlank()) {
            return AuthVerdict.deny(AuthVerdict.MISSING_USER);
        }
        if (token == null || token.isBlank()) {
            return AuthVerdict.deny(AuthVerdict.MISSING_TOKEN);
        }
        int first = token.indexOf('.');
        int second = first < 0 ? -1 : token.indexOf('.', first + 1);
        if (first <= 0 || second < 0 || token.indexOf('.', second + 1) >= 0) {
            return AuthVerdict.deny(AuthVerdict.MALFORMED_TOKEN);
        }
        if (!VERSION.equals(token.substring(0, first))) {
            return AuthVerdict.deny(AuthVerdict.MALFORMED_TOKEN);
        }
        long expireAt;
        try {
            expireAt = Long.parseLong(token.substring(first + 1, second));
        } catch (NumberFormatException e) {
            return AuthVerdict.deny(AuthVerdict.MALFORMED_TOKEN);
        }
        if (expireAt <= Instant.now().getEpochSecond()) {
            return AuthVerdict.deny(AuthVerdict.EXPIRED_TOKEN);
        }
        String expected = sign(userId, expireAt, secret);
        String actual = token.substring(second + 1);
        // 定长比较：普通 equals 会因提前返回而泄漏「前几位匹配」的信息
        if (!MessageDigest.isEqual(expected.getBytes(StandardCharsets.UTF_8),
                actual.getBytes(StandardCharsets.UTF_8))) {
            return AuthVerdict.deny(AuthVerdict.BAD_SIGNATURE);
        }
        return AuthVerdict.ok();
    }

    /**
     * 签发 token（认证中心/联调工具用；与校验共用同一套签名规则）。
     *
     * @param userId        被签发的用户
     * @param expireEpochSec 过期时间（epoch 秒）
     * @param secret        与网关配置一致的密钥
     */
    public static String issue(String userId, long expireEpochSec, String secret) {
        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("userId 不能为空");
        }
        return VERSION + '.' + expireEpochSec + '.' + sign(userId, expireEpochSec,
                secret.getBytes(StandardCharsets.UTF_8));
    }

    /** 本地联调签发工具：{@code <secret> <userId> [ttlSeconds]} */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("usage: HmacTokenAuthenticator <secret> <userId> [ttlSeconds=3600]");
            return;
        }
        long ttl = args.length > 2 ? Long.parseLong(args[2]) : 3600L;
        long expireAt = Instant.now().getEpochSecond() + ttl;
        System.out.println("userId   = " + args[1]);
        System.out.println("expireAt = " + expireAt + " (" + Instant.ofEpochSecond(expireAt) + ")");
        System.out.println("token    = " + issue(args[1], expireAt, args[0]));
    }

    private static String sign(String userId, long expireEpochSec, byte[] secret) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret, ALGORITHM));
            String material = VERSION + SEP + userId + SEP + expireEpochSec;
            byte[] digest = mac.doFinal(material.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (GeneralSecurityException e) {
            // 只有 JVM 不支持 HmacSHA256 才会走到这里：属于环境错误，直接抛出而不是静默放行
            throw new IllegalStateException("HMAC-SHA256 不可用", e);
        }
    }
}
