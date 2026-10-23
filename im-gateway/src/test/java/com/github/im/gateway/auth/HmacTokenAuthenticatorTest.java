package com.github.im.gateway.auth;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * token 校验的单测。
 *
 * <p>这些用例守的是「<b>能冒充别人</b>」这条红线，所以重点不在 happy path，而在三条否定断言：
 * 换 userId、改过期时间、动签名，任何一条能过就是越权漏洞。
 */
class HmacTokenAuthenticatorTest {

    private static final String SECRET = "test-secret-please-change";

    private final HmacTokenAuthenticator authenticator = new HmacTokenAuthenticator(SECRET);

    @Test
    @DisplayName("正常 token：同一个 userId 校验通过")
    void acceptsValidToken() {
        String token = HmacTokenAuthenticator.issue("alice", future(300), SECRET);

        assertTrue(authenticator.authenticate("alice", token).accepted());
    }

    @Test
    @DisplayName("越权防线：alice 的 token 拿去当 bob 用，必须被拒")
    void rejectsTokenReplayedForAnotherUser() {
        String token = HmacTokenAuthenticator.issue("alice", future(300), SECRET);

        AuthVerdict verdict = authenticator.authenticate("bob", token);

        assertFalse(verdict.accepted());
        assertEquals(AuthVerdict.BAD_SIGNATURE, verdict.reason());
    }

    @Test
    @DisplayName("过期 token 被拒（且原因与签名错误区分开，便于排障）")
    void rejectsExpiredToken() {
        String token = HmacTokenAuthenticator.issue("alice", Instant.now().getEpochSecond() - 1, SECRET);

        AuthVerdict verdict = authenticator.authenticate("alice", token);

        assertEquals(AuthVerdict.EXPIRED_TOKEN, verdict.reason());
    }

    @Test
    @DisplayName("改动过期时间会破签名（不能靠延长过期时间续命）")
    void rejectsTamperedExpiry() {
        String token = HmacTokenAuthenticator.issue("alice", future(60), SECRET);
        int first = token.indexOf('.');
        int second = token.indexOf('.', first + 1);
        String tampered = "v1." + future(86400) + token.substring(second);

        assertEquals(AuthVerdict.BAD_SIGNATURE, authenticator.authenticate("alice", tampered).reason());
    }

    @Test
    @DisplayName("用别的密钥签发 -> 签名不匹配")
    void rejectsTokenSignedWithAnotherSecret() {
        String forged = HmacTokenAuthenticator.issue("alice", future(300), "attacker-secret");

        assertEquals(AuthVerdict.BAD_SIGNATURE, authenticator.authenticate("alice", forged).reason());
    }

    @Test
    @DisplayName("缺失/格式错误的 token 有明确原因码（不含 token 内容，避免泄漏与 tag 基数爆炸）")
    void reportsMissingAndMalformedToken() {
        assertEquals(AuthVerdict.MISSING_TOKEN, authenticator.authenticate("alice", null).reason());
        assertEquals(AuthVerdict.MISSING_TOKEN, authenticator.authenticate("alice", "  ").reason());
        assertEquals(AuthVerdict.MALFORMED_TOKEN, authenticator.authenticate("alice", "abc").reason());
        assertEquals(AuthVerdict.MALFORMED_TOKEN, authenticator.authenticate("alice", "v2.1.2.3").reason());
        assertEquals(AuthVerdict.MALFORMED_TOKEN, authenticator.authenticate("alice", "v1.notanumber.sig").reason());
        assertEquals(AuthVerdict.MISSING_USER, authenticator.authenticate(null, "v1.1.sig").reason());
    }

    @Test
    @DisplayName("空 secret 直接拒绝构造（配错要炸在启动时，而不是静默放行）")
    void rejectsBlankSecret() {
        assertThrowsIllegalArgument(() -> new HmacTokenAuthenticator(" "));
    }

    private static long future(long plusSeconds) {
        return Instant.now().getEpochSecond() + plusSeconds;
    }

    private static void assertThrowsIllegalArgument(Runnable action) {
        try {
            action.run();
            throw new AssertionError("预期抛出 IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            // ok
        }
    }
}
