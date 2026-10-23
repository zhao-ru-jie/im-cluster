package com.github.im.gateway.auth;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link AuthGuard} 三档开关的行为约定。
 *
 * <p>重点验证「warn 不会误伤」：这是灰度上线的前提——如果 warn 档也会拒绝，
 * 那它就没法用来观察影响面，只能直接上 enforce 赌一把。
 */
class AuthGuardTest {

    private static final String SECRET = "test-secret-please-change";

    @Test
    @DisplayName("off：没带 token 也放行（保持既有本地联调体验）")
    void offModeAllowsEverything() {
        AuthGuard guard = new AuthGuard("off", "", null);

        assertTrue(guard.check("alice", null).accepted());
        assertTrue(guard.check("alice", "garbage").accepted());
        assertEquals(AuthMode.OFF, guard.mode());
    }

    @Test
    @DisplayName("warn：坏 token 放行但记原因（上线前观察用，不得误伤）")
    void warnModeObservesButAllows() {
        AuthGuard guard = new AuthGuard("warn", SECRET, null);

        assertTrue(guard.check("alice", "garbage").accepted(), "warn 档不应拒绝连接");
        assertTrue(guard.check("alice", null).accepted(), "warn 档不应拒绝连接");
        assertEquals(AuthMode.WARN, guard.mode());
    }

    @Test
    @DisplayName("enforce：坏 token 拒绝并给出原因，好 token 放行")
    void enforceModeBlocksInvalidTokens() {
        AuthGuard guard = new AuthGuard("enforce", SECRET, null);
        String good = HmacTokenAuthenticator.issue("alice", expireIn(300), SECRET);

        assertTrue(guard.check("alice", good).accepted());

        AuthVerdict missing = guard.check("alice", null);
        assertEquals(false, missing.accepted());
        assertEquals(AuthVerdict.MISSING_TOKEN, missing.reason());

        AuthVerdict replay = guard.check("bob", good);
        assertEquals(false, replay.accepted());
        assertEquals(AuthVerdict.BAD_SIGNATURE, replay.reason());
    }

    @Test
    @DisplayName("配成 warn/enforce 却没配 secret：启动即失败（不能出现「以为开了其实没开」）")
    void requiresSecretWhenEnabled() {
        assertThrows(IllegalStateException.class, () -> new AuthGuard("enforce", "", null));
        assertThrows(IllegalStateException.class, () -> new AuthGuard("warn", "  ", null));
    }

    @Test
    @DisplayName("非法 mode 值启动即失败")
    void rejectsUnknownMode() {
        assertThrows(IllegalArgumentException.class, () -> new AuthGuard("maybe", SECRET, null));
    }

    private static long expireIn(long seconds) {
        return java.time.Instant.now().getEpochSecond() + seconds;
    }
}
