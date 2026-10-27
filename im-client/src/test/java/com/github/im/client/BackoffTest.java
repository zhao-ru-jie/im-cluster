package com.github.im.client;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 退避语义的三条硬约定：<b>上限封顶</b>、<b>有下界</b>、<b>抖动落在区间内</b>。
 *
 * <p>为什么值得单测：退避算错不会报错，只会在线上表现成「重连风暴」或「半天连不回来」，
 * 而这两种症状都很难在没有指标的情况下定位到「其实是一行移位写错了」。
 */
class BackoffTest {

    @Test
    @DisplayName("上限按 2^n 增长并在 maxMs 处封顶，不会溢出成负数")
    void ceilingGrowsThenCaps() {
        Backoff backoff = new Backoff(500, 30_000, new Random(1));

        assertEquals(500, backoff.ceilingMs(0));
        assertEquals(1_000, backoff.ceilingMs(1));
        assertEquals(2_000, backoff.ceilingMs(2));
        assertEquals(30_000, backoff.ceilingMs(7));
        // 关键：attempt 很大时仍然是正数（base << n 会先溢出再被截断成负数）
        assertEquals(30_000, backoff.ceilingMs(64));
        assertEquals(30_000, backoff.ceilingMs(Integer.MAX_VALUE));
    }

    @Test
    @DisplayName("每次延迟都落在 [上限/5, 上限] 之间：20% 下界保证「真的退避了」")
    void delayStaysWithinFloorAndCeiling() {
        Backoff backoff = new Backoff(100, 10_000, new Random(7));

        for (int attempt = 0; attempt < 12; attempt++) {
            long ceiling = backoff.ceilingMs(attempt);
            long floor = ceiling / 5;
            for (int i = 0; i < 50; i++) {
                long delay = backoff.delayMs(attempt);
                assertTrue(delay >= floor && delay <= ceiling,
                        "attempt=" + attempt + " delay=" + delay + " 越界 [" + floor + ',' + ceiling + ']');
            }
        }
    }

    @Test
    @DisplayName("抖动确实在动：同一个 attempt 多次取值不全相同")
    void delayIsJittered() {
        Backoff backoff = new Backoff(1_000, 30_000, new Random(11));
        long first = backoff.delayMs(4);
        boolean sawDifference = false;
        for (int i = 0; i < 20 && !sawDifference; i++) {
            sawDifference = backoff.delayMs(4) != first;
        }
        assertTrue(sawDifference, "延迟没有抖动，所有客户端会在同一毫秒重连");
    }

    @Test
    @DisplayName("非法参数直接拒绝，而不是悄悄归一化")
    void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new Backoff(0, 100));
        assertThrows(IllegalArgumentException.class, () -> new Backoff(100, -1));
        assertThrows(IllegalArgumentException.class, () -> new Backoff(5_000, 1_000));
    }

    @Test
    @DisplayName("重连策略：migrate 用服务端提示打底，抖动不超过它的一半且不超过 maxMs")
    void migrateDelayHonoursServerHint() {
        ReconnectPolicy policy = new ReconnectPolicy(500, 30_000, 30_000, new Random(3));

        long hinted = policy.delayAfterMigrate(1_500);
        assertTrue(hinted >= 1_500 && hinted <= 1_500 + 750, "延迟 " + hinted + " 应在 [1500, 2250]");

        // 服务端给了一个巨大的提示值：抖动被 maxMs 截断，但不会无限放大
        long huge = policy.delayAfterMigrate(600_000);
        assertTrue(huge >= 600_000 && huge <= 600_000 + 30_000, "延迟 " + huge + " 应被 maxMs 截断");

        // 没给提示（<=0）时退回普通退避
        long noHint = policy.delayAfterMigrate(0);
        assertTrue(noHint >= 100 && noHint <= 500, "无提示时应走第 0 档退避，实际 " + noHint);
    }

    @Test
    @DisplayName("稳定连接判定：活够 stableAfterMs 才算通，秒断要持续退避")
    void stableConnectionResetsAttempts() {
        ReconnectPolicy policy = new ReconnectPolicy(500, 30_000, 10_000, new Random(5));

        assertTrue(policy.isStable(10_000));
        assertTrue(policy.isStable(60_000));
        assertTrue(!policy.isStable(9_999));
        assertTrue(!policy.isStable(0));
    }
}
