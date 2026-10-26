package com.github.im.gateway.node;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link GatewayNacosDiscovery#cleanupOwnerOf} 的选主约定。
 *
 * <p>重点验证「3 台网关挂 1 台时只有 1 台去扫 Redis」：Nacos 会把实例变更推给每个订阅者，
 * 若各实例算出的 owner 不一致，N-1 份 O(全部在线用户) 的全库 SCAN 会同时打到 Redis，
 * 而这正好发生在「刚掉一个节点、客户端批量重连」的最忙时刻。
 */
class GatewayNacosDiscoveryOwnerTest {

    private static final List<String> ALIVE_SORTED =
            List.of("10.0.0.1:9090", "10.0.0.2:9090", "10.0.0.3:9090");

    @Test
    @DisplayName("同一组输入在任何实例上算出同一个 owner（与通知里的实例顺序无关）")
    void ownerIsStableAcrossInstances() {
        String dead = "10.0.0.9:9090";
        String expected = GatewayNacosDiscovery.cleanupOwnerOf(dead, ALIVE_SORTED);

        // 各实例收到的 NamingEvent 里实例顺序不保证一致，各自排序后必须收敛到同一个结果
        List<List<String>> notifications = List.of(
                List.of("10.0.0.3:9090", "10.0.0.1:9090", "10.0.0.2:9090"),
                List.of("10.0.0.2:9090", "10.0.0.3:9090", "10.0.0.1:9090"),
                List.of("10.0.0.1:9090", "10.0.0.2:9090", "10.0.0.3:9090"));
        for (List<String> notification : notifications) {
            List<String> sorted = notification.stream().sorted().toList();
            assertEquals(expected, GatewayNacosDiscovery.cleanupOwnerOf(dead, sorted),
                    "各实例必须选出同一个 owner，否则会重复全库扫描");
        }
    }

    @Test
    @DisplayName("owner 一定落在健康实例里（死实例不可能自己清理自己）")
    void ownerIsAlwaysAlive() {
        String owner = GatewayNacosDiscovery.cleanupOwnerOf("10.0.0.9:9090", ALIVE_SORTED);
        assertTrue(ALIVE_SORTED.contains(owner), "owner 必须来自健康实例列表，实际为 " + owner);
    }

    @Test
    @DisplayName("先后挂掉不同实例时，清理任务会分摊到不同活实例上")
    void ownerSpreadsAcrossDeaths() {
        Set<String> owners = new HashSet<>();
        for (int i = 0; i < 60; i++) {
            owners.add(GatewayNacosDiscovery.cleanupOwnerOf("10.0.0.9:" + (9000 + i), ALIVE_SORTED));
        }
        assertEquals(ALIVE_SORTED.size(), owners.size(), "死实例 ID 不同时应能覆盖所有活实例，不能永远压同一台");
    }

    @Test
    @DisplayName("只剩一台活实例时由它兜住（此时没有第二台可以分担）")
    void singleAliveTakesAll() {
        List<String> only = List.of("10.0.0.1:9090");
        assertEquals("10.0.0.1:9090", GatewayNacosDiscovery.cleanupOwnerOf("10.0.0.9:9090", only));
    }
}
