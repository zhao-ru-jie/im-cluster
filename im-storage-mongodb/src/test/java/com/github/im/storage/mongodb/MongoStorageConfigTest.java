package com.github.im.storage.mongodb;

import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRepository;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.relation.FriendRequestRepository;
import com.github.im.server.relation.RelationRepository;
import com.github.im.storage.mongodb.config.MongoStorageConfig;
import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.mongodb.core.MongoTemplate;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * 装配条件测试：{@code im.storage} 的取值真的决定「四个端口由谁实现」。
 *
 * <p>为什么用最小容器而不是 {@code @SpringBootTest}：本模块的装配只关心两件事——
 * 「条件命中时四个端口都在」和「没命中时一个都不建」。启动整个应用反而会把这层断言
 * 埋在几十个 Bean 里。客户端与模板用 mock（配置类上加了 {@code @ConditionalOnMissingBean}），
 * 于是这套断言不需要任何 Mongo 实例。
 *
 * <p>与 {@code RedisStorageConfig} / {@code JdbcStorageConfig} 的互斥由各自的
 * {@code havingValue} 保证，这里只验本模块一侧。
 */
class MongoStorageConfigTest {

    @Test
    @DisplayName("im.storage=mongodb → 全部端口都是 MongoDB 实现")
    void registersAllFourPortsWhenMongodb() {
        try (AnnotationConfigApplicationContext context = context(Map.of("im.storage", "mongodb"))) {
            assertTrue(context.getBean(MessageRepository.class) instanceof MongoMessageRepository);
            assertTrue(context.getBean(GroupRepository.class) instanceof MongoGroupRepository);
            assertTrue(context.getBean(GroupMemberRepository.class) instanceof MongoGroupMemberRepository);
            assertTrue(context.getBean(RelationRepository.class) instanceof MongoRelationRepository);
            assertTrue(context.getBean(FriendRequestRepository.class) instanceof MongoFriendRequestRepository,
                    "好友申请没有实现的话，RelationService 起不来——这个端口和关系边一样是必需的");
        }
    }

    @Test
    @DisplayName("宿主已提供 MongoClient/MongoTemplate 时不自建（测试与既有装配可复用）")
    void backsOffWhenClientAndTemplateExist() {
        try (AnnotationConfigApplicationContext context = context(Map.of("im.storage", "mongodb"))) {
            // 容器里只应该有测试注入的那两个，配置类不该再建一份（自建那份会去 ping）
            assertEquals(1, context.getBeansOfType(MongoClient.class).size());
            assertEquals(1, context.getBeansOfType(MongoTemplate.class).size());
        }
    }

    @Test
    @DisplayName("im.storage=redis / jdbc → 本模块一个端口都不建")
    void registersNothingForOtherMedia() {
        for (String storage : new String[]{"redis", "jdbc"}) {
            try (AnnotationConfigApplicationContext context = context(Map.of("im.storage", storage))) {
                assertEquals(0, context.getBeansOfType(MessageRepository.class).size(),
                        storage + " 形态下不该同时存在 MongoDB 实现，否则同一端口会有两个 Bean");
                assertEquals(0, context.getBeansOfType(GroupRepository.class).size());
                assertEquals(0, context.getBeansOfType(GroupMemberRepository.class).size());
                assertEquals(0, context.getBeansOfType(RelationRepository.class).size());
                assertEquals(0, context.getBeansOfType(FriendRequestRepository.class).size());
                // 容器里只该有测试注入的那一个 mock：配置类自己不该再建客户端（自建那份会去 ping）
                assertEquals(1, context.getBeansOfType(MongoClient.class).size(),
                        storage + " 形态下不该额外建 Mongo 客户端");
                assertTrue(!List.of(context.getBeanDefinitionNames()).contains("imMongoClient"),
                        storage + " 形态下不该注册 imMongoClient");
            }
        }
    }

    /**
     * 最小容器：注入 mock 的 {@link MongoClient} 与 {@link MongoTemplate}，再注册配置类。
     *
     * <p>两个 mock 是「不需要真实 Mongo」的关键：配置类的自建 Bean 会 {@code ping} 数据库，
     * 而这里要验的是装配条件，不是连通性。
     */
    private static AnnotationConfigApplicationContext context(Map<String, Object> properties) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.registerBean(MongoClient.class, () -> mock(MongoClient.class));
        context.registerBean(MongoTemplate.class, () -> mock(MongoTemplate.class));
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", properties));
        context.register(MongoStorageConfig.class);
        context.refresh();
        return context;
    }
}
