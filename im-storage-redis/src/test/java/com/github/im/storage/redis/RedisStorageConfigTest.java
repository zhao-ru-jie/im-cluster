package com.github.im.storage.redis;

import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupRepository;
import com.github.im.server.message.MessageRepository;
import com.github.im.server.relation.RelationRepository;
import com.github.im.storage.redis.config.RedisStorageConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.data.redis.core.StringRedisTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

/**
 * 装配条件测试：抽出独立模块后，{@code im.storage} 的默认值必须仍然落在 Redis 上。
 *
 * <p>这一步是最容易被抽离动作搞坏的：实现类从 im-server 搬走以后，只要没人 {@code @Bean} 它，
 * 「不配 im.storage 直接启动」就会报「找不到 MessageRepository」。所以这里用最小容器
 * （非 Web、非 Netty）把三条分支钉住。
 *
 * <p>与 {@code JdbcStorageConfig} 的互斥由 {@code havingValue} 保证，这里只验本模块一侧。
 */
class RedisStorageConfigTest {

    @Test
    @DisplayName("不配 im.storage → 缺省装配 Redis 版 MessageRepository")
    void registersRedisRepositoryWhenPropertyMissing() {
        try (AnnotationConfigApplicationContext context = context(Map.of())) {
            assertTrue(context.getBean(MessageRepository.class) instanceof RedisMessageRepository,
                    "缺省形态必须是 Redis 实现");
        }
    }

    @Test
    @DisplayName("im.storage=redis → 同上（显式声明也不该变）")
    void registersRedisRepositoryWhenExplicitRedis() {
        try (AnnotationConfigApplicationContext context = context(Map.of("im.storage", "redis"))) {
            assertTrue(context.getBean(MessageRepository.class) instanceof RedisMessageRepository);
        }
    }

    @Test
    @DisplayName("缺省 → 四个存储端口都是 Redis 实现（与 im-storage-jdbc 端口一一对应）")
    void registersAllFourPortsByDefault() {
        try (AnnotationConfigApplicationContext context = context(Map.of())) {
            assertTrue(context.getBean(GroupRepository.class) instanceof RedisGroupRepository);
            assertTrue(context.getBean(GroupMemberRepository.class) instanceof RedisGroupMemberRepository);
            assertTrue(context.getBean(RelationRepository.class) instanceof RedisRelationRepository);
        }
    }

    @Test
    @DisplayName("im.storage=jdbc → 本模块一个 Bean 都不建（交给 im-storage-jdbc）")
    void registersNothingWhenJdbc() {
        try (AnnotationConfigApplicationContext context = context(Map.of("im.storage", "jdbc"))) {
            assertEquals(0, context.getBeansOfType(MessageRepository.class).size(),
                    "jdbc 形态下不该同时存在 Redis 实现，否则同一端口会有两个 Bean");
            assertEquals(0, context.getBeansOfType(GroupRepository.class).size());
            assertEquals(0, context.getBeansOfType(GroupMemberRepository.class).size());
            assertEquals(0, context.getBeansOfType(RelationRepository.class).size());
            List<String> beanNames = List.of(context.getBeanDefinitionNames());
            assertTrue(beanNames.stream().noneMatch(name -> name.contains("messageRepository")
                    || name.contains("groupRepository") || name.contains("relationRepository")));
        }
    }

    /**
     * 最小容器：只注册配置类与它的两个依赖。
     *
     * <p>{@link StringRedisTemplate} 用 mock 而不是 {@code new}：容器会回调
     * {@code afterPropertiesSet()}，真实例在没有 connectionFactory 时会直接抛异常，
     * 而这里要验的是「装配条件」，不是连接。
     */
    private static AnnotationConfigApplicationContext context(Map<String, Object> properties) {
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
        context.registerBean(StringRedisTemplate.class, () -> mock(StringRedisTemplate.class));
        // 写成显式 lambda：ObjectMapper 还有 (MapperBuilder) 构造器，方法引用会与
        // registerBean(Class, BeanDefinitionCustomizer...) 重载撞在一起
        context.registerBean(ObjectMapper.class, () -> new ObjectMapper());
        context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test", properties));
        context.register(RedisStorageConfig.class);
        context.refresh();
        return context;
    }
}
