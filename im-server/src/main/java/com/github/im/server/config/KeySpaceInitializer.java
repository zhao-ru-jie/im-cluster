package com.github.im.server.config;

import com.github.im.contract.RedisKeys;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 启动时把租户维度灌进 key 空间（{@code im.tenant.app-id}）。
 *
 * <p>为什么是「启动时设置一次」而不是每次构造 key 都读配置：
 * {@link RedisKeys} 是纯静态契约（网关与 IM 服务共用、且在测试里被直接调用），
 * 让它在构造 key 时依赖 Spring 配置就得把整条调用链改成注入式——
 * 而这个值是<b>进程级不变</b>的（多租户按实例隔离，见 {@link RedisKeys} 类注释）。
 *
 * <p>为什么网关侧也有一份同样的初始化：{@code im-proto} 不依赖 Spring，
 * 而 im-server 与 im-gateway 之间也没有依赖关系（只有 {@code im-app} 单体同时包含两者）。
 * 重复设置同一个值是幂等的，单体形态下多跑一次没有副作用。
 *
 * <p><b>bean 名必须显式指定</b>：网关侧那个类同名，默认 bean 名（类简名首字母小写）在
 * {@code im-app} 单体里会撞成 {@code ConflictingBeanDefinitionException}，
 * 表现为「两个模块单独都能起，合起来起不来」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component("serverKeySpaceInitializer")
public class KeySpaceInitializer {

    private static final Logger log = LoggerFactory.getLogger(KeySpaceInitializer.class);

    private final String appId;

    public KeySpaceInitializer(@Value("${im.tenant.app-id:}") String appId) {
        this.appId = appId;
    }

    @PostConstruct
    public void init() {
        RedisKeys.configure(appId);
        log.info("[keyspace] Redis key 根前缀 = {}（租户 {}）", RedisKeys.root(),
                appId == null || appId.isBlank() ? "默认" : appId);
    }
}
