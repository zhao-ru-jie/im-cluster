package com.github.im.gateway.session;

import com.github.im.contract.RedisKeys;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * 启动时把租户维度灌进 key 空间（{@code im.tenant.app-id}）。
 *
 * <p>长连接服务必须与 IM 服务算出<b>完全一样</b>的 key：在线路由（{@code im:gateway:user:{u}}）
 * 是网关写、IM 服务读的，两边任一处前缀不同，下行推送就会静默走「用户不在线」分支——
 * 这类故障没有任何报错，只有「消息明明发出去了但对方没收到」。
 *
 * <p>幂等，且与 {@code im-server} 里的同名类互不影响——前提是 bean 名不同：
 * 默认 bean 名是类简名首字母小写，{@code im-app} 单体同时扫描两个模块时会撞成
 * {@code ConflictingBeanDefinitionException}（两个模块单独都能起，合起来起不来）。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component("gatewayKeySpaceInitializer")
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
