package com.github.im.rpc.common;

/**
 * 把「网关实例 ID」解析成「可 dial 的 gRPC 目标地址」。
 *
 * <p>默认实现 {@link #DIRECT} 直接把实例 ID 当地址用——因为实例 ID 本身就被定义为
 * {@code host:port}（见 {@code NodeAddressResolver}）。当实例 ID 不再是可达地址
 * （例如改用 StatefulSet 的 POD_NAME 作实例 ID）时，可在部署入口注入一个
 * Nacos/服务发现实现，替换本接口即可，推送逻辑无需改动。
 */
@FunctionalInterface
public interface AddressResolver {

    /** 实例 ID 即地址：零配置、零额外依赖，单机 / 同集群部署够用 */
    AddressResolver DIRECT = instanceId -> instanceId;

    String resolve(String instanceId);
}
