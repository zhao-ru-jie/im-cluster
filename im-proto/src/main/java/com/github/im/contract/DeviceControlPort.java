package com.github.im.contract;

import com.github.im.proto.push.v1.KickResult;
import com.github.im.proto.push.v1.KickTask;

/**
 * 连接控制端口：IM 服务 → 长连接服务（「踢下线」这类必须由网关动手的动作）。
 *
 * <h2>为什么不复用 {@link PushPort}</h2>
 *
 * <p>{@link PushPort} 的契约是「把这些帧推给这些 userId」，粒度只到用户，语义是<b>可丢</b>
 * （失败即转离线，客户端重连后按 seq 拉齐）。而踢下线是<b>不可丢</b>的：
 * 它要的结果是「这条连接现在就不在了」，推一帧让客户端自己断只能劝退守规矩的客户端，
 * token 被盗、协议被改的客户端会直接忽略——那等于没踢。
 *
 * <p>所以它是独立端口：调用方拿到的是「真的关掉了几条」，而不是「推到没推到」。
 *
 * <h2>和在线路由的关系</h2>
 *
 * <p>实现方（网关 / 远程实现）自己按路由表决定要打哪些实例；调用方（IM 服务）
 * 只表达意图：「把这个账号的这台设备踢掉」。返回 {@code kicked=0} 表示目标本来就不在线，
 * 这在业务上是正常结果，不是错误。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface DeviceControlPort {

    /**
     * 关闭指定用户的连接。
     *
     * @param task {@code deviceId} 空 = 该用户全部设备；{@code exceptDeviceId} = 要保留的设备
     *             （「退出其他设备」）；{@code frame} 为关闭前要下发的帧（可空）
     * @return 实际关闭的连接数与被关闭的设备；<b>永不抛异常</b>，失败返回 {@code kicked=0}
     */
    KickResult kick(KickTask task);

    /**
     * 这个部署是否真的能控制连接。
     *
     * <p>返回 {@code false} 时调用方应如实告诉调用者「踢下线不生效」（例如网关侧未装配），
     * 而不是回一个「成功」然后连接安好无损——那是最容易在排障时浪费时间的一种谎。
     */
    default boolean available() {
        return true;
    }
}
