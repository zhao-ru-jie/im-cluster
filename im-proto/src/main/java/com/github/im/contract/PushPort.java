package com.github.im.contract;

import com.github.im.proto.push.v1.PushResult;
import com.github.im.proto.push.v1.PushTask;

/**
 * 下行端口：IM 业务服务(im-server) → 长连接服务(im-gateway)。
 *
 * <p>IM 服务只表达「这条帧要推给哪些 userId」，完全不感知：
 * 目标用户连在哪个网关实例、本机有没有它的会话、要不要走 gRPC。
 * 这些「连接侧知识」全部由网关模块的 {@link PushPort} 实现自己解决。
 *
 * <p>返回值里的 {@code failed_user_ids} 是 IM 服务判定「需要转离线存储」的唯一依据，
 * 因此实现必须如实上报失败，不能吞掉。
 */
public interface PushPort {

    /**
     * 推送一批下行帧。
     *
     * @return 成功/失败的用户列表；实现异常时应返回全部失败而不是抛出，
     *         让 IM 服务统一走离线兜底（可丢实时性，不可丢消息）。
     */
    PushResult push(PushTask task);
}
