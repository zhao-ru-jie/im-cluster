package com.github.im.contract;

import com.github.im.proto.presence.v1.DeviceAck;
import com.github.im.proto.presence.v1.DeviceReport;
import com.github.im.proto.presence.v1.GetPresenceRequest;
import com.github.im.proto.presence.v1.GetPresenceResponse;
import com.github.im.proto.presence.v1.PresenceReport;
import com.github.im.proto.presence.v1.PresenceReportAck;
import com.github.im.proto.presence.v1.TypingRequest;
import com.github.im.proto.presence.v1.TypingResponse;

/**
 * 状态上报端口：长连接服务(im-gateway) → IM 业务服务(im-server)。
 *
 * <p>与 {@link MessageIngress} 并列而不是合并的理由见 {@code im_presence.proto} 头部：
 * <b>可靠性模型不同</b>。消息那条路是 at-least-once（落库、幂等、离线兜底），
 * 这条路是 best-effort（丢了就丢了，绝不落离线队列，也不需要客户端重试）。
 * 一旦合成一个端口，调用方就会用「消息」的方式处理「状态」。
 *
 * <p>三个方法都给默认实现，和 {@link MessageIngress} 同一个考虑：让只需要消息的实现
 * （测试替身、只关心收发的部署）不必被迫实现这些边角能力。默认实现是<b>静默成功</b>——
 * 状态上报失败本来就不该让调用方（以及连接）付出任何代价。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface PresencePort {

    /**
     * 转发一条「正在输入」状态。
     *
     * <p>实现约定：<b>不抛异常</b>。typing 是装饰性信息，让它把网关的上行串行队列
     * 拖住或断掉一条连接，是最典型的「拿主链路换装饰品」。
     */
    default TypingResponse reportTyping(TypingRequest request) {
        return TypingResponse.newBuilder().setCode(0).setMessage("ok").build();
    }

    /**
     * 批量查在线状态。
     *
     * <p>默认返回空列表（= 全部按「没有信息」处理），而不是「全部离线」：
     * 把「查不到」说成「离线」会让客户端把好友列表画成一片灰。
     */
    default GetPresenceResponse presence(GetPresenceRequest request) {
        return GetPresenceResponse.getDefaultInstance();
    }

    /**
     * 上报一个用户的在线状态边沿变化（首个设备上线 / 最后一个设备下线）。
     *
     * <p>由网关在连接生命周期里调用，IM 服务负责「推给谁」（好友）与「记什么」（lastSeen）。
     */
    default PresenceReportAck reportPresence(PresenceReport report) {
        return PresenceReportAck.newBuilder().setCode(0).setMessage("ok").build();
    }

    /**
     * 上报设备级登录/下线事件。
     *
     * <p>实现要负责两件事：更新设备台账（供「设备列表」查询），以及在新设备登录时
     * 通知这个账号的<b>其他</b>在线设备（多端登录通知）。
     *
     * <p>默认实现静默成功：没装这个能力的部署，设备列表会退化成「只有槽位、没有登录信息」，
     * 登录通知不发——两者都是装饰性信息，不该让连接建立失败。
     */
    default DeviceAck reportDevice(DeviceReport report) {
        return DeviceAck.newBuilder().setCode(0).setMessage("ok").build();
    }
}
