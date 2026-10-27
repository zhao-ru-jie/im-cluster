package com.github.im.server.relation;

import java.util.Collection;
import java.util.List;

/**
 * 关系链存储端口（好友 / 黑名单）。
 *
 * <h2>为什么消息服务需要关系链</h2>
 *
 * <p>没有它的时候，单聊会话只是「两个 userId 排序拼接」这个字符串约定
 * （{@link com.github.im.contract.MessageEnvelope#singleConversationId}），
 * 意味着<b>任何人都能给任何人发消息</b>：骚扰、批量营销、按 userId 遍历探测活跃账号，
 * 全都只需要一次 {@code msg.send}。这类问题必须在<b>投递之前</b>拦，
 * 事后过滤等于消息已经进了对方设备。
 *
 * <h2>为什么没有「好友申请」两阶段</h2>
 *
 * <p>{@link #addFriends} 直接建立双向关系。真实产品里「申请 → 通知 → 同意」需要
 * 通知子系统（红点、未读数、跨端已处理状态），它属于<b>业务侧</b>；IM 侧只需要
 * 「关系成立之后」的那个事实。所以这里的定位是：<b>由接入方的好友/组织架构系统同步过来</b>，
 * 不做申请流。
 *
 * <h2>黑名单与好友的区别</h2>
 *
 * <p>好友是对称的（两侧集合都写），黑名单是单向的（只记「我拉黑了谁」），
 * 且<b>不通知被拉黑方</b>——能查到「你被谁拉黑了」本身就是信息泄露。
 * 判定可达性时必须两个方向都查。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public interface RelationRepository {

    /** 双方是否互为好友（任一方向不成立就是 false） */
    boolean areFriends(String a, String b);

    /**
     * {@code ownerId} 是否拉黑了 {@code otherId}。
     *
     * <p>注意参数语义：只回答「owner 拉黑了 other」这一个方向。
     * 判断「A 和 B 之间是否被拉黑隔断」要查两次（A→B、B→A）。
     */
    boolean isBlocked(String ownerId, String otherId);

    /** 批量建立双向好友关系（幂等） */
    void addFriends(String userId, Collection<String> friendIds);

    /** 解除双向好友关系（不在好友列表里时静默成功） */
    void removeFriend(String a, String b);

    /** 拉黑（幂等；不改变好友关系，是否同时删好友由业务侧决定） */
    void block(String ownerId, String otherId);

    /** 取消拉黑（幂等） */
    void unblock(String ownerId, String otherId);

    /** 好友列表（无序） */
    List<String> friendsOf(String userId);

    /** 黑名单列表（无序） */
    List<String> blockedOf(String userId);
}
