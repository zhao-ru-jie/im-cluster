package com.github.im.server.group;

/**
 * 一张群邀请码（邀请链接 / 二维码背后就是它）。
 *
 * <h2>为什么「链接」和「二维码」是同一个东西</h2>
 *
 * <p>扫码入群在数据上不是一种新机制：二维码只是把 {@code im://group/join?code=xxx}
 * 这条链接画成图。所以服务端只维护「码」这一种实体，链接由客户端拼、二维码由客户端生成。
 * 加一个「扫码表」等于给同一个东西做两份存储，且两份会不一致。
 *
 * <h2>字段为什么这样定</h2>
 *
 * <ul>
 *   <li>{@code code} 是随机短码而不是 {@code groupId}：链接会被转发到群外，
 *       用群 ID 当邀请码等于把「不可枚举的群 ID」公开出去；</li>
 *   <li>{@code expiresAt} 与 {@code maxUses} 可同时生效，任一触顶即失效——这是拉人链接
 *       最常见的两个诉求（限时、限量）；</li>
 *   <li>{@code uses} 必须由存储侧<b>原子自增</b>（Redis Lua），否则并发扫码会把
 *       {@code maxUses=1} 的链接用成多次；</li>
 *   <li>{@code approvalRequired}：链接默认直接入群（拉人场景），但公开群/大群常需要
 *       管理员过一道，此时扫码只是「提交一条申请」。</li>
 * </ul>
 *
 * @param code             邀请码（随机 base36）
 * @param groupId          目标群
 * @param inviterId        创建者（留痕：出问题能定位是谁把群放出去的）
 * @param createdAt        创建时间
 * @param expiresAt        到期时间；{@code 0} 表示长期有效
 * @param maxUses          可用次数上限；{@code 0} 表示不限
 * @param uses             已使用次数
 * @param approvalRequired 使用后是否需要管理员审批
 * @author zhaorujie
 * @since 1.0.0
 */
public record GroupInvite(String code,
                          String groupId,
                          String inviterId,
                          long createdAt,
                          long expiresAt,
                          int maxUses,
                          int uses,
                          boolean approvalRequired) {

    /** 码长度：10 位 base36 ≈ 3.6e15；配合「限次/限时」与调用侧限流，扫不出来也猜不中 */
    public static final int CODE_LENGTH = 10;

    public GroupInvite {
        if (expiresAt < 0) {
            expiresAt = 0;
        }
        if (maxUses < 0) {
            maxUses = 0;
        }
        if (uses < 0) {
            uses = 0;
        }
    }

    public boolean expired(long now) {
        return expiresAt > 0 && now >= expiresAt;
    }

    public boolean exhausted() {
        return maxUses > 0 && uses >= maxUses;
    }

    /** 现在还能不能被使用（不含「是否已是成员」这类调用方判断） */
    public boolean usable(long now) {
        return !expired(now) && !exhausted();
    }

    public GroupInvite withUses(int newUses) {
        return new GroupInvite(code, groupId, inviterId, createdAt, expiresAt, maxUses, newUses, approvalRequired);
    }

    /** 邀请链接：客户端可以直接展示/复制，也可以据此生成二维码 */
    public String link(String base) {
        String prefix = base == null || base.isBlank() ? "im://group/join?code=" : base;
        return prefix + code;
    }
}
