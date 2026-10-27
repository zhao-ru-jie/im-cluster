package com.github.im.server.relation;

import com.github.im.contract.MessageEnvelope;
import com.github.im.server.group.GroupMemberRepository;
import com.github.im.server.group.GroupService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.util.Locale;

/**
 * 投递前准入（ACL）：一条消息<b>能不能发</b>。
 *
 * <h2>为什么必须在投递前</h2>
 *
 * <p>事后过滤等于消息已经进了对方设备——骚扰信息的第一屏已经显示出来了。
 * 而这里拦下的消息<b>不占 seq、不落库、不进离线队列</b>（调用点在发号之前），
 * 所以「被拒绝」在服务端是零成本、在客户端是一个明确的 403，
 * 不给「试探对方是否在线」留任何旁路。
 *
 * <h2>三档开关（与 {@code im.auth.mode} 同一套风格）</h2>
 *
 * <table border="1">
 *   <caption>im.acl.mode</caption>
 *   <tr><th>值</th><th>行为</th><th>用途</th></tr>
 *   <tr><td>{@code off}（默认）</td><td>不校验，<b>零额外往返</b></td><td>本地联调、纯内网</td></tr>
 *   <tr><td>{@code warn}</td><td>校验并记日志，但一律放行</td><td>上线前观察「本该拦掉多少」</td></tr>
 *   <tr><td>{@code enforce}</td><td>校验失败即 403</td><td>正式生效</td></tr>
 * </table>
 *
 * <p>默认 {@code off} 是刻意的：开启后每条单聊消息要多 2~4 次 Redis 往返
 * （黑名单双向 + 可选的好友判定），每条群消息要多 1 次 {@code SISMEMBER}。
 * 这个开销应该由部署方在「知道自己在为什么付费」的前提下打开，
 * 而不是让所有人默认承担。
 *
 * <h2>规则</h2>
 *
 * <ul>
 *   <li><b>群聊</b>：发送者必须是群成员。这是最容易漏、后果最直接的一条——
 *       群 ID 往往可枚举（{@code g:1}、{@code g:sales}），不校验等于任何人可以往任何群发言；</li>
 *   <li><b>单聊 · 黑名单</b>：任一方向拉黑都拒绝。拉黑是「我不想再来往」，
 *       只拦单向会让被拉黑方继续单方面发送（虽然对方看不到，但服务端在为骚扰者买单）；</li>
 *   <li><b>单聊 · 好友</b>：可选（{@code im.acl.require-friend=true}）。开放社交产品不要求，
 *       企业/内部 IM 通常要求——此时关系链由组织架构同步过来；</li>
 *   <li><b>发给自己</b>：始终允许（文件传输助手这类场景）。</li>
 * </ul>
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Component
public class MessageAcl {

    private static final Logger log = LoggerFactory.getLogger(MessageAcl.class);

    /** 与网关的 error 帧语义对齐：403 = 没有权限，客户端不该重试 */
    public static final int CODE_FORBIDDEN = 403;

    public enum Mode {
        OFF, WARN, ENFORCE;

        static Mode parse(String raw) {
            if (!StringUtils.hasText(raw)) {
                return OFF;
            }
            try {
                return valueOf(raw.trim().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(
                        "im.acl.mode 只支持 off / warn / enforce，当前值: " + raw, e);
            }
        }
    }

    /**
     * 判定结果。
     *
     * @param allowed 是否放行
     * @param code    拒绝时回给客户端的错误码（{@link #CODE_FORBIDDEN}）
     * @param reason  拒绝原因（会出现在 error 帧的 message 里，所以不能带对方的隐私信息）
     */
    public record Verdict(boolean allowed, int code, String reason) {

        static final Verdict ALLOW = new Verdict(true, 0, "ok");

        static Verdict deny(String reason) {
            return new Verdict(false, CODE_FORBIDDEN, reason);
        }
    }

    private final RelationRepository relations;
    private final GroupMemberRepository groupMembers;
    /** 群治理规则（成员/禁言/解散）的统一来源；为空时退化成「只查是不是成员」 */
    private final GroupService groupService;
    private final Mode mode;
    private final boolean requireFriend;

    public MessageAcl(ObjectProvider<RelationRepository> relationsProvider,
                      GroupMemberRepository groupMembers,
                      ObjectProvider<GroupService> groupServiceProvider,
                      @Value("${im.acl.mode:off}") String mode,
                      @Value("${im.acl.require-friend:false}") boolean requireFriend) {
        this.relations = relationsProvider == null ? null : relationsProvider.getIfAvailable();
        this.groupMembers = groupMembers;
        this.groupService = groupServiceProvider == null ? null : groupServiceProvider.getIfAvailable();
        this.mode = Mode.parse(mode);
        this.requireFriend = requireFriend;
        if (requireFriend && relations == null) {
            // 配置了「必须好友」但没有任何关系链实现（例如 im.storage=jdbc 却没建 im_relation 表）：
            // 只能降级成「只查群成员」，并明确告警——静默放行比报错更难查
            log.error("[acl] im.acl.require-friend=true 但未装配 RelationRepository，好友校验被跳过");
        }
        log.info("[acl] 投递前准入 {}，好友校验 {}", this.mode,
                requireFriend ? "开启（非好友不能发单聊）" : "关闭");
    }

    /**
     * 判定一条消息是否允许投递。
     *
     * @return {@code allowed=false} 时载荷里带错误码与原因；{@code mode=off} 时恒为放行且不访问任何存储
     */
    public Verdict check(MessageEnvelope envelope) {
        if (mode == Mode.OFF || envelope == null) {
            return Verdict.ALLOW;
        }
        Verdict verdict;
        try {
            verdict = judge(envelope);
        } catch (RuntimeException e) {
            // 准入依赖（Redis / 群成员表）不可用时倾向放行：宁可漏拦一条，
            // 也不要在存储抖动时让全站发不出消息——这与「鉴权失败即拒绝」不同，
            // 后者是安全问题，这里只是策略问题
            log.warn("[acl] 准入判定失败，本条放行 from={} to={} err={}",
                    envelope.from(), envelope.to(), e.getMessage());
            return Verdict.ALLOW;
        }
        if (verdict.allowed()) {
            return verdict;
        }
        if (mode == Mode.WARN) {
            log.warn("[acl] 本该拒绝但放行（warn 模式）conv={} from={} to={} reason={}",
                    envelope.conversationId(), envelope.from(), envelope.to(), verdict.reason());
            return Verdict.ALLOW;
        }
        log.debug("[acl] 拒绝 conv={} from={} to={} reason={}",
                envelope.conversationId(), envelope.from(), envelope.to(), verdict.reason());
        return verdict;
    }

    private Verdict judge(MessageEnvelope envelope) {
        String from = envelope.from();
        String to = envelope.to();
        if (envelope.isGroup()) {
            // 群维度复用治理层的规则（成员 / 个人禁言 / 全员禁言 / 已解散），
            // 不在两处各写一套——两套规则漂移的典型症状是「接口拦了但消息能发」
            if (groupService != null) {
                String reason = groupService.canSpeak(to, from);
                return reason == null ? Verdict.ALLOW : Verdict.deny(reason);
            }
            return groupMembers.isMember(to, from)
                    ? Verdict.ALLOW
                    : Verdict.deny("sender is not a member of group " + to);
        }
        if (from.equals(to)) {
            return Verdict.ALLOW;
        }
        if (relations == null) {
            // 没有关系链实现：只能放弃单聊维度的校验（群维度不依赖它，仍然生效）
            return Verdict.ALLOW;
        }
        // 两个方向各查一次：黑名单只存「我拉黑了谁」，单向查询永远不足以判断可达性
        if (relations.isBlocked(to, from)) {
            return Verdict.deny("blocked by receiver");
        }
        if (relations.isBlocked(from, to)) {
            return Verdict.deny("receiver is blocked by sender");
        }
        if (requireFriend && !relations.areFriends(from, to)) {
            return Verdict.deny("not friends");
        }
        return Verdict.ALLOW;
    }

    /** 当前模式（运维/测试观测用） */
    public Mode mode() {
        return mode;
    }
}
