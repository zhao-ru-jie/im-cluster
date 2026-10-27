package com.github.im.client;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 群查询 / 群变更的统一返回。
 *
 * <p>为什么把 {@code code}/{@code group}/{@code members}/{@code roles}/{@code mutes} 放在一起：
 * 服务端这几个接口的返回形状几乎一致（变化类接口只返回 {@code code+message+group}，
 * 查询类还会带成员与角色），拆成七八个类型只会让调用方在多个类型之间搬运同样的字段。
 * 用不到的字段为 {@code null}/{@code 0}，文档里已注明每个接口填哪些——
 * 三个集合字段例外：缺失时归一成<b>空集合</b>，调用方不用判空。
 *
 * @param code        0 = 成功；400/403/404/409/413 见服务端定义
 * @param message     失败原因
 * @param group       群快照（查询类接口才有）
 * @param memberCount 成员数（{@code GET /group/{id}} 有，{@code /members} 用 {@code members.size()}）
 * @param members     成员 userId 列表（{@code /members} 才有）
 * @param roles       userId → MEMBER / ADMIN / OWNER
 * @param mutes       userId → 解禁时刻（毫秒），只含仍在禁言期内的人
 * @author zhaorujie
 * @since 1.0.0
 */
public record GroupInfo(
        int code,
        String message,
        GroupView group,
        int memberCount,
        List<String> members,
        Map<String, String> roles,
        Map<String, Long> mutes) {

    /**
     * 把「服务端没给这几个集合」归一成空集合：变更类接口只回 {@code code+message+group}，
     * 这时 {@code members/roles/mutes} 反序列化出来是 null，调用方 {@code for} 一下就会 NPE。
     */
    public GroupInfo {
        members = members == null ? new ArrayList<>() : members;
        roles = roles == null ? new LinkedHashMap<>() : roles;
        mutes = mutes == null ? new LinkedHashMap<>() : mutes;
    }

    public boolean success() {
        return code == HttpResult.OK;
    }
}
