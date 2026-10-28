package com.github.im.storage.mongodb;

/**
 * MongoDB 集合名（单一出处）。
 *
 * <p>集合名只在这里写一次，实体上的 {@code @Document(collection = ...)} 引用它：
 * 散开写必然有一天「建索引的那个集合」和「读写的那个集合」拼写不一致——而这种错误的表现是
 * <b>永远查不到数据</b>，不报任何异常。集中在这里，改名时编译器会替你找出所有引用点。
 *
 * <p>命名刻意与 MySQL 形态的表名保持一致（{@code im_message} / {@code im_group} …）：
 * 两种介质之间做数据迁移时，脚本里不需要再维护一张「表名 ↔ 集合名」的对照表。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public final class MongoCollections {

    /** 会话消息，一条文档 = 一条已受理消息 */
    public static final String MESSAGE = "im_message";

    /** 用户级「本地删除」记录（与消息分开存，理由同 MySQL 形态的 im_message_delete） */
    public static final String MESSAGE_DELETE = "im_message_delete";

    /** 群资料（{@code _id} 就是 groupId） */
    public static final String GROUP = "im_group";

    /** 群成员（一条文档 = 群 × 人，角色与禁言也在这条文档上） */
    public static final String GROUP_MEMBER = "im_group_member";

    /** 关系链（好友双向两条、黑名单单向一条） */
    public static final String RELATION = "im_relation";

    private MongoCollections() {
    }
}
