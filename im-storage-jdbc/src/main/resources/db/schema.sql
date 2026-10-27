-- IM 服务消息存储（MySQL 8.x）
-- 启用方式：im.storage=jdbc + im.db.url/username/password
-- 建表由 im.db.init-schema=true 触发；生产建议关掉，改由 Flyway/Liquibase 管理版本。
--
-- 从「server_msg_id = conversation_id#seq」升级到「UUIDv7 独立生成」时，已有库需要手工改一次
-- （CREATE TABLE IF NOT EXISTS 不会改已存在的表）：
--   ALTER TABLE im_message MODIFY server_msg_id VARCHAR(64) NOT NULL;
--   ALTER TABLE im_message ADD UNIQUE KEY uk_msg_server_id (server_msg_id);
-- 历史行的 ID 仍是旧的 conversation_id#seq 格式——不影响唯一性，无需回填。
--
-- 支持富媒体（content_type / attrs）时，已有库需要手工加两列：
--   ALTER TABLE im_message
--       ADD COLUMN content_type VARCHAR(24) NOT NULL DEFAULT 'TEXT' COMMENT '消息类型' AFTER created_at,
--       ADD COLUMN attrs TEXT COMMENT '媒体元数据 JSON' AFTER content_type;
-- 历史行默认是 TEXT，与旧客户端的解析行为一致。
--
-- 支持消息撤回（recalled_at）时再加一列：
--   ALTER TABLE im_message ADD COLUMN recalled_at BIGINT NULL COMMENT '撤回时间（毫秒）' AFTER attrs;
-- 历史行 NULL = 未撤回。

CREATE TABLE IF NOT EXISTS im_message
(
    id            BIGINT       NOT NULL AUTO_INCREMENT COMMENT '自增主键',
    conversation_id       VARCHAR(160) NOT NULL COMMENT '会话 ID，单聊 s:{a}:{b}，群聊 g:{groupId}',
    seq           BIGINT       NOT NULL COMMENT '会话内单调序号（保序与拉齐依据）',
    server_msg_id VARCHAR(64)  NOT NULL COMMENT '服务端消息 ID（UUIDv7，全局唯一；与 seq 解耦）',
    from_user     VARCHAR(64)  NOT NULL COMMENT '发送方 userId',
    to_user       VARCHAR(160) NOT NULL COMMENT '单聊接收方 / 群 ID',
    conv_type     VARCHAR(16)  NOT NULL COMMENT 'SINGLE / GROUP',
    client_msg_id VARCHAR(96)  NOT NULL COMMENT '客户端幂等键',
    body          MEDIUMTEXT COMMENT '业务消息体（网关透传，不解析）',
    created_at    BIGINT       NOT NULL COMMENT '服务端受理时间（毫秒）',
    content_type  VARCHAR(24)  NOT NULL DEFAULT 'TEXT' COMMENT '消息类型：TEXT/IMAGE/FILE/VOICE/VIDEO/CUSTOM',
    attrs         TEXT COMMENT '媒体元数据 JSON（url/size/name/duration/...），服务端不解析',
    recalled_at   BIGINT NULL COMMENT '撤回时间（毫秒），NULL = 未撤回',
    PRIMARY KEY (id),
    -- 幂等最终防线：Redis 幂等表过期后仍能拒绝重复落库
    UNIQUE KEY uk_msg_idem (from_user, client_msg_id),
    -- 保序 + 漫游拉取主索引
    UNIQUE KEY uk_msg_conv_seq (conversation_id, seq),
    -- 消息 ID 全局唯一：ID 由 UUIDv7 独立生成、不与 seq 绑定，这条索引是「生成器不出错」的断言
    UNIQUE KEY uk_msg_server_id (server_msg_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '会话消息表';

-- 群成员表：im.storage=jdbc 时承接 GroupMemberRepository
-- （默认 Redis 实现为 im:group:members:{groupId} 集合）
CREATE TABLE IF NOT EXISTS im_group_member
(
    id         BIGINT      NOT NULL AUTO_INCREMENT,
    group_id   VARCHAR(96) NOT NULL,
    user_id    VARCHAR(64) NOT NULL,
    role       VARCHAR(16) NOT NULL DEFAULT 'MEMBER' COMMENT 'OWNER / ADMIN / MEMBER',
    created_at BIGINT      NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_group_member (group_id, user_id),
    KEY idx_group_member_user (user_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '群成员表';

-- 群实体：im.storage=jdbc 时承接 GroupRepository（默认 Redis 实现是 im:group:{id}:info）
--
-- 在此之前「群」只是一个 ID + 一个成员集合，做不了群名、群主、公告、全员禁言这些
-- 最基础的治理动作。主键直接用 group_id（由建群接口生成的短随机串），
-- 不引入自增 ID：群 ID 会出现在会话 ID（g:{groupId}）里并落进所有消息行，
-- 换一次 ID 就要迁移全量数据。
CREATE TABLE IF NOT EXISTS im_group
(
    group_id     VARCHAR(64)   NOT NULL,
    name         VARCHAR(128)  NOT NULL,
    owner_id     VARCHAR(64)   NOT NULL COMMENT '群主：解散/转让的唯一权威',
    announcement VARCHAR(1024) NOT NULL DEFAULT '',
    max_members  INT           NOT NULL DEFAULT 200,
    created_at   BIGINT        NOT NULL,
    muted_all    TINYINT       NOT NULL DEFAULT 0 COMMENT '全员禁言：只有群主/管理员能发言',
    dismissed    TINYINT       NOT NULL DEFAULT 0 COMMENT '已解散（逻辑删除，历史仍可读）',
    PRIMARY KEY (group_id),
    KEY idx_group_owner (owner_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '群实体表';

-- 支持群角色与单人禁言时，已有库需要给成员表加一列（role 列早已存在）：
--   ALTER TABLE im_group_member
--       ADD COLUMN mute_until BIGINT NOT NULL DEFAULT 0 COMMENT '禁言到期时间（毫秒），0=未禁言';
-- 注意 role 的取值语义升级为 OWNER / ADMIN / MEMBER（历史行都是 MEMBER）。

-- 关系链表：im.storage=jdbc 时承接 RelationRepository（投递前准入的数据来源）
-- （默认 Redis 实现为 im:rel:friend:{userId} / im:rel:block:{userId} 两个集合）
--
-- 好友是双向的两行（A→B、B→A），黑名单是单向一行：
-- 这样「查询某人与某人是否好友/是否被拉黑」永远是 (owner, target) 主键上的点查，
-- 不需要 OR 条件或联合索引。写入放大 2 倍，换来热路径上的 O(1)。
CREATE TABLE IF NOT EXISTS im_relation
(
    id            BIGINT      NOT NULL AUTO_INCREMENT,
    owner_id      VARCHAR(64) NOT NULL COMMENT '关系主体',
    target_id     VARCHAR(64) NOT NULL COMMENT '关系对象',
    relation_type VARCHAR(16) NOT NULL COMMENT 'FRIEND（双向两行）/ BLOCK（单向一行）',
    created_at    BIGINT      NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_relation (owner_id, target_id, relation_type),
    KEY idx_relation_target (target_id)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '关系链表（好友 / 黑名单）';

-- 用户「本地删除」的消息：im.storage=jdbc 时承接 MessageRepository#markDeleted
-- （默认 Redis 实现为 im:msg:del:{userId}:{conversationId} 集合）
--
-- 为什么单独一张表而不是给 im_message 加一列：删除是**用户级**的（我删了对方还要看），
-- 一条消息可能被会话里的 N 个人各删一次，加列只能表达「有人删过」，表达不了「谁删的」。
-- 撤回则相反（会话级），所以它是 im_message 上的一列。
CREATE TABLE IF NOT EXISTS im_message_delete
(
    id              BIGINT      NOT NULL AUTO_INCREMENT,
    user_id         VARCHAR(64) NOT NULL COMMENT '谁删的',
    conversation_id VARCHAR(160) NOT NULL,
    seq             BIGINT      NOT NULL COMMENT '删掉的是这个会话里的第几号',
    created_at      BIGINT      NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_msg_delete (user_id, conversation_id, seq),
    KEY idx_msg_delete_conv (conversation_id, seq)
) ENGINE = InnoDB
  DEFAULT CHARSET = utf8mb4 COMMENT '用户本地删除记录';
