-- IM 服务消息存储（MySQL 8.x）
-- 启用方式：im.storage=jdbc + im.db.url/username/password
-- 建表由 im.db.init-schema=true 触发；生产建议关掉，改由 Flyway/Liquibase 管理版本。
--
-- 从「server_msg_id = conversation_id#seq」升级到「UUIDv7 独立生成」时，已有库需要手工改一次
-- （CREATE TABLE IF NOT EXISTS 不会改已存在的表）：
--   ALTER TABLE im_message MODIFY server_msg_id VARCHAR(64) NOT NULL;
--   ALTER TABLE im_message ADD UNIQUE KEY uk_msg_server_id (server_msg_id);
-- 历史行的 ID 仍是旧的 conversation_id#seq 格式——不影响唯一性，无需回填。

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
