package com.github.im.storage.jdbc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.im.storage.jdbc.entity.MessageDeleteEntity;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * {@code im_message_delete} 的 Mapper。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Mapper
public interface MessageDeleteMapper extends BaseMapper<MessageDeleteEntity> {

    /** 重复删除由 {@code uk_msg_delete} 静默跳过（删除是幂等动作，客户端可能重试） */
    @Insert("""
            INSERT IGNORE INTO im_message_delete (user_id, conversation_id, seq, created_at)
            VALUES (#{userId}, #{conversationId}, #{seq}, #{createdAt})
            """)
    int insertIgnore(@Param("userId") String userId,
                     @Param("conversationId") String conversationId,
                     @Param("seq") long seq,
                     @Param("createdAt") long createdAt);
}
