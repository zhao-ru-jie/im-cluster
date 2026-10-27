package com.github.im.storage.jdbc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.im.storage.jdbc.entity.MessageEntity;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

/**
 * {@code im_message} 的 Mapper。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Mapper
public interface MessageMapper extends BaseMapper<MessageEntity> {

    /**
     * 撤回一条消息：{@code recalled_at IS NULL} 是幂等条件——
     * 已撤回的行不会被再次更新，影响行数因此能区分「首次撤回」与「重复撤回」，
     * 调用方据此决定要不要广播变更帧。
     */
    @Update("""
            UPDATE im_message SET recalled_at = #{recalledAt}
            WHERE conversation_id = #{conversationId} AND seq = #{seq} AND recalled_at IS NULL
            """)
    int markRecalled(@Param("conversationId") String conversationId,
                     @Param("seq") long seq,
                     @Param("recalledAt") long recalledAt);
}
