package com.github.im.storage.jdbc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.im.storage.jdbc.entity.MessageEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * {@code im_message} 的 Mapper。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Mapper
public interface MessageMapper extends BaseMapper<MessageEntity> {
}
