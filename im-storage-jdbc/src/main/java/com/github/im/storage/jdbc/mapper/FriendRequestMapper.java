package com.github.im.storage.jdbc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.im.storage.jdbc.entity.FriendRequestEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * {@code im_friend_request} 的 Mapper。
 *
 * <p>不需要手写 SQL：读写都是 {@code (from_id, to_id)} 唯一键上的点查或按
 * {@code to_id}/{@code from_id} 的小范围列表，{@link BaseMapper} 的
 * {@code selectOne} / {@code selectList} / {@code insert} / {@code updateById} 够用。
 * （对比 {@code RelationMapper}：那里批量插入要用 {@code INSERT IGNORE}，所以必须手写。）
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Mapper
public interface FriendRequestMapper extends BaseMapper<FriendRequestEntity> {
}
