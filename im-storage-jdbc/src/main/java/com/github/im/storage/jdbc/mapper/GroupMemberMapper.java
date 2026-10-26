package com.github.im.storage.jdbc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.im.storage.jdbc.entity.GroupMemberEntity;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.Collection;

/**
 * {@code im_group_member} 的 Mapper。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Mapper
public interface GroupMemberMapper extends BaseMapper<GroupMemberEntity> {

    @Insert("""
            <script>
            INSERT IGNORE INTO im_group_member (group_id, user_id, role, created_at) VALUES
            <foreach collection="userIds" item="userId" separator=",">
                (#{groupId}, #{userId}, #{role}, #{createdAt})
            </foreach>
            </script>
            """)
    int insertIgnoreBatch(@Param("groupId") String groupId,
                          @Param("userIds") Collection<String> userIds,
                          @Param("role") String role,
                          @Param("createdAt") long createdAt);
}
