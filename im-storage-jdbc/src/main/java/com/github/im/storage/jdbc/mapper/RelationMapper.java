package com.github.im.storage.jdbc.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.github.im.storage.jdbc.entity.RelationEntity;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * {@code im_relation} 的 Mapper。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
@Mapper
public interface RelationMapper extends BaseMapper<RelationEntity> {

    /**
     * 批量建立关系，重复的行由 {@code uk_relation} 静默跳过。
     *
     * <p>用 {@code INSERT IGNORE} 而不是「先查再插」：好友同步是幂等重放（业务侧可能整批重推），
     * 「查再插」在并发下仍会撞唯一键，最终还是要靠 IGNORE 兜底。
     */
    @Insert("""
            <script>
            INSERT IGNORE INTO im_relation (owner_id, target_id, relation_type, created_at) VALUES
            <foreach collection="rows" item="row" separator=",">
                (#{row.ownerId}, #{row.targetId}, #{row.relationType}, #{row.createdAt})
            </foreach>
            </script>
            """)
    int insertIgnoreBatch(@Param("rows") List<RelationEntity> rows);
}
