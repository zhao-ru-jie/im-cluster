package com.github.im.storage.jdbc;

import com.github.im.server.group.GroupMemberRepository;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * MySQL 版群成员存储（{@code im.storage=jdbc}）。
 *
 * <p>表结构见 {@code db/schema.sql} 的 {@code im_group_member}：
 * {@code uk_group_member (group_id, user_id)} 保证加成员幂等，
 * {@code idx_group_member_user (user_id)} 支撑「某用户加入了哪些群」的反查（Redis Set 做不到这一点）。
 *
 * <p>与 Redis 版的行为差异：这里第一版统一写 {@code role=MEMBER}。角色（OWNER/ADMIN）留给
 * 群管理接口引入，不在此处凭空发明默认群主——扇出只关心「谁在群里」。
 *
 * @author zhaorujie
 * @since 1.0.0
 */
public class JdbcGroupMemberRepository implements GroupMemberRepository {

    /** INSERT IGNORE 把「重复加成员」的幂等语义下沉到数据库的唯一索引 */
    private static final String INSERT_SQL = """
            INSERT IGNORE INTO im_group_member (group_id, user_id, role, created_at)
            VALUES (?, ?, ?, ?)
            """;

    private static final String DELETE_SQL = """
            DELETE FROM im_group_member WHERE group_id = ? AND user_id = ?
            """;

    private static final String MEMBERS_SQL = """
            SELECT user_id FROM im_group_member WHERE group_id = ?
            """;

    private static final String DEFAULT_ROLE = "MEMBER";

    private final JdbcTemplate jdbc;

    public JdbcGroupMemberRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    @Override
    public void addMembers(String groupId, Collection<String> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        List<Object[]> batch = new ArrayList<>(userIds.size());
        for (String userId : userIds) {
            batch.add(new Object[]{groupId, userId, DEFAULT_ROLE, now});
        }
        jdbc.batchUpdate(INSERT_SQL, batch);
    }

    @Override
    public void removeMember(String groupId, String userId) {
        jdbc.update(DELETE_SQL, groupId, userId);
    }

    @Override
    public List<String> membersOf(String groupId) {
        return jdbc.queryForList(MEMBERS_SQL, String.class, groupId);
    }
}
