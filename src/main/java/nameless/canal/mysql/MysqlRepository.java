package nameless.canal.mysql;

import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Repository
public class MysqlRepository {
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public MysqlRepository(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<Map<String, Object>> fetch(String sql, Map<String, ?> parameters) {
        MapSqlParameterSource parameterSource = new MapSqlParameterSource(parameters);
        return jdbcTemplate.queryForList(sql, parameterSource);
    }

}
