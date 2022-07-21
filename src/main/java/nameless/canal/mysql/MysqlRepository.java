package nameless.canal.mysql;

import nameless.canal.config.EsMappingProperties;
import nameless.canal.transfer.CasePreDefinedColumnMapRowMapper;
import nameless.canal.util.ObjectTypeUtils;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class MysqlRepository {
    private static final String typeConvertMark = ":";
    private final NamedParameterJdbcTemplate jdbcTemplate;
    private final EsMappingProperties esMappingProperties;

    public MysqlRepository(NamedParameterJdbcTemplate jdbcTemplate, EsMappingProperties esMappingProperties) {
        this.jdbcTemplate = jdbcTemplate;
        this.esMappingProperties = esMappingProperties;
    }

    public List<Map<String, Object>> fetch(String sql, Map<String, ?> parameters) {
        MapSqlParameterSource parameterSource = new MapSqlParameterSource(parameters);
        List<Map<String, Object>> result = jdbcTemplate.query(sql, parameterSource, new CasePreDefinedColumnMapRowMapper(esMappingProperties.getPropertyCaseType()));
        doTypeConvert(result);
        return result;
    }

    /**
     * 解析是否需要做类型转换
     */
    private Map<String, String[]> getTypeConvertMap(List<Map<String, Object>> result) {
        Map<String, String[]> typeConvertMap = new HashMap<>();
        if (!result.isEmpty()) {
            Map<String, Object> row0 = result.get(0);
            for (String columnName : row0.keySet()) {
                if (columnName.contains(typeConvertMark)) {
                    typeConvertMap.put(columnName, columnName.split(typeConvertMark));
                }
            }
        }
        return typeConvertMap;
    }

    private void doTypeConvert(List<Map<String, Object>> result) {
        Map<String, String[]> typeConvertMap = getTypeConvertMap(result);
        if (!typeConvertMap.isEmpty()) {
            for (Map<String, Object> dataRow : result) {
                for (Map.Entry<String, String[]> convertRow : typeConvertMap.entrySet()) {
                    Object o = dataRow.remove(convertRow.getKey());
                    dataRow.put(convertRow.getValue()[0], ObjectTypeUtils.convertToType(o, convertRow.getValue()[1]));
                }
            }
        }
    }
}
