package nameless.canal.transfer;

import org.springframework.jdbc.core.ColumnMapRowMapper;

public class CasePreDefinedColumnMapRowMapper extends ColumnMapRowMapper {

    private final PropertyCaseType caseType;

    public CasePreDefinedColumnMapRowMapper(PropertyCaseType caseType) {
        this.caseType = caseType;
    }

    @Override
    protected String getColumnKey(String columnName) {
        return caseType.normalizeKey(columnName);
    }
}
