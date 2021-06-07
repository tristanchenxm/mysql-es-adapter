package nameless.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import lombok.Getter;
import lombok.Setter;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
public class RawRow {
    private String table;
    private CanalEntry.EventType eventType;
    private List<RawColumn> beforeColumns;
    private List<RawColumn> afterColumns;
    private String sql;

    public List<RawColumn> getBeforeColumns() {
        return beforeColumns == null ? Collections.emptyList() : beforeColumns;
    }

    public List<RawColumn> getAfterColumns() {
        return afterColumns == null ? Collections.emptyList() : afterColumns;
    }

    public RawColumn getAfterColumn(String columnName) {
        return afterColumns.stream().filter(c -> c.getName().equalsIgnoreCase(columnName)).findFirst().get();
    }

    public String getAfterColumnValue(String columnName) {
        return getAfterColumn(columnName).getValue();
    }

    public RawColumn getBeforeColumn(String columnName) {
        return beforeColumns.stream().filter(c -> c.getName().equalsIgnoreCase(columnName)).findFirst().get();
    }

    public String getBeforeColumnValue(String columnName) {
        return getBeforeColumn(columnName).getValue();
    }

    public List<RawColumn> getChangedAfterColumns() {
        return afterColumns.stream().filter(RawColumn::isUpdated).collect(Collectors.toList());
    }
}
