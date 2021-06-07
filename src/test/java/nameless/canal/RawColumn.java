package nameless.canal;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class RawColumn {
    private int index;
    private String mysqlType;
    private String name;
    private String value;
    private boolean updated;
}
