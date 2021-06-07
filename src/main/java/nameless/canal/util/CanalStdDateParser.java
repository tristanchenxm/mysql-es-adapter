package nameless.canal.util;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class CanalStdDateParser {
    public static final HashMap<Integer, String> formatMap = new HashMap<>();

    static {
        formatMap.put("yyyy-MM-dd HH:mm:ss.SSS".length(), "yyyy-MM-dd HH:mm:ss.SSS");
        formatMap.put("yyyy-MM-dd HH:mm:ss".length(), "yyyy-MM-dd HH:mm:ss");
        formatMap.put("yyyy-MM-dd".length(), "yyyy-MM-dd");
        formatMap.put("HH:mm:ss".length(), "HH:mm:ss");
    }

    ;

    public Date parse(String dateStr) throws ParseException {
        String pattern = formatMap.get(dateStr.length());
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        return sdf.parse(dateStr);
    }
}
