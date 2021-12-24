package nameless.canal.util;

import nameless.canal.util.JsonUtil;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class ObjectTypeUtils {
    private static final CanalStdDateParser stdDateFormat = new CanalStdDateParser();

    private static final Map<String, String> sqlTypeMapping = new HashMap<>();

    static {
        sqlTypeMapping.put("int", "int");
        sqlTypeMapping.put("tinyint", "int");
        sqlTypeMapping.put("smallint", "int");
        sqlTypeMapping.put("mediumint", "int");
        sqlTypeMapping.put("bigint", "long");
        sqlTypeMapping.put("float", "float");
        sqlTypeMapping.put("double", "double");
        sqlTypeMapping.put("decimal", "decimal");
        sqlTypeMapping.put("bit", "int");
        sqlTypeMapping.put("char", "string");
        sqlTypeMapping.put("varchar", "string");
        sqlTypeMapping.put("tinytext", "string");
        sqlTypeMapping.put("text", "string");
        sqlTypeMapping.put("mediumtext", "string");
        sqlTypeMapping.put("longtext", "string");
        sqlTypeMapping.put("datetime", "date");
        sqlTypeMapping.put("timestamp", "date");
        sqlTypeMapping.put("time", "date");
        sqlTypeMapping.put("date", "date");
        sqlTypeMapping.put("json", "json");
        // others do not support yet
    }

    public static Object convertFromSqlType(String value, String mysqlType) {
        mysqlType = mysqlType.toLowerCase();
        return convertType(value, mysqlType, sqlTypeMapping.get(mysqlType));
    }

    public static Object convertType(String value, String srcSqlType, String targetType) {
        if (value == null) {
            return null;
        }

        String cleanSrcType = sqlTypeMapping.get(srcSqlType.split("\\(")[0].toLowerCase());
        if (cleanSrcType == null) {
            throw new RuntimeException("Cannot handle srcType " + srcSqlType);
        }
        String type = targetType == null ? cleanSrcType : targetType;
        switch (type) {
            case "string":
                return value;
            case "int":
            case "integer":
                return Integer.valueOf(value);
            case "long":
                return Long.valueOf(value);
            case "float":
                return Float.valueOf(value);
            case "double":
                return Double.valueOf(value);
            case "decimal":
                return new BigDecimal(value);
            case "boolean":
                return value.equals("1") || value.equalsIgnoreCase("true");
            case "date":
                try {
                    return stdDateFormat.parse(value);
                } catch (ParseException e) {
                    throw new RuntimeException(e);
                }
            case "json":
                return JsonUtil.parse(value, Map.class);
            default:
                throw new RuntimeException("Cannot handle srcType=" + srcSqlType + " and targetType=" + targetType);
        }
    }

    public static Object convertToType(Object value, String targetType) {
        if (value == null) {
            return null;
        }
        // 目标类型和原始类型一致，无需转换
        if (targetType == null) {
            return value;
        }
        switch (targetType) {
            case "string":
                return value.toString();
            case "int":
            case "integer":
                if (value instanceof Date) {
                    return ((Date) value).getTime();
                } else if (value instanceof Number) {
                    return ((Number) value).intValue();
                } else {
                    return Integer.valueOf(value.toString());
                }
            case "long":
                if (value instanceof Date) {
                    return ((Date) value).getTime();
                } else if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else {
                    return Long.valueOf(value.toString());
                }
            case "float":
                if (value instanceof Date) {
                    return ((Date) value).getTime();
                } else if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else {
                    return Float.valueOf(value.toString());
                }
            case "double":
                if (value instanceof Date) {
                    return ((Date) value).getTime();
                } else if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else {
                    return Double.valueOf(value.toString());
                }
            case "decimal":
                if (value instanceof Date) {
                    return ((Date) value).getTime();
                } else {
                    return new BigDecimal(value.toString());
                }
            case "boolean":
                if (value instanceof Number) {
                    return ((Number)value).intValue() == 1;
                } else {
                    return value.toString().equalsIgnoreCase("true");
                }
            case "date":
                if (value instanceof Date) {
                    if (value.getClass() != Date.class) {
                        return new Date(((Date) value).getTime());
                    }
                    return value;
                } else {
                    try {
                        return stdDateFormat.parse(value.toString());
                    } catch (ParseException e) {
                        throw new RuntimeException(e);
                    }
                }
            case "json":
                if (value instanceof String) {
                    return JsonUtil.parse((String)value, Map.class);
                } else {
                    return value;
                }
            default:
                throw new RuntimeException("Cannot convert object type " + value.getClass() + " to targetType=" + targetType);
        }
    }
}
