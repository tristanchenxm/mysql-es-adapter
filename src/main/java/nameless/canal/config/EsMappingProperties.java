package nameless.canal.config;

import lombok.AccessLevel;
import lombok.Data;
import lombok.Setter;
import nameless.canal.util.NamedParameterUtils;
import nameless.canal.util.ObjectTypeUtils;
import nameless.canal.util.ParsedSql;
import org.apache.commons.lang3.StringUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.Valid;
import javax.validation.ValidationException;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Validated
@ConfigurationProperties(prefix = "sync-config")
@Data
public class EsMappingProperties {
    private Map<String, Mapping> mappings;
    /**
     * 联动表mapping, 当关联表发生变化时，需要重建constructedProperties
     */
    @Setter(AccessLevel.NONE)
    private Map<String, List<Mapping>> cascadeEventMapping = new HashMap<>();

    public void setMappings(@NotEmpty List<Mapping> mappings) {
        if (mappings == null) {
            return;
        }
        this.mappings = mappings.stream().collect(Collectors.toMap(Mapping::getTable, m -> m));
        // 构建联动表事件监听mapping
        for (Mapping mapping : mappings) {
            List<Mapping.ConstructedProperty> constructedProperties = mapping.getConstructedProperties();
            if (constructedProperties != null) {
                for (Mapping.ConstructedProperty constructedProperty : constructedProperties) {
                    Mapping.ConstructedProperty.ReconstructionCondition reconstructionCondition = constructedProperty.getReconstructionCondition();
                    if (reconstructionCondition != null) {
                        List<Mapping> cascadedMappings = cascadeEventMapping.computeIfAbsent(reconstructionCondition.getTable(), k -> new ArrayList<>());
                        cascadedMappings.add(mapping);
                    }
                }
            }
        }
    }

    @Validated
    @Data
    public static class Mapping {
        @NotEmpty
        private String table;
        @NotEmpty
        private String esIndex;
        private String id = "id"; // id列名
        private Map<String, SimpleProperty> simplePropertyMap;
        @Valid
        private List<ConstructedProperty> constructedProperties;

        public String getEsIndex() {
            return StringUtils.isEmpty(esIndex) ? table : esIndex;
        }

        public void setSimpleProperties(@NotEmpty List<String> simpleProperties) {
            if (simpleProperties == null) {
                return;
            }
            simplePropertyMap = simpleProperties.stream().map(SimpleProperty::new)
                    .collect(Collectors.toMap(SimpleProperty::getColumn, p -> p));
        }

        /**
         * 简单属性，直接复制表字段名称和值
         */
        @Data
        public static class SimpleProperty {
            /**
             * 数据库列名
             */
            private String column;
            /**
             * 别名，作为 es field name
             */
            private String alias;
            /**
             * 目标类型
             */
            private String targetType;

            public SimpleProperty(String columnAndTargetType) {
                String[] cat = columnAndTargetType.split(":");
                this.column = cat[0];
                if (cat.length == 2) {
                    targetType = cat[1].toLowerCase();
                } else if (cat.length == 3) {
                    alias = cat[2];
                }
                if (alias == null) {
                    alias = column;
                }
            }

            public Object convertFromType(String value, String srcType) {
                return ObjectTypeUtils.convertType(value, srcType, targetType);
            }

            public Object convertType(Object value) {
                return ObjectTypeUtils.convertToType(value, targetType);
            }

        }

        /**
         * 根据条件生成的字段
         */
        @Validated
        @Data
        public static class ConstructedProperty {
            /**
             * 字段名，如果joinType为FLAT_SIMPLE_PROPERTY，可以用逗号分隔多个字段名
             */
            @NotEmpty
            private String name;

            /**
             * 如果
             */
            private String[] propertyNames;
            /**
             * 关联类型，1:1或者1:M
             */
            private JoinType joinType;
            /**
             * 每次主表变更时，通过sql获取构建字段
             */
            private String sql;
            /**
             * 主表数据变更时，是否触发关联字段重新构建
             * 多数情况一般是先有主表记录，再插入关联表，所以默认为false. 只在关联表数据变更的时候再构建更新
             */
            private boolean constructOnMainTableChange;
            /**
             * 重建constructedProperty的触发条件
             */
            @Valid
            private ReconstructionCondition reconstructionCondition;
            /**
             * non-configurable property
             * SQL动态参数，必须是主表中的属性名
             */
            @Setter(AccessLevel.NONE)
            private Set<String> parameterNames;

            /**
             * 构建字段的形态
             */
            public enum JoinType {
                /**
                 * 普通字段，int, long, boolean, float, double, decimal, date等简单类型
                 */
                FLAT_SIMPLE_PROPERTY,
                /**
                 * 普通列表（列表元素是简单类型，而非复杂对象）
                 */
                FLAT_LIST,
                /**
                 * 内嵌对象
                 */
                NESTED_OBJECT,
                /**
                 * 内嵌列表
                 */
                NESTED_OBJECT_LIST
            }

            @Data
            public static class ReconstructionCondition {
                /**
                 * 重建属性方式
                 */
                public enum DatasourceType {
                    /**
                     * 通过SQL查询
                     */
                    RETRIEVE_SQL,
                    /**
                     * 直接使用变更行数据
                     */
                    ROW_DATA
                }

                @NotEmpty
                private String table;
                private DatasourceType datasourceType = DatasourceType.RETRIEVE_SQL;

                private List<SimpleProperty> dataColumns;

                private String retrieveSql;
                /**
                 * es index id对应的列
                 */
                @NotEmpty
                private String indexId;

                private List<String> onColumnsUpdated = Collections.emptyList();
                /**
                 * 必须是table中的列名
                 */
                @Setter(AccessLevel.NONE)
                private Set<String> parameterNames;

                public void setRetrieveSql(String retrieveSql) {
                    this.retrieveSql = retrieveSql;
                    ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(retrieveSql);
                    parameterNames = new HashSet<>(parsedSql.getParameterNames());
                }

                public List<String> getOnColumnsUpdated() {
                    return onColumnsUpdated;
                }

                public void setOnColumnsUpdated(String onColumnsUpdated) {
                    if (StringUtils.isEmpty(onColumnsUpdated) || onColumnsUpdated.equals("*")) {
                        this.onColumnsUpdated = Collections.emptyList();
                    } else {
                        this.onColumnsUpdated = Arrays.asList(onColumnsUpdated.split("\\s*,\\s*"));
                    }
                }

                public void setDataColumns(List<String> dataColumns) {
                    this.dataColumns = dataColumns.stream().map(SimpleProperty::new).collect(Collectors.toList());
                }
            }

            private static final Pattern nameSplitPattern = Pattern.compile("\\s*,\\s*");

            public void setName(String name) {
                this.name = name;
                propertyNames = nameSplitPattern.split(name);
                validateJoinTypeAndPropertyNames();
            }

            public void setPropertyNames(String[] propertyNames) {
                this.propertyNames = propertyNames;
                this.name = String.join(",", propertyNames);
                validateJoinTypeAndPropertyNames();
            }

            private void validateJoinTypeAndPropertyNames() {
                if (joinType == null || joinType == JoinType.FLAT_SIMPLE_PROPERTY
                        || propertyNames == null || propertyNames.length == 1) {
                    return;
                }
                throw new ValidationException("only join type FLAT_SIMPLE_PROPERTY allows multiple property names");
            }

            public void setReconstructionCondition(ReconstructionCondition reconstructionCondition) {
                this.reconstructionCondition = reconstructionCondition;
            }

            public void setJoinType(@NotNull String joinType) {
                this.joinType = JoinType.valueOf(joinType);
                validateJoinTypeAndPropertyNames();
            }

            public void setSql(String sql) {
                this.sql = sql;
                ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
                parameterNames = new HashSet<>(parsedSql.getParameterNames());
            }
        }
    }

}
