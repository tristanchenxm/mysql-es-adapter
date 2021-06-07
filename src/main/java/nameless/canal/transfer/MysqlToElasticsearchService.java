package nameless.canal.transfer;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.*;
import nameless.canal.util.JsonUtil;
import nameless.canal.config.EsMappingProperties;
import nameless.canal.config.EsMappingProperties.Mapping;
import nameless.canal.config.EsMappingProperties.Mapping.ConstructedProperty;
import nameless.canal.config.EsMappingProperties.Mapping.ConstructedProperty.ReconstructionCondition;
import nameless.canal.config.EsMappingProperties.Mapping.SimpleProperty;
import nameless.canal.es.EsRepository;
import nameless.canal.mysql.MysqlRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class MysqlToElasticsearchService {
    private final EsMappingProperties esMappingProperties;
    private final MysqlRepository mysqlRepository;
    private final EsRepository esRepository;
    /**
     * 是否正在重建es索引
     */
    private volatile static boolean isReIndexing = false;

    public MysqlToElasticsearchService(EsMappingProperties esMappingProperties,
                                       MysqlRepository mysqlRepository,
                                       EsRepository esRepository) {
        this.esMappingProperties = esMappingProperties;
        this.mysqlRepository = mysqlRepository;
        this.esRepository = esRepository;
    }

    public TransferResult loadIndex(String table, String condition) {
        if (isReIndexing) {
            return TransferResult.builder().message("please wait until another re-indexing thread is done").build();
        }
        Mapping mapping = esMappingProperties.getMappings().get(table);
        if (mapping == null) {
            return TransferResult.builder().message(String.format("table %s not configured", table)).build();
        }
        log.info("start to load data into elasticsearch for table {}", table);
        int totalIndexed = 0;
        try {
            isReIndexing = true;
            String idColumn = mapping.getId();
            String sql = "select " + StringUtils.join(mapping.getSimplePropertyMap().keySet(), ",")
                    + " from " + table
                    + " where " + idColumn + ">:id"
                    + (StringUtils.isEmpty(condition) ? "" : " and " + condition)
                    + " order by " + idColumn
                    + " limit 10000";
            HashMap<String, Object> parameters = new HashMap<>();
            String minId = "0";
            List<Map<String, Object>> dataList;
            do {
                parameters.put(idColumn, minId);
                dataList = mysqlRepository.fetch(sql, parameters);
                if (!dataList.isEmpty()) {
                    minId = dataList.get(dataList.size() - 1).get(idColumn).toString();
                    List<IndexQuery> indexQueries = dataList.stream().map(data -> {
                                String id = data.get(idColumn).toString();
                                processConstructedProperties(id, data, mapping, true);
                                data.remove(idColumn);
                                // 类型转换
                                data.entrySet().forEach(entry -> {
                                    SimpleProperty configuredProperty = mapping.getSimplePropertyMap().get(entry.getKey());
                                    if (configuredProperty != null) {
                                        entry.setValue(configuredProperty.convertType(entry.getValue()));
                                    }
                                });
                                IndexQuery indexQuery = new IndexQuery();
                                indexQuery.setId(id);
                                indexQuery.setObject(data);
                                return indexQuery;
                            }
                    ).collect(Collectors.toList());
                    esRepository.bulkIndex(mapping.getEsIndex(), indexQueries);
                }
                totalIndexed += dataList.size();
            } while (!dataList.isEmpty());
            log.info("load data into elasticsearch for table {} done. total {} rows indexed. the last index id is {}", table, totalIndexed, minId);
            return TransferResult.builder().rowsProcessed(totalIndexed).message("DONE").build();
        } catch (Exception e) {
            log.error("an exception occurred when loading table " + table, e);
            return TransferResult.builder().rowsProcessed(totalIndexed).message("failed to index").build();
        } finally {
            isReIndexing = false;
        }
    }

    /**
     * {indexName}-{indexId}
     */
    private final String PROCESSED_INDEX_KEY_FORMAT = "%s-%s";
    /**
     * {indexName}-{indexId}-{propertyName}
     */
    private final String PROCESSED_CONSTRUCTED_KEY_FORMAT = "%s-%s-%s";
    public void handleRowChanges(List<Entry> entries) throws Exception {
        while (isReIndexing) {
            log.info("another thread is re-indexing tables. wait until it is done");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                log.error("thread interrupted", e);
            }
        }
        /*
         * 对于构建字段，如果是FLAT_LIST或者NESTED_OBJECT_LIST映射，则经常存在的一个场景是因为批量增删改而导致的一条binlog中包含多行数据。
         * 而在我们的处理逻辑中，处理第一行数据的同时会连带包含其余是同一index id的数据行。因此，后面几行数据可以忽略不处理。
         * 比如：insert into table_many(id, table_one_id) values (1, 1), (2, 1), (3, 1) 此sql会触发一条包含三行数据的binlog，
         * 在我们的处理逻辑中，处理完id=1的数据后，就会正确构造出index属性many_field: [1, 2, 3]，后面两行数据可以忽略掉以节约资源。
         * 此map的key为{indexName}-{indexId}-{propertyName}
         */
        HashMap<String, Object> processedIndexMap = new HashMap<>();
        for (Entry entry : entries) {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            EventType eventType = rowChange.getEventType();
            if (entry.getEntryType() != EntryType.ROWDATA) {
                continue;
            }
            String tableName = entry.getHeader().getTableName();
            log.info("binlog[{}:{}], name[{}.{}], eventType: {}",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), tableName,
                    eventType);
            for (RowData rowData : rowChange.getRowDatasList()) {
                ifSimplePropertiesChanged(tableName, eventType, rowData, processedIndexMap);
                ifReconstructedPropertiesChanged(tableName, eventType, rowData, processedIndexMap);
            }
        }
    }

    /**
     * 处理主表（即mappings[].table）发生数据变更
     */
    private void ifSimplePropertiesChanged(String tableName, EventType eventType, RowData rowData,
                                           Map<String, Object> processedIndexMap) {
        Mapping mapping = esMappingProperties.getMappings().get(tableName);
        if (mapping == null) {
            return;
        }
        if (eventType == EventType.DELETE) {
            tryDelete(rowData, mapping, processedIndexMap);
        } else if (eventType == EventType.INSERT) {
            tryInsert(rowData, mapping, processedIndexMap);
        } else if (eventType == EventType.UPDATE) {
            tryUpdate(rowData, mapping, processedIndexMap);
        }
    }

    /**
     * 提取数据
     * @param changedOnly 只提取变更的列
     */
    private Map<String, Object> getDataMap(List<CanalEntry.Column> columns, boolean changedOnly, Mapping mapping) {
        Map<String, SimpleProperty> simpleProperties = mapping.getSimplePropertyMap();
        Map<String, Object> result = new HashMap<>();
        for (CanalEntry.Column c : columns) {
            String columnName = c.getName();
            String value = c.getIsNull() ? null : c.getValue();
            SimpleProperty simpleProperty = simpleProperties.get(columnName);
            if (simpleProperty != null && (!changedOnly || c.getUpdated())) {
                result.put(columnName, simpleProperty.convertType(value, c.getMysqlType()));
            }
        }
        return result;
    }


    private void tryDelete(RowData rowData, Mapping mapping, Map<String, Object> processedIndexMap) {
        String esIndex = mapping.getEsIndex();
        Map<String, Object> delete = getDataMap(rowData.getBeforeColumnsList(), false, mapping);
        log.info("data to delete: {}", JsonUtil.toString(delete));
        String id = delete.get(mapping.getId()).toString();
        if (esRepository.exists(esIndex, id)) {
            esRepository.deleteById(esIndex, id);
        } else {
            log.info("{}.{} not exists", esIndex, id);
        }
        processedIndexMap.put(String.format(PROCESSED_INDEX_KEY_FORMAT, esIndex, id), Boolean.TRUE);
    }

    private void tryInsert(RowData rowData, Mapping mapping, Map<String, Object> processedIndexMap) {
        String esIndex = mapping.getEsIndex();
        Map<String, Object> insert = getDataMap(rowData.getAfterColumnsList(), false, mapping);
        String id = insert.get(mapping.getId()).toString();
        if (!esRepository.exists(esIndex, id)) {
            insert(esIndex, insert, mapping);
        } else {
            update(esIndex, id, insert, mapping);
        }
        processedIndexMap.put(String.format(PROCESSED_INDEX_KEY_FORMAT, esIndex, id), Boolean.TRUE);
    }

    private void insert(String esIndex, Map<String, Object> data, Mapping mapping) {
        String idName = mapping.getId();
        String id = data.get(idName).toString();
        processConstructedProperties(id, data, mapping, false);
        data.remove(idName);
        log.info("{} id: {} insert data: {}", esIndex, id, JsonUtil.toString(data));
        esRepository.insert(esIndex, id, data);
    }

    private void update(String esIndex, String id, Map<String, Object> data, Mapping mapping) {
        processConstructedProperties(id, data, mapping, false);
        data.remove(mapping.getId());
        log.info("{} id: {} update data: {}", esIndex, id, JsonUtil.toString(data));
        esRepository.updateById(esIndex, id, data);
    }

    private void tryUpdate(RowData rowData, Mapping mapping, Map<String, Object> processedIndexMap) {
        String esIndex = mapping.getEsIndex();
        Map<String, Object> before = getDataMap(rowData.getBeforeColumnsList(), false, mapping);
        log.info("data before change: {}", JsonUtil.toString(before));
        Map<String, Object> after = getDataMap(rowData.getAfterColumnsList(), true, mapping);
        log.info("data changed: {}", JsonUtil.toString(after));
        if (!after.isEmpty()) {
            String idName = mapping.getId();
            String beforeId = before.get(idName).toString();
            Object afterId = after.get(idName);
            if (afterId != null) { // ID被UPDATE
                log.info("id changed from {} to {}, there will be one insert and one delete for elasticsearch", beforeId, afterId);
                Map<String, Object> insert = getDataMap(rowData.getAfterColumnsList(), false, mapping);
                insert(esIndex, insert, mapping);
                if (esRepository.exists(esIndex, beforeId)) {
                    esRepository.deleteById(esIndex, beforeId);
                }
            } else {
                if (esRepository.exists(esIndex, beforeId)) {
                    update(esIndex, beforeId, after, mapping);
                } else {
                    Map<String, Object> insert = getDataMap(rowData.getAfterColumnsList(), false, mapping);
                    insert(esIndex, insert, mapping);
                }
            }
            processedIndexMap.put(String.format(PROCESSED_INDEX_KEY_FORMAT, esIndex, beforeId), Boolean.TRUE);
            processedIndexMap.put(String.format(PROCESSED_INDEX_KEY_FORMAT, esIndex, afterId), Boolean.TRUE);
        }
    }

    /**
     * @param constructionGuaranteed 是否忽略constructOnMainTableChange, 一定构建关联属性。发生在数据一次性导入时
     *                               主表信息变更时，构建关联属性
     */
    private void processConstructedProperties(final String id, Map<String, Object> data, Mapping mapping, boolean constructionGuaranteed) {
        List<ConstructedProperty> constructedProperties = mapping.getConstructedProperties();
        if (constructedProperties == null) {
            return;
        }
        for (ConstructedProperty constructedProperty : constructedProperties) {
            if (!constructionGuaranteed && !constructedProperty.isConstructOnMainTableChange()) {
                continue;
            }
            String sql = constructedProperty.getSql();
            Map<String, Object> parameters = constructedProperty.getParameterNames().stream()
                    .collect(Collectors.toMap(p -> p, p -> mapping.getId().equals(p) ? id : data.get(p)));
            if (sql != null && !parameters.isEmpty()) {
                List<Map<String, Object>> relatedList = mysqlRepository.fetch(sql, parameters);
                Map<String, Object> constructedData = doConstructProperty(constructedProperty, relatedList);
                data.putAll(constructedData);
            }
        }
    }

    /**
     * 处理关联表数据变更，重新构建关联属性并保存变更到elasticsearch
     * @param processedIndexMap 用于标记前面已经处理过的拥有同一indexId的变更数据行，当sql批量更改时，可以避免重复构建节省资源
     *                                      此map的key为{indexName}-{indexId}-{propertyName}
     */
    private void ifReconstructedPropertiesChanged(String tableName, EventType eventType, RowData rowData, Map<String, Object> processedIndexMap) {
        List<Mapping> mappings = esMappingProperties.getCascadeEventMapping().get(tableName);
        if (mappings == null) {
            return;
        }

        for (Mapping mapping : mappings) {
            String esIndexName = mapping.getEsIndex();
            @SuppressWarnings("OptionalGetWithoutIsPresent")
            ConstructedProperty constructedProperty = mapping.getConstructedProperties().stream()
                    .filter(cp -> cp.getReconstructionCondition() != null && cp.getReconstructionCondition().getTable().equals(tableName))
                    .findFirst().get();

            if (eventType == EventType.DELETE) {
                Map<String, Object> before = getDataMap(rowData.getBeforeColumnsList());
                if (needProcess(esIndexName, constructedProperty, before, processedIndexMap)) {
                    updateIndexWhenConstructedPropertiesChanged(eventType, esIndexName, constructedProperty, before);
                }
            } else if (eventType == EventType.INSERT) {
                Map<String, Object> after = getDataMap(rowData.getAfterColumnsList());
                 //判断是否需要处理此行数据。如果有相同indexIdValue的数据已经处理过，则可以忽略此行数据
                if (needProcess(esIndexName, constructedProperty, after, processedIndexMap)) {
                    updateIndexWhenConstructedPropertiesChanged(eventType, esIndexName, constructedProperty, after);
                }
            } else if (eventType == EventType.UPDATE) {
                Map<String, Object> before = getDataMap(rowData.getBeforeColumnsList());
                Map<String, Object> after = getDataMap(rowData.getAfterColumnsList());
                String indexIdName = constructedProperty.getReconstructionCondition().getIndexId();
                if (isMonitoringColumnsChanged(before, after, constructedProperty.getReconstructionCondition().getOnColumnsUpdated())) {
                    //变更了indexId，需要先将before关联的indexId更新
                    if (!Objects.equals(before.get(indexIdName), after.get(indexIdName))
                            && needProcess(esIndexName, constructedProperty, before, processedIndexMap)) {
                        updateIndexWhenConstructedPropertiesChanged(EventType.DELETE, esIndexName, constructedProperty, before);
                    }
                    // *** 不能是else if ***, 因为无论是否变更了indexId，都需要根据after的数据更新一遍。
                    if (needProcess(esIndexName, constructedProperty, before, processedIndexMap)
                            || needProcess(esIndexName, constructedProperty, after, processedIndexMap)) {
                        updateIndexWhenConstructedPropertiesChanged(eventType, esIndexName, constructedProperty, after);
                    }
                } else {
                    log.info("no monitoring columns changed for index {}.{}", esIndexName, after.get(indexIdName));
                }
            }

        }
    }

    private boolean isMonitoringColumnsChanged(Map<String, Object> before, Map<String, Object> after, List<String> monitoringColumns) {
        if (monitoringColumns.isEmpty()) {
            return true;
        }
        return monitoringColumns.stream().anyMatch(columnName -> !Objects.equals(before.get(columnName), after.get(columnName)));
    }

    private boolean needProcess(String indexName,
                                ConstructedProperty constructedProperty,
                                Map<String, Object> data,
                                Map<String, Object> processedIndexMap) {
        ReconstructionCondition reconstructionCondition = constructedProperty.getReconstructionCondition();
        String indexId = data.get(reconstructionCondition.getIndexId()).toString();
        if (reconstructionCondition.getDatasourceType() == ReconstructionCondition.DatasourceType.ROW_DATA) {
            // 如果构建方式是直接取变更后的binlog数据，则一定要更新
            return true;
        }
        if (constructedProperty.isConstructOnMainTableChange()
                && processedIndexMap.get(String.format(PROCESSED_INDEX_KEY_FORMAT, indexName, indexId)) == Boolean.TRUE) {
            return false;
        }
        String propertyName = constructedProperty.getName();
        return processedIndexMap.put(String.format(PROCESSED_CONSTRUCTED_KEY_FORMAT, indexName, indexId, propertyName), Boolean.TRUE) == null;
    }

    private Map<String, Object> getDataMap(List<CanalEntry.Column> columns) {
        Map<String, Object> result = new HashMap<>();
        for (CanalEntry.Column c : columns) {
            String columnName = c.getName();
            String value = c.getIsNull() ? null : c.getValue();
            result.put(columnName, SimpleProperty.convertFromSqlType(value, c.getMysqlType()));
        }
        return result;
    }

    /**
     * 处理构建字段
     */
    private Map<String, Object> doConstructProperty(ConstructedProperty constructedPropertyConfig,
                                                    List<Map<String, Object>> constructedList) {
        convertDateToRecognizable(constructedList);
        Map<String, Object> changeData = new HashMap<>();
        String propertyName = constructedPropertyConfig.getName();
        switch (constructedPropertyConfig.getJoinType()) {
            case FLAT_SIMPLE_PROPERTY:
                assertSingleProperty(constructedList, propertyName);
                List<Object> values = constructedList.stream()
                        .map(row -> row.entrySet().iterator().next().getValue()).collect(Collectors.toList());

                assertListSize(values, propertyName);
                changeData.put(propertyName, values.isEmpty() ? null : values.get(0));
                break;
            case FLAT_LIST:
                assertSingleProperty(constructedList, propertyName);
                values = constructedList.stream()
                        .map(row -> row.entrySet().iterator().next().getValue()).collect(Collectors.toList());
                changeData.put(propertyName, values);
                break;
            case NESTED_OBJECT:
                assertListSize(constructedList, propertyName);
                changeData.put(propertyName, constructedList.isEmpty() ? null : constructedList.get(0));
                break;
            case NESTED_OBJECT_LIST:
                changeData.put(propertyName, constructedList);
                break;
        }
        return changeData;
    }

    /**
     * 对于日期类，elasticsearch库只认识getClass() == Date.class，所以需要事先把timestamp等类型预先处理
     */
    private void convertDateToRecognizable(List<Map<String, Object>> list) {
        list.forEach(row -> row.entrySet().forEach(entry -> {
            Object value = entry.getValue();
            if (value instanceof Date && value.getClass() != Date.class) {
                entry.setValue(new Date(((Date)value).getTime()));
            }
        }));
    }

    private void assertListSize(List<?> list, String name) {
        if (list.size() > 1) {
            throw new IllegalArgumentException(name + ": expect one or zero values, but got " + list.size());
        }
    }

    private void assertSingleProperty(List<Map<String, Object>> list, String name) {
        if (list.stream().anyMatch(m -> m.size() > 1)) {
            throw new IllegalArgumentException(name + ": expected only one property, but actual more");
        }
    }

    private void updateIndexWhenConstructedPropertiesChanged(EventType eventType, String esIndexName,
                                                             ConstructedProperty constructedPropertyConfig,
                                                             Map<String, Object> rowData) {
        ReconstructionCondition reconstructionCondition = constructedPropertyConfig.getReconstructionCondition();
        List<Map<String, Object>> constructedList;
        if (reconstructionCondition.getDatasourceType() == ReconstructionCondition.DatasourceType.RETRIEVE_SQL) {
            Map<String, ?> parameters = reconstructionCondition.getParameterNames().stream().collect(Collectors.toMap(p -> p, rowData::get));
            constructedList = mysqlRepository.fetch(reconstructionCondition.getRetrieveSql(), parameters);
        } else {
            constructedList = eventType == EventType.DELETE ?
                    Collections.emptyList() :
                    Collections.singletonList(reconstructionCondition.getDataColumns().stream()
                            .collect(Collectors.toMap(SimpleProperty::getColumn, prop -> prop.convertType(rowData.get(prop.getColumn())))));
        }
        Map<String, Object> changeData = doConstructProperty(constructedPropertyConfig, constructedList);
        Object esIndexId = rowData.get(reconstructionCondition.getIndexId());
        if (esIndexId != null && esRepository.exists(esIndexName, esIndexId.toString())) {
            log.info("to update constructed data {}.{}: {}", esIndexName, esIndexId.toString(), JsonUtil.toString(changeData));
            esRepository.updateById(esIndexName, esIndexId.toString(), changeData);
        }
    }
}
