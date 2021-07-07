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
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

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

    public TransferResult loadIndex(String table, String condition, int batchSize) {
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
                    + " limit " + batchSize;
            HashMap<String, Object> parameters = new HashMap<>();
            String minId = "0";
            List<Map<String, Object>> dataList;
            do {
                parameters.put(idColumn, minId);
                dataList = mysqlRepository.fetch(sql, parameters);
                if (!dataList.isEmpty()) {
                    minId = dataList.get(dataList.size() - 1).get(idColumn).toString();
                    List<UpdateObject> indexObjects = dataList.stream().map(data -> {
                                String id = data.get(idColumn).toString();
                                data.remove(idColumn);
                                UpdateObject indexObject = new UpdateObject(mapping.getEsIndex(), id, data);
                                processConstructedPropertiesOnMainTableChanged(indexObject, mapping, true);
                                // 类型转换
                                indexObject.entrySet().forEach(entry -> {
                                    SimpleProperty configuredProperty = mapping.getSimplePropertyMap().get(entry.getKey());
                                    if (configuredProperty != null) {
                                        entry.setValue(configuredProperty.convertType(entry.getValue()));
                                    }
                                });
                                return indexObject;
                            }
                    ).collect(Collectors.toList());
                    esRepository.bulkInsert(mapping.getEsIndex(), indexObjects);
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


    public void handleRowChanges(List<Entry> entries) throws Exception {
        while (isReIndexing) {
            log.info("another thread is re-indexing tables. wait until it is done");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                log.error("thread interrupted", e);
            }
        }

        UpdateObjects updateObjects = new UpdateObjects();
        for (Entry entry : entries) {
            RowChange rowChange = RowChange.parseFrom(entry.getStoreValue());
            EventType eventType = rowChange.getEventType();
            if (entry.getEntryType() != EntryType.ROWDATA) {
                continue;
            }
            String tableName = entry.getHeader().getTableName();
            if (isUnrecognizableTable(tableName) || !isRecognizableEventType(eventType)) {
                continue;
            }

            log.info("binlog[{}:{}], name[{}.{}], eventType: {}",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), tableName,
                    eventType);
            for (RowData rowData : rowChange.getRowDatasList()) {
                ifSimplePropertiesChanged(tableName, eventType, rowData, updateObjects);
                ifReconstructedPropertiesChanged(tableName, eventType, rowData, updateObjects);
            }
        }

        updateObjects.getDeletes().forEach(deleteObject -> esRepository.deleteById(deleteObject.getIndexName(), deleteObject.getId()));
        updateObjects.getInserts().stream()
                .collect(Collectors.groupingBy(UpdateObject::getIndexName))
                .forEach(esRepository::bulkInsert);
        updateObjects.getUpdates().stream()
                .collect(Collectors.groupingBy(UpdateObject::getIndexName))
                .forEach(esRepository::bulkUpdate);
        if (updateObjects.isNotEmpty()) {
            log.info("Total {} insert/update/delete operations done", updateObjects.size());
        }
    }

    private boolean isRecognizableEventType(EventType eventType) {
        return eventType == EventType.INSERT || eventType == EventType.UPDATE || eventType == EventType.DELETE;
    }

    private boolean isUnrecognizableTable(String tableName) {
        return esMappingProperties.getMappings().get(tableName) == null
                && CollectionUtils.isEmpty(esMappingProperties.getCascadeEventMapping().get(tableName));
    }

    /**
     * 处理主表（即mappings[].table）发生数据变更
     */
    private void ifSimplePropertiesChanged(String tableName, EventType eventType, RowData rowData, UpdateObjects updateObjects) {
        Mapping mapping = esMappingProperties.getMappings().get(tableName);
        if (mapping == null) {
            return;
        }
        if (eventType == EventType.DELETE) {
            tryDelete(rowData, mapping, updateObjects);
        } else if (eventType == EventType.INSERT) {
            tryInsert(rowData, mapping, updateObjects);
        } else if (eventType == EventType.UPDATE) {
            tryUpdate(rowData, mapping, updateObjects);
        }
    }

    private void tryDelete(RowData rowData, Mapping mapping, UpdateObjects updateObjects) {
        UpdateObject delete = getUpdateObject(rowData.getBeforeColumnsList(), mapping);
        log.info("index {}.{} to delete", delete.getIndexName(), delete.getId());
        updateObjects.delete(delete);
    }

    private void tryInsert(RowData rowData, Mapping mapping, UpdateObjects updateObjects) {
        UpdateObject insert = getUpdateObject(rowData.getAfterColumnsList(), mapping);
        processConstructedPropertiesOnMainTableChanged(insert, mapping, false);
        log.info("index {}.{} to insert", insert.getIndexName(), insert.getId());
        updateObjects.insert(insert);
    }

    private void tryUpdate(RowData rowData, Mapping mapping, UpdateObjects updateObjects) {
        String esIndex = mapping.getEsIndex();
        UpdateObject before = getUpdateObject(rowData.getBeforeColumnsList(), mapping);
        log.info("data before change: {}", JsonUtil.toString(before));
        UpdateObject after = getUpdateObject(rowData.getAfterColumnsList(), mapping);
        log.info("data after change: {}", JsonUtil.toString(after));
        String beforeId = before.getId();
        Object afterId = after.getId();
        if (before.getId().equals(after.getId())) {
            if (after.hasAnyChange()) {
                processConstructedPropertiesOnMainTableChanged(after, mapping, false);
                if (updateObjects.isInInsertBuffer(after) || esRepository.exists(esIndex, beforeId)) {
                    updateObjects.update(after.changedOnlyObject());
                } else {
                    updateObjects.insert(after);
                }
            }
        } else { // ID被UPDATE
            log.info("index {} id changed from {} to {}, delete {} and insert {}", esIndex, beforeId, afterId, beforeId, afterId);
            processConstructedPropertiesOnMainTableChanged(after, mapping, false);
            updateObjects.insert(after);
            updateObjects.delete(before);
        }
    }

    /**
     * 提取数据
     */
    private UpdateObject getUpdateObject(List<CanalEntry.Column> columns, Mapping mapping) {
        Map<String, SimpleProperty> simpleProperties = mapping.getSimplePropertyMap();
        UpdateObject o = new UpdateObject();
        o.setIndexName(mapping.getEsIndex());

        for (CanalEntry.Column c : columns) {
            String columnName = c.getName();
            String value = c.getIsNull() ? null : c.getValue();
            SimpleProperty simpleProperty = simpleProperties.get(columnName);
            if (columnName.equalsIgnoreCase(mapping.getId())) {
                o.setId(value);
            } else if (simpleProperty != null) {
                o.put(columnName, simpleProperty.convertType(value, c.getMysqlType()), c.getUpdated());
            }
        }
        return o;
    }

    /**
     * @param constructionGuaranteed 是否忽略constructOnMainTableChange, 一定构建关联属性。发生在数据一次性导入时
     *                               主表信息变更时，构建关联属性
     */
    private void processConstructedPropertiesOnMainTableChanged(UpdateObject data, Mapping mapping, boolean constructionGuaranteed) {
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
                    .collect(Collectors.toMap(p -> p, p -> mapping.getId().equals(p) ? data.getId() : data.get(p)));
            if (sql != null && !parameters.isEmpty()) {
                List<Map<String, Object>> relatedList = mysqlRepository.fetch(sql, parameters);
                Map<String, Object> constructedData = doConstructProperty(constructedProperty, relatedList);
                log.info("constructed property for index {}.{}: {}", data.getIndexName(), data.getId(), JsonUtil.toString(constructedData));
                data.putAll(constructedData);
            }
        }
    }

    /**
     * 处理关联表数据变更，重新构建关联属性并保存变更到elasticsearch
     *
     */
    private void ifReconstructedPropertiesChanged(String tableName, EventType eventType,
                                                  RowData rowData, UpdateObjects updateObjects) {
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
                if (needProcess(esIndexName, constructedProperty, before, updateObjects)) {
                    updateIndexWhenConstructedPropertiesChanged(eventType, esIndexName, constructedProperty, before, updateObjects);
                }
            } else if (eventType == EventType.INSERT) {
                Map<String, Object> after = getDataMap(rowData.getAfterColumnsList());
                //判断是否需要处理此行数据。如果有相同indexIdValue的数据已经处理过，则可以忽略此行数据
                if (needProcess(esIndexName, constructedProperty, after, updateObjects)) {
                    updateIndexWhenConstructedPropertiesChanged(eventType, esIndexName, constructedProperty, after, updateObjects);
                }
            } else if (eventType == EventType.UPDATE) {
                Map<String, Object> before = getDataMap(rowData.getBeforeColumnsList());
                Map<String, Object> after = getDataMap(rowData.getAfterColumnsList());
                String indexIdName = constructedProperty.getReconstructionCondition().getIndexId();
                if (isMonitoringColumnsChanged(before, after, constructedProperty.getReconstructionCondition().getOnColumnsUpdated())) {
                    //变更了indexId，需要先将before关联的indexId更新
                    if (!Objects.equals(before.get(indexIdName), after.get(indexIdName))
                            && needProcess(esIndexName, constructedProperty, before, updateObjects)) {
                        updateIndexWhenConstructedPropertiesChanged(EventType.DELETE, esIndexName, constructedProperty, before, updateObjects);
                    }
                    // *** 不能是else if ***, 因为无论是否变更了indexId，都需要根据after的数据更新一遍。
                    if (needProcess(esIndexName, constructedProperty, before, updateObjects)
                            || needProcess(esIndexName, constructedProperty, after, updateObjects)) {
                        updateIndexWhenConstructedPropertiesChanged(eventType, esIndexName, constructedProperty, after, updateObjects);
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

    /*
     * 对于构建字段，如果是FLAT_LIST或者NESTED_OBJECT_LIST映射，则经常存在的一个场景是因为批量增删改而导致的一条binlog中包含多行数据。
     * 而在我们的处理逻辑中，处理第一行数据的同时会连带包含其余是同一index id的数据行。因此，后面几行数据可以忽略不处理。
     * 比如：insert into table_many(id, table_one_id) values (1, 1), (2, 1), (3, 1) 此sql会触发一条包含三行数据的binlog，
     * 在我们的处理逻辑中，处理完id=1的数据后，就会正确构造出index属性many_field: [1, 2, 3]，后面两行数据可以忽略掉以节约资源。
     */
    private boolean needProcess(String indexName,
                                ConstructedProperty constructedProperty,
                                Map<String, Object> data,
                                UpdateObjects updateObjects) {
        ReconstructionCondition reconstructionCondition = constructedProperty.getReconstructionCondition();
        String indexId = data.get(reconstructionCondition.getIndexId()).toString();
        if (reconstructionCondition.getDatasourceType() == ReconstructionCondition.DatasourceType.ROW_DATA) {
            // 如果构建方式是直接取变更后的binlog数据，则一定要更新
            return true;
        }
        // 查找已经在缓存中排队准备insert/update/delete的index文档对象
        // 不在队列中或者虽然文档对象在队列中，但是对应的property并未有更新过，则需要更新对象
        // 否则忽略该条binlog
        UpdateObject queueingUpdateObject = updateObjects.find(indexName, indexId);
        String propertyName = constructedProperty.getPropertyNames()[0];
        return queueingUpdateObject == null || !queueingUpdateObject.hasSetValue(propertyName);
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
                                                    List<Map<String, Object>> dataList) {
        convertDateToRecognizable(dataList);
        Map<String, Object> changeData = new HashMap<>();
        String propertyName = constructedPropertyConfig.getName();
        switch (constructedPropertyConfig.getJoinType()) {
            case FLAT_SIMPLE_PROPERTY:
                assertSingletonList(dataList, propertyName);
                Map<String, Object> rowData = dataList.isEmpty() ? Collections.emptyMap() : dataList.get(0);
                // 可以允许多个property
                for (String pn : constructedPropertyConfig.getPropertyNames()) {
                    changeData.put(pn, rowData.get(pn));
                }
                break;
            case FLAT_LIST:
                assertSingleProperty(dataList, propertyName);
                List<Object> values = dataList.stream()
                        .map(row -> row.entrySet().iterator().next().getValue()).collect(Collectors.toList());
                changeData.put(propertyName, values);
                break;
            case NESTED_OBJECT:
                assertSingletonList(dataList, propertyName);
                changeData.put(propertyName, dataList.isEmpty() ? null : dataList.get(0));
                break;
            case NESTED_OBJECT_LIST:
                changeData.put(propertyName, dataList);
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
                entry.setValue(new Date(((Date) value).getTime()));
            }
        }));
    }

    private void assertSingletonList(List<?> list, String name) {
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
                                                             Map<String, Object> rowData,
                                                             UpdateObjects updateObjects) {
        ReconstructionCondition reconstructionCondition = constructedPropertyConfig.getReconstructionCondition();
        List<Map<String, Object>> constructedList;
        if (reconstructionCondition.getDatasourceType() == ReconstructionCondition.DatasourceType.RETRIEVE_SQL) {
            Map<String, ?> parameters = reconstructionCondition.getParameterNames().stream().collect(Collectors.toMap(p -> p, rowData::get));
            constructedList = mysqlRepository.fetch(reconstructionCondition.getRetrieveSql(), parameters);
        } else {
            constructedList = eventType == EventType.DELETE ?
                    Collections.emptyList() :
                    Collections.singletonList(reconstructionCondition.getDataColumns().stream()
                            .collect(HashMap::new, (m, prop) -> m.put(prop.getColumn(), prop.convertType(rowData.get(prop.getColumn()))), HashMap::putAll));
        }
        Map<String, Object> changeData = doConstructProperty(constructedPropertyConfig, constructedList);
        Object esIndexId = rowData.get(reconstructionCondition.getIndexId());
        if (esIndexId == null) {
            log.warn("no index id column in the changed row: {}", JsonUtil.toString(rowData));
            return;
        }
        UpdateObject update = new UpdateObject(esIndexName, esIndexId.toString(), changeData);
        if (updateObjects.exists(update) || esRepository.exists(esIndexName, esIndexId.toString())) {
            log.info("constructed data to be updated for {}.{}: {}", esIndexName, esIndexId.toString(), JsonUtil.toString(changeData));
            updateObjects.update(update);
        }
    }
}
