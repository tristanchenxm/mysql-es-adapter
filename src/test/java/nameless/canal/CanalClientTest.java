package nameless.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import nameless.canal.client.CanalClient;
import nameless.canal.client.ConfiguredCanalConnector;
import nameless.canal.config.EsMappingProperties;
import nameless.canal.config.EsMappingProperties.Mapping.ConstructedProperty;
import nameless.canal.config.EsMappingProperties.Mapping.SimpleProperty;
import nameless.canal.mysql.MysqlRepository;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.data.elasticsearch.core.query.UpdateResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.*;

@SuppressWarnings("unchecked")
@ActiveProfiles("test")
@SpringBootTest(classes = Main.class)
public class CanalClientTest {
    @MockBean
    private ConfiguredCanalConnector connector;
    @MockBean
    private ElasticsearchOperations operations;
    @Autowired
    private JdbcTemplate jdbcTemplate;
    @Autowired
    private CanalClient canalClient;
    @Autowired
    private EsMappingProperties esMappingProperties;
    @Autowired
    private MysqlRepository mysqlRepository;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @BeforeEach
    public void setup() {
        jdbcTemplate.queryForList("show tables").forEach(
                rs -> jdbcTemplate.execute("truncate table " + rs.get("TABLE_NAME"))
        );

    }

    private String readFile(String fileName) throws Exception {
        try (InputStream inputStream = new ClassPathResource(fileName).getInputStream();
             Scanner scanner = new Scanner(inputStream)) {
            List<String> lines = new LinkedList<>();
            while (scanner.hasNextLine()) {
                lines.add(scanner.nextLine());
            }
            return String.join("\n", lines);
        }
    }

    private List<RawRow> readList(String fileName) throws Exception {
        return objectMapper.readValue(readFile(fileName), new TypeReference<List<RawRow>>() {
        });
    }

    private void executeSql(String sql) {
        jdbcTemplate.execute(sql);
    }

    private List<Entry> getEntries(List<RawRow> rows) {
        List<Entry> entries = new ArrayList<>();
        rows.forEach(row -> {
            executeSql(row.getSql());
            CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
            row.getAfterColumns().forEach(column -> {
                rowDataBuilder.addAfterColumns(CanalEntry.Column.newBuilder()
                        .setMysqlType(column.getMysqlType())
                        .setIndex(column.getIndex())
                        .setName(column.getName())
                        .setValue(column.getValue())
                        .setUpdated(column.isUpdated())
                        .build());
            });
            row.getBeforeColumns().forEach(column -> rowDataBuilder.addBeforeColumns(CanalEntry.Column.newBuilder()
                    .setMysqlType(column.getMysqlType())
                    .setIndex(column.getIndex())
                    .setName(column.getName())
                    .setValue(column.getValue())
                    .build()));
            CanalEntry.RowData rowData = rowDataBuilder.build();
            RowChange rowChange = RowChange.newBuilder()
                    .addRowDatas(rowData)
                    .setEventType(row.getEventType())
                    .build();
            entries.add(Entry.newBuilder()
                    .setEntryType(CanalEntry.EntryType.ROWDATA)
                    .setStoreValue(rowChange.toByteString())
                    .setHeader(CanalEntry.Header.newBuilder().setTableName(row.getTable()).build())
                    .build());
        });
        return entries;
    }

    @Test
    public void testHandleEntries_personOnly() throws Exception {
        reset(operations);
        List<RawRow> rows = readList("test_data_person_1.json");
        Map<EventType, List<RawRow>> grouping = rows.stream().collect(Collectors.groupingBy(RawRow::getEventType));
        testInsertPerson(grouping.get(EventType.INSERT));
        testUpdatePerson(grouping.get(EventType.UPDATE));
        testDeletePerson(grouping.get(EventType.DELETE));
    }

    private void testInsertPerson(List<RawRow> inserts) {
        ArgumentCaptor<List<IndexQuery>> indexQueryCapture = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);
        canalClient.handleEntries(getEntries(inserts));
        verify(operations, times(1))
                .bulkIndex(indexQueryCapture.capture(), indexCoordinatesCaptor.capture());
        for (int i = 0; i < inserts.size(); i++) {
            assertInserts(indexQueryCapture.getAllValues().get(0).get(i), inserts.get(i));
        }
    }

    private void assertInserts(IndexQuery indexQuery, RawRow row) {
        Map<String, Object> data = (Map<String, Object>) indexQuery.getObject();
        Assertions.assertNotNull(data);
        List<RawColumn> insertedColumns = row.getAfterColumns();
        insertedColumns.forEach(column -> {
            if (column.getName().equals("id")) {
                Assertions.assertEquals(indexQuery.getId(), column.getValue());
            } else if (!column.getMysqlType().equals("timestamp")) {
                Assertions.assertEquals(data.get(column.getName()), column.getValue());
            } else {
                Assertions.assertEquals(dateFormat.format((Date) data.get(column.getName())), column.getValue());
            }
        });
    }

    private void testUpdatePerson(List<RawRow> updates) {
        given(operations.exists(anyString(), any(IndexCoordinates.class))).willReturn(true);
        given(operations.update(any(UpdateQuery.class), any(IndexCoordinates.class))).willReturn(new UpdateResponse(UpdateResponse.Result.UPDATED));
        ArgumentCaptor<List<UpdateQuery>> updateQueryCapture = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);
        canalClient.handleEntries(getEntries(updates));
        verify(operations, times(1))
                .bulkUpdate(updateQueryCapture.capture(), indexCoordinatesCaptor.capture());
        List<UpdateQuery> updateQueries = updateQueryCapture.getAllValues().get(0);
        for (int i = 0; i < updates.size(); i++) {
            RawRow row = updates.get(i);
            UpdateQuery updateQuery = updateQueries.get(i);
            Assertions.assertEquals(updateQuery.getId(), row.getAfterColumnValue("id"));
            Document data = updateQuery.getDocument();
            Assertions.assertNotNull(data);
            List<RawColumn> updatedColumns = row.getChangedAfterColumns();
            updatedColumns.forEach(column -> {
                if (!column.getMysqlType().equals("timestamp")) {
                    Assertions.assertEquals(data.get(column.getName()), column.getValue());
                } else {
                    Assertions.assertEquals(dateFormat.format((Date) data.get(column.getName())), column.getValue());
                }
            });
        }
    }

    private void testDeletePerson(List<RawRow> deletes) {
        ArgumentCaptor<String> deleteCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);
        canalClient.handleEntries(getEntries(deletes));
        verify(operations, times(deletes.size()))
                .delete(deleteCaptor.capture(), indexCoordinatesCaptor.capture());
        for (int i = 0; i < deletes.size(); i++) {
            RawRow row = deletes.get(i);
            String deletedId = deleteCaptor.getAllValues().get(i);
            Assertions.assertEquals(deletedId, row.getBeforeColumnValue("id"));
        }
    }

    @Test
    public void testHandleEntries_personInfo_1() throws Exception {
        reset(operations);
        ArgumentCaptor<List<IndexQuery>> indexQueryCapture = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> personRows = readList("test_data_person_1.json");
        List<RawRow> insertPersonRows = personRows.stream().filter(row -> row.getEventType() == EventType.INSERT).collect(Collectors.toList());
        List<Entry> insertPersonEntries = getEntries(insertPersonRows);
        List<RawRow> fieldRows = readList("test_data_person_info_1.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        List<Entry> entries = new ArrayList<>(insertPersonEntries);
        entries.addAll(fieldEntries);
        canalClient.handleEntries(entries);

        verify(operations, times(1))
                .bulkIndex(indexQueryCapture.capture(), indexCoordinatesCaptor.capture());
        verify(operations, times(0))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());
        List<IndexQuery> indexQueries = indexQueryCapture.getAllValues().get(0);
        for (int i = 0; i < insertPersonEntries.size(); i++) {
            RawRow row = insertPersonRows.get(i);
            IndexQuery indexQuery = indexQueries.get(i);
            assertInserts(indexQuery, row);
            ConstructedProperty prop = esMappingProperties.getMappings().get("person").getConstructedProperties()
                    .stream().filter(p -> p.getName().equals("age")).findFirst().get();
            String sql = prop.getSql();
            Map<String, Object> sqlParams = new HashMap<>();
            sqlParams.put("id", Integer.valueOf(row.getAfterColumnValue("id")));
            List<Map<String, Object>> sqlResult = mysqlRepository.fetch(sql, sqlParams);
            if (!sqlResult.isEmpty()) {
                Integer age = (Integer) sqlResult.get(0).get("age");
                Map<String, Object> data = (Map<String, Object>) indexQuery.getObject();
                Assertions.assertEquals(age, data.get("age"));
            }
        }
    }

    @Test
    public void testHandleEntries_personInfo_2() throws Exception {
        reset(operations);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> fieldRows = readList("test_data_person_info_1.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        given(operations.exists(anyString(), any(IndexCoordinates.class))).willReturn(true);
        canalClient.handleEntries(fieldEntries);
        verify(operations, times(1))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());

        List<UpdateQuery> updateQueries = updateQueryCaptor.getAllValues().get(0);
        String idColumnName = "person_id";
        List<RawRow> deduplicatedRows = new ArrayList<>();
        for (RawRow row : fieldRows) {
            boolean foundPrevious = false;
            for (int i = 0; i < deduplicatedRows.size(); i++) {
                RawRow previousSetRow = deduplicatedRows.get(i);
                if (previousSetRow.getAfterColumnValue(idColumnName).equals(row.getAfterColumnValue(idColumnName))) {
                    deduplicatedRows.set(i, row);
                    foundPrevious = true;
                    break;
                }
            }
            if (!foundPrevious) {
                deduplicatedRows.add(row);
            }
        }
        for (int i = 0; i < deduplicatedRows.size(); i++) {
            RawRow sqlDataRow = deduplicatedRows.get(i);
            UpdateQuery updateQuery = updateQueries.get(i);
            Assertions.assertEquals(1, updateQuery.getDocument().keySet().size());
            Integer age = (Integer) updateQuery.getDocument().get("age");
            Assertions.assertEquals(sqlDataRow.getAfterColumnValue("age"), age.toString());
        }
    }


    @Test
    public void testHandleEntries_event_1() throws Exception {
        reset(operations);
        ArgumentCaptor<List<IndexQuery>> indexQueryCapture = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> personRows = readList("test_data_person_1.json");
        List<RawRow> insertPersonRows = personRows.stream().filter(row -> row.getEventType() == EventType.INSERT).collect(Collectors.toList());
        List<Entry> insertPersonEntries = getEntries(insertPersonRows);
        List<RawRow> fieldRows = readList("test_data_event_1.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        List<Entry> entries = new ArrayList<>(insertPersonEntries);
        entries.addAll(fieldEntries);
        canalClient.handleEntries(entries);

        verify(operations, times(1))
                .bulkIndex(indexQueryCapture.capture(), indexCoordinatesCaptor.capture());
        verify(operations, times(0))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());
        List<IndexQuery> indexQueries = indexQueryCapture.getAllValues().get(0);
        for (int i = 0; i < insertPersonEntries.size(); i++) {
            RawRow row = insertPersonRows.get(i);
            IndexQuery indexQuery = indexQueries.get(i);
            assertInserts(indexQuery, row);
            ConstructedProperty prop = esMappingProperties.getMappings().get("person").getConstructedProperties()
                    .stream().filter(p -> p.getName().equals("events")).findFirst().get();
            String sql = prop.getSql();
            Map<String, Object> sqlParams = new HashMap<>();
            sqlParams.put("id", Integer.valueOf(row.getAfterColumnValue("id")));
            List<Map<String, Object>> sqlResult = mysqlRepository.fetch(sql, sqlParams);
            if (!sqlResult.isEmpty()) {
                int eventSize = sqlResult.size();
                Map<String, Object> data = (Map<String, Object>) indexQuery.getObject();
                Assertions.assertEquals(eventSize, ((List<?>) data.get("events")).size());
            }
        }
    }

    @Test
    public void testHandleEntries_event_2() throws Exception {
        reset(operations);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> fieldRows = readList("test_data_event_1.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        given(operations.exists(anyString(), any(IndexCoordinates.class))).willReturn(true);
        canalClient.handleEntries(fieldEntries);

        verify(operations, times(1))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());
        List<UpdateQuery> updateQueries = updateQueryCaptor.getAllValues().get(0);
        for (int i = 0; i < fieldRows.size(); i++) {
            RawRow row = fieldRows.get(i);
            ConstructedProperty prop = esMappingProperties.getMappings().get("person").getConstructedProperties()
                    .stream().filter(p -> p.getName().equals("events")).findFirst().get();
            String sql = prop.getReconstructionCondition().getRetrieveSql();
            Map<String, Object> sqlParams = new HashMap<>();
            sqlParams.put("person_id", Integer.valueOf(row.getAfterColumnValue("id")));
            List<Map<String, Object>> sqlResult = mysqlRepository.fetch(sql, sqlParams);
            if (!sqlResult.isEmpty()) {
                int eventSize = sqlResult.size();
                UpdateQuery updateQuery = updateQueries.get(i);
                Document data = updateQuery.getDocument();
                Assertions.assertEquals(eventSize, ((List<?>) data.get("events")).size());
            }
        }
    }

    @Test
    public void testHandleEntries_relative_1() throws Exception {
        reset(operations);
        ArgumentCaptor<List<IndexQuery>> indexQueryCapture = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> personRows = readList("test_data_person_1.json");
        List<RawRow> insertPersonRows = personRows.stream().filter(row -> row.getEventType() == EventType.INSERT).collect(Collectors.toList());
        List<Entry> insertPersonEntries = getEntries(insertPersonRows);
        List<RawRow> fieldRows = readList("test_data_relative_1.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        List<Entry> entries = new ArrayList<>(insertPersonEntries);
        entries.addAll(fieldEntries);
        canalClient.handleEntries(entries);

        verify(operations, times(1))
                .bulkIndex(indexQueryCapture.capture(), indexCoordinatesCaptor.capture());
        verify(operations, times(0))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());
        Map<String, List<RawRow>> grouping = fieldRows.stream().collect(Collectors.groupingBy(r -> r.getAfterColumnValue("person_id_1")));
        List<IndexQuery> indexQueries = indexQueryCapture.getAllValues().get(0);
        for (int i = 0; i < insertPersonEntries.size(); i++) {
            RawRow row = insertPersonRows.get(i);
            String idValue = row.getAfterColumnValue("id");
            IndexQuery indexQuery = indexQueries.get(i);
            assertInserts(indexQuery, row);
            ConstructedProperty prop = esMappingProperties.getMappings().get("person").getConstructedProperties()
                    .stream().filter(p -> p.getName().equals("children")).findFirst().get();
            String sql = prop.getSql();
            Map<String, Object> sqlParams = new HashMap<>();
            sqlParams.put("id", Integer.valueOf(idValue));
            List<Map<String, Object>> sqlResult = mysqlRepository.fetch(sql, sqlParams);
            if (!sqlResult.isEmpty()) {
                int childrenCount = sqlResult.size();
                Map<String, Object> data = (Map<String, Object>) indexQuery.getObject();
                Assertions.assertEquals(childrenCount, ((List<?>) data.get("children")).size());
                List<RawRow> allRelations = grouping.get(idValue);
                Assertions.assertEquals(childrenCount, allRelations.stream().filter(r -> r.getAfterColumnValue("relation").equals("CHILD")).count());
            }
        }
    }


    @Test
    public void testHandleEntries_personJob_1() throws Exception {
        reset(operations);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> fieldRows = readList("test_data_person_job_1.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        given(operations.exists(anyString(), any(IndexCoordinates.class))).willReturn(true);
        canalClient.handleEntries(fieldEntries);
        verify(operations, times(1))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());

        ConstructedProperty prop = esMappingProperties.getMappings().get("person").getConstructedProperties()
                .stream().filter(p -> p.getName().equals("job")).findFirst().get();
        List<SimpleProperty> jobFields = prop.getReconstructionCondition().getDataColumns();
        List<UpdateQuery> updateQueries = updateQueryCaptor.getAllValues().get(0);
        for (int i = 0; i < fieldRows.size(); i++) {
            RawRow sqlDataRow = fieldRows.get(i);
            UpdateQuery updateQuery = updateQueries.get(i);
            Assertions.assertEquals(1, updateQuery.getDocument().keySet().size());
            Map<String, Object> esUpdateFields = (Map<String, Object>) updateQuery.getDocument().get("job");
            jobFields.forEach(jobField -> {
                RawColumn column = sqlDataRow.getAfterColumn(jobField.getColumn());
                Assertions.assertEquals(esUpdateFields.get(jobField.getColumn()), jobField.convertType(column.getValue(), column.getMysqlType()));
            });
        }
    }

    @Test
    public void testHandleEntries_personJob_2() throws Exception {
        reset(operations);
        ArgumentCaptor<List<UpdateQuery>> updateQueryCaptor = ArgumentCaptor.forClass(List.class);
        ArgumentCaptor<IndexCoordinates> indexCoordinatesCaptor = ArgumentCaptor.forClass(IndexCoordinates.class);

        List<RawRow> fieldRows = readList("test_data_person_job_2.json");
        List<Entry> fieldEntries = getEntries(fieldRows);
        RawRow row = fieldRows.get(0);
        given(operations.exists(anyString(), any(IndexCoordinates.class))).willReturn(true);
        canalClient.handleEntries(fieldEntries);
        verify(operations, times(1))
                .bulkUpdate(updateQueryCaptor.capture(), indexCoordinatesCaptor.capture());

        ConstructedProperty prop = esMappingProperties.getMappings().get("person").getConstructedProperties()
                .stream().filter(p -> p.getName().equals("job")).findFirst().get();
        List<SimpleProperty> jobFields = prop.getReconstructionCondition().getDataColumns();
        UpdateQuery updateQuery0 = updateQueryCaptor.getAllValues().get(0).get(0);
        // person_id变化，先清空原person_id的job字段，再添加到新person_id的job
        Map<String, Object> esUpdateFields0 = (Map<String, Object>) updateQuery0.getDocument().get("job");
        Assertions.assertNull(esUpdateFields0);
        Assertions.assertEquals(row.getBeforeColumnValue("person_id"), updateQuery0.getId());

        UpdateQuery updateQuery1 = updateQueryCaptor.getAllValues().get(0).get(1);
        Map<String, Object> esUpdateFields1 = (Map<String, Object>) updateQuery1.getDocument().get("job");
        Assertions.assertNotNull(esUpdateFields1);
        jobFields.forEach(jobField -> {
            RawColumn column = row.getAfterColumn(jobField.getColumn());
            Assertions.assertEquals(esUpdateFields1.get(jobField.getColumn()), jobField.convertType(column.getValue(), column.getMysqlType()));
        });
    }
}
