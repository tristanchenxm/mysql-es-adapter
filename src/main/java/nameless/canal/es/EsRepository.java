package nameless.canal.es;

import lombok.extern.slf4j.Slf4j;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.*;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;

@Slf4j
@Repository
public class EsRepository {
    private final ElasticsearchOperations operations;

    public EsRepository(ElasticsearchOperations elasticsearchOperations) {
        this.operations = elasticsearchOperations;
    }

    public void updateById(String indexName, String id, Map<String, Object> updateFields) {
        UpdateQuery updateQuery = UpdateQuery.builder(id).withDocument(Document.from(updateFields)).build();
        UpdateResponse updateResponse = operations.update(updateQuery, IndexCoordinates.of(indexName));
        log.info("update result of {}.{}: {}", indexName, id, updateResponse.getResult());
    }

    public void deleteById(String indexName, String id) {
        operations.delete(id, IndexCoordinates.of(indexName));
        log.info("{}.{} deleted", indexName, id);
    }

    public void insert(String indexName, String id, Map<String, Object> fields) {
        IndexQuery query = new IndexQuery();
        query.setId(id);
        query.setObject(fields);
        operations.index(query, IndexCoordinates.of(indexName));
        log.info("{}.{} inserted", indexName, id);
    }

    public boolean exists(String indexName, String id) {
        return operations.exists(id, IndexCoordinates.of(indexName));
    }

    public void bulkIndex(String indexName, List<IndexQuery> queries) {
        operations.bulkIndex(queries, IndexCoordinates.of(indexName));
    }
}
