package nameless.canal.es;

import lombok.extern.slf4j.Slf4j;
import nameless.canal.transfer.UpdateObject;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.stereotype.Repository;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Repository
public class EsRepository {
    private final ElasticsearchOperations operations;

    public EsRepository(ElasticsearchOperations elasticsearchOperations) {
        this.operations = elasticsearchOperations;
    }

    public void bulkUpdate(String indexName, List<UpdateObject> updateObjects) {
        List<UpdateQuery> updateQueries = updateObjects.stream().map(o ->
                UpdateQuery.builder(o.getId()).withDocument(Document.from(o)).build())
                .collect(Collectors.toList());
        operations.bulkUpdate(updateQueries, IndexCoordinates.of(indexName));
        log.info("updated {} documents into {}", updateObjects.size(), indexName);
    }

    public void deleteById(String indexName, String id) {
        operations.delete(id, IndexCoordinates.of(indexName));
        log.info("{}.{} deleted", indexName, id);
    }

    public void bulkInsert(String indexName, List<UpdateObject> inserts) {
        List<IndexQuery> indexQueries = inserts.stream().map(o -> {
            IndexQuery query = new IndexQuery();
            query.setId(o.getId());
            query.setObject(new HashMap<>(o));
            return query;
        }).collect(Collectors.toList());
        operations.bulkIndex(indexQueries, IndexCoordinates.of(indexName));
        log.info("{} documents indexed into {}. id list: {}", inserts.size(), indexName,
                String.join(", ", inserts.stream().map(UpdateObject::getId).collect(Collectors.toList())));
    }

    public boolean exists(String indexName, String id) {
        return operations.exists(id, IndexCoordinates.of(indexName));
    }

}
