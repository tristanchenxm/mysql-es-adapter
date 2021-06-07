package nameless.canal.controller;

import nameless.canal.transfer.MysqlToElasticsearchService;
import nameless.canal.transfer.TransferResult;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class OperationController {
    private final MysqlToElasticsearchService mysqlToElasticsearchService;

    public OperationController(MysqlToElasticsearchService mysqlToElasticsearchService) {
        this.mysqlToElasticsearchService = mysqlToElasticsearchService;
    }

    @Data
    public static class QueryCondition {
        private String condition;
    }

    @PostMapping("/load/{table}")
    public TransferResult loadIndex(@PathVariable String table,
                                    @RequestBody QueryCondition condition) {
        return mysqlToElasticsearchService.loadIndex(table, condition == null ? "" : condition.condition);
    }

}
