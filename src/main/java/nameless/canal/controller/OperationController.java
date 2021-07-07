package nameless.canal.controller;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import nameless.canal.transfer.MysqlToElasticsearchService;
import nameless.canal.transfer.TransferResult;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

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
        @Min(1)
        @Max(10000)
        private int batchSize = 1000;
    }

    @PostMapping("/load/{table}")
    public TransferResult loadIndex(@PathVariable String table,
                                    @Validated @RequestBody QueryCondition condition) {
        return mysqlToElasticsearchService.loadIndex(table, condition.condition, condition.batchSize);
    }

}
