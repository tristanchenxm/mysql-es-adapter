基于canal的同步MySQL数据到Elasticsearch的小工具
* 配置文件application.yml. 请参照application-sample.yml文件
* 开始之前先建好elasticsearch index 
* 执行全表导入
  ```bash
  curl -X POST 'localhost:8080/load/{mysql-table-name}' -H 'Content-Type: application/json' -d '{}'
  ```
