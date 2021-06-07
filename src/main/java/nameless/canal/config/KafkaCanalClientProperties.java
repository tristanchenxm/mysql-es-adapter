package nameless.canal.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

@ConfigurationProperties(prefix = "canal.kafka-client")
@Getter
@Setter
public class KafkaCanalClientProperties {
    @NotEmpty
    private String servers;
    @NotEmpty
    private String topic;
    @NotNull
    private Integer partition;
    @NotEmpty
    private String groupId;
    private int batchSize = 1000;
    /**
     * 拉取binlog的最长等待时间
     */
    private long pollingTimeout = 1000L;
}
