package nameless.canal.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import javax.validation.constraints.NotEmpty;

@ConfigurationProperties(prefix = "canal.direct-client")
@Getter
@Setter
public class DirectCanalClientProperties {
    private String host;
    private String zkServers;
    private String username;
    private String password;
    @NotEmpty
    private String subscribe;
    @NotEmpty
    private String destination;
    /**
     *
     */
    private int batchSize = 1000;
    /**
     * 拉取binlog的最长等待时间
     */
    private long pollingTimeout = 100L;
}
