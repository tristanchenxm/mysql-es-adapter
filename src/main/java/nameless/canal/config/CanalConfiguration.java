package nameless.canal.config;

import nameless.canal.client.ConfiguredDirectCanalConnector;
import nameless.canal.client.ConfiguredKafkaCanalConnector;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({
        DirectCanalClientProperties.class,
        KafkaCanalClientProperties.class,
        EsMappingProperties.class
})
public class CanalConfiguration {

    @ConditionalOnProperty(name = "canal.client-type", havingValue = "KAFKA_CLIENT")
    @Bean
    public ConfiguredKafkaCanalConnector configuredKafkaCanalConnector(KafkaCanalClientProperties properties) {
        return new ConfiguredKafkaCanalConnector(properties);
    }

    @ConditionalOnProperty(name = "canal.client-type", havingValue = "DIRECT_CLIENT")
    @Bean
    public ConfiguredDirectCanalConnector configuredDirectCanalConnector(DirectCanalClientProperties properties) {
        return new ConfiguredDirectCanalConnector(properties);
    }
}
