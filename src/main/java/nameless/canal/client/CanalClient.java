package nameless.canal.client;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.Message;
import nameless.canal.transfer.MysqlToElasticsearchService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@Slf4j
public class CanalClient {

    private volatile boolean keepRunning = true;
    /**
     * 当发生不可预料Exception时，重试初始时间间隔。1 s
     */
    private static final long INITIAL_DELAY_ON_ERROR = 1000L;
    /**
     * 当发生不可预料Exception时，最大重试时间间隔。1 hour
     */
    private static final long MAX_DELAY_INTERVAL = 3600000L; // 1 hour

    private final ConfiguredCanalConnector connector;
    private final MysqlToElasticsearchService mysqlToElasticsearchService;

    public CanalClient(ConfiguredCanalConnector connector,
                       MysqlToElasticsearchService mysqlToElasticsearchService) {
        this.connector = connector;
        this.mysqlToElasticsearchService = mysqlToElasticsearchService;
    }

    public synchronized void run() {
        try {
            connector.connect();
            connector.subscribe();
            while (keepRunning) {
                List<Message> messages = connector.getListWithoutAck();
                if (!messages.isEmpty()) {
                    List<Entry> entries = messages.stream().flatMap(m -> m.getEntries().stream()).collect(Collectors.toList());
                    handleEntries(entries);
                }
            }
        } finally {
            log.info("stop auto syncing data");
            connector.disconnect();
        }
    }

    public void handleEntries(List<Entry> entries) {
        long delay = INITIAL_DELAY_ON_ERROR;
        while (keepRunning) {
            try {
                mysqlToElasticsearchService.handleRowChanges(entries);
                connector.ack();
                break;
            } catch (Exception e) {
                log.error("An error occurred on processing bin log", e);
                sleep(delay);
                delay = delay > MAX_DELAY_INTERVAL ? delay : delay * 2;
            }
        }
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            log.error("Sleep thread interrupted", e);
        }
    }

    public void shutdown() {
        log.info("Got stop signal");
        keepRunning = false;
        synchronized (this) {
            log.info("STOPPED");
        }
    }

}
