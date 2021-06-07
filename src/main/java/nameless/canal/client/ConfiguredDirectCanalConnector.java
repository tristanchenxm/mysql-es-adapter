package nameless.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import nameless.canal.config.DirectCanalClientProperties;
import org.apache.commons.lang3.math.NumberUtils;

//import javax.annotation.concurrent.NotThreadSafe;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

//@NotThreadSafe
public class ConfiguredDirectCanalConnector implements ConfiguredCanalConnector {
    private final CanalConnector connector;
    private final String subscribe;
    private final int batchSize;
    private final long pollingTimeout;
    private final long BATCH_ID_FOR_NULL = -1;
    private long cursorBatchId = BATCH_ID_FOR_NULL;

    public ConfiguredDirectCanalConnector(DirectCanalClientProperties properties) {
        if (properties.getHost() == null && properties.getZkServers() == null) {
            throw new CanalClientException("canal.canal-client.host or canal.canal-client.zk-servers must not be empty");
        }
        if (properties.getHost() != null) {
            if (!properties.getHost().contains(",")) {
                connector = CanalConnectors.newSingleConnector(
                        parseAddress(properties.getHost()),
                        properties.getDestination(),
                        properties.getUsername(),
                        properties.getPassword()
                );
            } else {
                connector = CanalConnectors.newClusterConnector(
                        Arrays.stream(properties.getHost().split(",")).map(this::parseAddress).collect(Collectors.toList()),
                        properties.getDestination(),
                        properties.getUsername(),
                        properties.getPassword()
                );
            }
        } else {
            connector = CanalConnectors.newClusterConnector(
                    properties.getZkServers(),
                    properties.getDestination(),
                    properties.getUsername(),
                    properties.getPassword());
        }

        this.subscribe = properties.getSubscribe();
        this.batchSize = properties.getBatchSize();
        this.pollingTimeout = properties.getPollingTimeout();
    }

    private InetSocketAddress parseAddress(String address) {
        if (address.contains(":")) {
            String[] parts = address.split(":");
            if (NumberUtils.isDigits(parts[parts.length - 1])) {
                int port = Integer.parseInt(parts[parts.length - 1]);
                String host = address.substring(0, address.length() - parts[parts.length - 1].length() - 1);
                return new InetSocketAddress(host, port);
            }
        }
        return new InetSocketAddress(address, 11111);
    }

    @Override
    public void connect() throws CanalClientException {
        connector.connect();
    }

    @Override
    public void disconnect() throws CanalClientException {
        connector.disconnect();
    }

    @Override
    public void subscribe() throws CanalClientException {
        connector.subscribe(subscribe);
    }

    @Override
    public Message getWithoutAck() throws CanalClientException {
        Message message = preProcess(connector.getWithoutAck(batchSize));
        if (message == null) {
            sleep(TIME_UNIT.toMillis(pollingTimeout));
        }
        return message;
    }

    private void sleep(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            throw new CanalClientException("Thread interrupted", e);
        }
    }

    private Message preProcess(Message message) {
        if (message == null || message.getId() == BATCH_ID_FOR_NULL) {
            cursorBatchId = BATCH_ID_FOR_NULL;
            return null;
        } else {
            cursorBatchId = message.getId();
            return message;
        }
    }

    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        Message message = preProcess(connector.getWithoutAck(batchSize, timeout, unit));
        return message == null ? Collections.emptyList() : Collections.singletonList(message);
    }

    @Override
    public List<Message> getListWithoutAck() throws CanalClientException {
        Message message = preProcess(connector.getWithoutAck(batchSize));
        if (message == null) {
            sleep(TIME_UNIT.toMillis(pollingTimeout));
            return Collections.emptyList();
        } else {
            return Collections.singletonList(message);
        }
    }

    @Override
    public void ack() throws CanalClientException {
        if (cursorBatchId != BATCH_ID_FOR_NULL) {
            connector.ack(cursorBatchId);
            cursorBatchId = BATCH_ID_FOR_NULL;
        }
    }

    @Override
    public void rollback() throws CanalClientException {
        if (cursorBatchId != BATCH_ID_FOR_NULL) {
            connector.rollback(cursorBatchId);
            cursorBatchId = BATCH_ID_FOR_NULL;
        }
    }

}
