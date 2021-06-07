package nameless.canal.client;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import nameless.canal.config.KafkaCanalClientProperties;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class ConfiguredKafkaCanalConnector implements ConfiguredCanalConnector {

    private final KafkaCanalConnector connector;

    private final long pollTimeout;

    private final LinkedList<Message> messageBuffer;

    public ConfiguredKafkaCanalConnector(KafkaCanalClientProperties properties) {
        connector = new KafkaCanalConnector(properties.getServers(), properties.getTopic(),
                properties.getPartition(), properties.getGroupId(), properties.getBatchSize(), false);
        messageBuffer = new LinkedList<>();
        pollTimeout = properties.getPollingTimeout();
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
        connector.subscribe();
    }

    public Message getWithoutAck() throws CanalClientException {
        if (messageBuffer.isEmpty()) {
            messageBuffer.addAll(getListWithoutAck(pollTimeout, TIME_UNIT));
        }
        return messageBuffer.poll();
    }

    @Override
    public List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException {
        if (!messageBuffer.isEmpty()) {
            List<Message> messages = new ArrayList<>(messageBuffer);
            messageBuffer.clear();
            return messages;
        }
        return connector.getListWithoutAck(timeout, unit);
    }

    public List<Message> getListWithoutAck() throws CanalClientException {
        return getListWithoutAck(pollTimeout, TIME_UNIT);
    }

    @Override
    public void ack() throws CanalClientException {
        /*
         * 如果是messageBuffer不为空，即通过 {@link #getWithoutAck()} 方式拉取的Message还未消费完，
         * 则什么都不做，直到所有message都消费完后才发送ack
         */
        if (messageBuffer.isEmpty()) {
            connector.ack();
        }
    }

    @Override
    public void rollback() throws CanalClientException {
        messageBuffer.clear();
        connector.rollback();
    }
}
