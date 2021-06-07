package nameless.canal.client;

import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;

import java.util.List;
import java.util.concurrent.TimeUnit;

public interface ConfiguredCanalConnector {
    TimeUnit TIME_UNIT = TimeUnit.MICROSECONDS;

    void connect() throws CanalClientException;

    void disconnect() throws CanalClientException;

    void subscribe() throws CanalClientException;

    Message getWithoutAck() throws CanalClientException;

    List<Message> getListWithoutAck(Long timeout, TimeUnit unit) throws CanalClientException;

    List<Message> getListWithoutAck() throws CanalClientException;

    void ack() throws CanalClientException;

    void rollback() throws CanalClientException;
}
