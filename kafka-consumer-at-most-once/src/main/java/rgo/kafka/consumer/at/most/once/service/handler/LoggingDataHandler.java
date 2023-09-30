package rgo.kafka.consumer.at.most.once.service.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LoggingDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataHandler.class);

    @Override
    public void handle(List<ConsumerRecord<Long, String>> data) {
        data.forEach(record ->
                LOGGER.info("Handle message. offset={}, value={}", record.offset(), record.value()));
    }
}