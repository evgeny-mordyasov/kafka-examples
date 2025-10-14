package rgo.nativekafka.consumer.service.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LoggingDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataHandler.class);

    @Override
    public void handle(List<ConsumerRecord<Long, String>> data) {
        data.forEach(message ->
                LOGGER.info("Handle message: t-p@o:{}-{}@{}, k:{}, v:{}, h:{}",
                        message.topic(), message.partition(), message.offset(), message.key(), message.value(), message.headers()));
    }
}
