package rgo.nativekafka.consumer.service.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataHandler.class);

    @Override
    public void handle(ConsumerRecords<Long, String> records) {
        records.forEach(message ->
                LOGGER.info("Handle message: t-p@o:{}-{}@{}, k:{}, v:{}, h:{}",
                        message.topic(), message.partition(), message.offset(), message.key(), message.value(), message.headers()));
    }
}
