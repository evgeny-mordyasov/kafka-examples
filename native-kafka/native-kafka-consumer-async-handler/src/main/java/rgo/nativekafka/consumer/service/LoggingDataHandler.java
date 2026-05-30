package rgo.nativekafka.consumer.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.consumer.api.RequestMessage;
import rgo.nativekafka.consumer.kafka.utils.StringifyUtils;

import java.util.List;
import java.util.concurrent.Future;

public class LoggingDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataHandler.class);

    @Override
    public Future<Void> handle(List<RequestMessage<String>> messages) {
        messages.forEach(message -> LOGGER.info(StringifyUtils.briefMessage(message)));
        return null;
    }
}
