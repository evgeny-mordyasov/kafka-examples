package rgo.nativekafka.consumer.service.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.common.api.RequestMessage;
import rgo.nativekafka.common.kafka.StringifyUtils;

public class LoggingDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataHandler.class);

    @Override
    public void handle(RequestMessage<String> message) {
        LOGGER.info(StringifyUtils.briefMessage(message));
    }
}
