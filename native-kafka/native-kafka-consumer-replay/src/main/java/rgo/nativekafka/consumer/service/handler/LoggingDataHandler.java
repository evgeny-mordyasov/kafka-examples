package rgo.nativekafka.consumer.service.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rgo.nativekafka.common.api.RequestMessage;
import rgo.nativekafka.common.kafka.StringifyUtils;

import java.util.List;

public class LoggingDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(LoggingDataHandler.class);

    @Override
    public void handle(List<RequestMessage<String>> messages) {
        messages.forEach(message -> LOGGER.info(StringifyUtils.briefMessage(message)));
    }
}
