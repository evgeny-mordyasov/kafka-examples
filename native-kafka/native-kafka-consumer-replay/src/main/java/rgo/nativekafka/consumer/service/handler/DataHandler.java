package rgo.nativekafka.consumer.service.handler;

import rgo.nativekafka.common.api.RequestMessage;

public interface DataHandler {

    void handle(RequestMessage<String> message);
}
