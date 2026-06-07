package rgo.nativekafka.consumer.service.handler;

import rgo.nativekafka.common.api.RequestMessage;

import java.util.List;

public interface DataHandler {

    void handle(List<RequestMessage<String>> messages);
}
