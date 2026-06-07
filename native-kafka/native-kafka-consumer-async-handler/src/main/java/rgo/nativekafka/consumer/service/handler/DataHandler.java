package rgo.nativekafka.consumer.service.handler;

import rgo.nativekafka.common.api.RequestMessage;

import java.util.List;
import java.util.concurrent.Future;

public interface DataHandler {

    Future<Void> handle(List<RequestMessage<String>> messages);
}
