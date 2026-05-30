package rgo.nativekafka.consumer.service;

import rgo.nativekafka.consumer.api.RequestMessage;

import java.util.List;
import java.util.concurrent.Future;

public interface DataHandler {

    Future<Void> handle(List<RequestMessage<String>> messages);
}
