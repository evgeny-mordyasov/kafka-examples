package rgo.nativekafka.consumer.service.handler;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface DataHandler {

    void handle(ConsumerRecords<Long, String> records);
}
