package rgo.kafka.consumer.at.most.once.service.handler;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

public interface DataHandler {

    void handle(List<ConsumerRecord<Long, String>> data);
}
