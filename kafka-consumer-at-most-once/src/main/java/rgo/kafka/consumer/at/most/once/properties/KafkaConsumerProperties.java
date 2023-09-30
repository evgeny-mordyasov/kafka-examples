package rgo.kafka.consumer.at.most.once.properties;

import java.util.Map;

public class KafkaConsumerProperties {

    private String topic;
    private long waitPollMs;
    private Map<String, Object> properties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getWaitPollMs() {
        return waitPollMs;
    }

    public void setWaitPollMs(long waitPollMs) {
        this.waitPollMs = waitPollMs;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
