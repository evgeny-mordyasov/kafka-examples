package rgo.nativekafka.consumer.spring.properties;

import java.util.Map;

public class KafkaConsumerProperties {

    private String topic;
    private long checkTimeoutMillis;
    private long pollTimeoutMillis;
    private long closeTimeoutMillis;
    private Map<String, Object> properties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getCheckTimeoutMillis() {
        return checkTimeoutMillis;
    }

    public void setCheckTimeoutMillis(long checkTimeoutMillis) {
        this.checkTimeoutMillis = checkTimeoutMillis;
    }

    public long getPollTimeoutMillis() {
        return pollTimeoutMillis;
    }

    public void setPollTimeoutMillis(long pollTimeoutMillis) {
        this.pollTimeoutMillis = pollTimeoutMillis;
    }

    public long getCloseTimeoutMillis() {
        return closeTimeoutMillis;
    }

    public void setCloseTimeoutMillis(long closeTimeoutMillis) {
        this.closeTimeoutMillis = closeTimeoutMillis;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
