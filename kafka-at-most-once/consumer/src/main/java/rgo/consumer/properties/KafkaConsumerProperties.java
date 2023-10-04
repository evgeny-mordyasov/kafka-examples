package rgo.consumer.properties;

import java.util.Map;

public class KafkaConsumerProperties {

    private String topic;
    private long timeoutPollMs;
    private int threadPoolSize;
    private long timeoutSleepMs;
    private Map<String, Object> properties;

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public long getTimeoutPollMs() {
        return timeoutPollMs;
    }

    public void setTimeoutPollMs(long timeoutPollMs) {
        this.timeoutPollMs = timeoutPollMs;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public long getTimeoutSleepMs() {
        return timeoutSleepMs;
    }

    public void setTimeoutSleepMs(long timeoutSleepMs) {
        this.timeoutSleepMs = timeoutSleepMs;
    }

    public Map<String, Object> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, Object> properties) {
        this.properties = properties;
    }
}
