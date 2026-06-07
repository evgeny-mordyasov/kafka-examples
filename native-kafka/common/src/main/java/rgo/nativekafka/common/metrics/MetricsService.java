package rgo.nativekafka.common.metrics;

public class MetricsService {

    private final CustomMetricsProcessor cmp;

    public MetricsService(CustomMetricsProcessor cmp) {
        this.cmp = cmp;
        initMetrics();
    }

    private void initMetrics() {
        reportIncomingMessages(0);
        updateFailedConsumerPollMetric("request_topic", 0);

        reportConsumerResumed();
        reportConsumerLagSeconds("", 0);
        reportInFlightVectorsCount(0);
    }

    public void reportIncomingMessages(int count) {
        cmp.processCustomCounterMetrics(
                "kafka_incoming",
                "",
                "",
                "request_topic",
                count
        );
    }

    public void incFailedConsumerPollMainTopic() {
        updateFailedConsumerPollMetric("request_topic", 1);
    }

    private void updateFailedConsumerPollMetric(String topic, int count) {
        cmp.processCustomCounterMetrics(
                "consumer_poll",
                "FAIL",
                "",
                topic,
                count
        );
    }

    public void reportConsumerPaused() {
        updateConsumerStateMetric(0);
    }

    public void reportConsumerResumed() {
        updateConsumerStateMetric(1);
    }

    private void updateConsumerStateMetric(int flag) {
        cmp.processCustomGaugeMetrics(
                "consumer_status",
                "",
                "",
                "request_topic",
                flag,
                Integer::doubleValue
        );
    }

    public void reportConsumerLagSeconds(String topic, long seconds) {
        cmp.processCustomGaugeMetrics(
                "lag_sec",
                "",
                "",
                topic,
                seconds,
                Long::doubleValue
        );
    }

    public void reportInFlightVectorsCount(int size) {
        cmp.processCustomGaugeMetrics(
                "in_flight_vectors",
                "",
                "",
                "request_topic",
                size,
                Integer::doubleValue
        );
    }
}
