package rgo.nativekafka.common.metrics;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

class MetricsServiceTest {

    @Test
    void createInstance_withValidProcessor_ShouldInitializeMetrics() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);

        new MetricsService(processor);

        verify(processor).processCustomCounterMetrics("kafka_incoming", "", "", "request_topic", 0);
        verify(processor).processCustomCounterMetrics("consumer_poll", "FAIL", "", "request_topic", 0);
        verify(processor).processCustomGaugeMetrics(eq("consumer_status"), eq(""), eq(""), eq("request_topic"), eq(1), any());
        verify(processor).processCustomGaugeMetrics(eq("lag_sec"), eq(""), eq(""), eq(""), eq(0L), any());
        verify(processor).processCustomGaugeMetrics(eq("in_flight_vectors"), eq(""), eq(""), eq("request_topic"), eq(0), any());
    }

    @Test
    void reportIncomingMessages_withCount_ShouldIncrementKafkaIncomingMetric() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);
        MetricsService service = new MetricsService(processor);
        reset(processor);

        service.reportIncomingMessages(3);

        verify(processor).processCustomCounterMetrics("kafka_incoming", "", "", "request_topic", 3);
    }

    @Test
    void incFailedConsumerPoll_ShouldIncrementFailedPollMetric() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);
        MetricsService service = new MetricsService(processor);
        reset(processor);

        service.incFailedConsumerPoll();

        verify(processor).processCustomCounterMetrics("consumer_poll", "FAIL", "", "request_topic", 1);
    }

    @Test
    void reportConsumerPaused_ShouldSetConsumerStatusToZero() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);
        MetricsService service = new MetricsService(processor);
        reset(processor);

        service.reportConsumerPaused();

        verify(processor).processCustomGaugeMetrics(eq("consumer_status"), eq(""), eq(""), eq("request_topic"), eq(0), any());
    }

    @Test
    void reportConsumerLagSeconds_withTopicAndSeconds_ShouldSetLagMetric() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);
        MetricsService service = new MetricsService(processor);
        reset(processor);

        service.reportConsumerLagSeconds("topic-a", 12);

        verify(processor).processCustomGaugeMetrics(eq("lag_sec"), eq(""), eq(""), eq("topic-a"), eq(12L), any());
    }

    @Test
    void reportInFlightVectorsCount_withSize_ShouldSetInFlightMetric() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);
        MetricsService service = new MetricsService(processor);
        reset(processor);

        service.reportInFlightVectorsCount(5);

        verify(processor).processCustomGaugeMetrics(eq("in_flight_vectors"), eq(""), eq(""), eq("request_topic"), eq(5), any());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    void reportConsumerResumed_ShouldUseGaugeFunctionReturningOne() {
        CustomMetricsProcessor processor = mock(CustomMetricsProcessor.class);
        MetricsService service = new MetricsService(processor);
        reset(processor);
        ArgumentCaptor<Integer> flagCaptor = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<java.util.function.ToDoubleFunction> functionCaptor = ArgumentCaptor.forClass(java.util.function.ToDoubleFunction.class);

        service.reportConsumerResumed();

        verify(processor).processCustomGaugeMetrics(
                eq("consumer_status"),
                eq(""),
                eq(""),
                eq("request_topic"),
                flagCaptor.capture(),
                functionCaptor.capture());
        assertThat(functionCaptor.getValue().applyAsDouble(flagCaptor.getValue())).isOne();
    }
}
