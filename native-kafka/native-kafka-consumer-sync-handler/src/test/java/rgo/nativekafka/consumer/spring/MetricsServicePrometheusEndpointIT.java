package rgo.nativekafka.consumer.spring;

import org.junit.jupiter.api.Test;
import org.springframework.test.web.servlet.client.EntityExchangeResult;

import static org.assertj.core.api.Assertions.assertThat;

class MetricsServicePrometheusEndpointIT extends IntegrationTest {

    @Test
    void customCountStatsTotal_exists() {
        EntityExchangeResult<String> response = client
                .get()
                .uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult();

        assertThat(response.getResponseBody())
                .contains("custom_count_stats_total{direction=\"\",method=\"kafka_incoming\",status=\"\",type=\"request_topic\"}")
                .contains("custom_count_stats_total{direction=\"\",method=\"consumer_poll\",status=\"FAIL\",type=\"request_topic\"}");
    }

    @Test
    void customGaugeStats_exists() {
        EntityExchangeResult<String> response = client
                .get()
                .uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult();

        assertThat(response.getResponseBody())
                .contains("custom_gauge_stats{direction=\"\",id=\"\",method=\"consumer_status\",status=\"\",type=\"request_topic\"}")
                .contains("custom_gauge_stats{direction=\"\",id=\"\",method=\"lag_sec\",status=\"\",type=\"\"}")
                .contains("custom_gauge_stats{direction=\"\",id=\"\",method=\"in_flight_vectors\",status=\"\",type=\"request_topic\"}");
    }
}
