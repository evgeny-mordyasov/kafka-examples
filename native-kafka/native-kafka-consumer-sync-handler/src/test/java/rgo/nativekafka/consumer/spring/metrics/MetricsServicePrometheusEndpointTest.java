package rgo.nativekafka.consumer.spring.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.client.EntityExchangeResult;
import org.springframework.test.web.servlet.client.RestTestClient;
import rgo.nativekafka.consumer.kafka.consumer.NativeConsumer;
import rgo.nativekafka.consumer.spring.Main;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
        classes = Main.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
class MetricsServicePrometheusEndpointTest {

    @LocalServerPort int port;
    private RestTestClient client;

    @BeforeEach
    void setup() {
        client = RestTestClient
                .bindToServer()
                .baseUrl("http://localhost:" + port)
                .build();
    }

    @MockitoBean(name = "consumerAtLeastOnce")
    private NativeConsumer consumerAtLeastOnce;

    @Test
    void prometheusEndpoint_ShouldExposeMetricsServiceMetrics() {
        EntityExchangeResult<String> response = client
                .get()
                .uri("/actuator/prometheus")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult();

        assertThat(response.getResponseBody())
                .contains("custom_count_stats_total{direction=\"\",method=\"kafka_incoming\",status=\"\",type=\"request_topic\"}")
                .contains("custom_count_stats_total{direction=\"\",method=\"consumer_poll\",status=\"FAIL\",type=\"request_topic\"}")
                .contains("custom_gauge_stats{direction=\"\",id=\"\",method=\"consumer_status\",status=\"\",type=\"request_topic\"}")
                .contains("custom_gauge_stats{direction=\"\",id=\"\",method=\"lag_sec\",status=\"\",type=\"\"}")
                .contains("custom_gauge_stats{direction=\"\",id=\"\",method=\"in_flight_vectors\",status=\"\",type=\"request_topic\"}");
    }
}
