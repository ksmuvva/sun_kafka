package com.sunkafka.karate;

import static org.assertj.core.api.Assertions.assertThat;

import com.intuit.karate.Results;
import com.intuit.karate.Runner;
import com.sunkafka.support.LinkingStub;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Proof of concept: Karate (Gherkin-style API tests) against the real running application.
 * Starts the app on a random HTTP port with an embedded Kafka broker, then runs every feature under
 * {@code src/test/resources/karate}. Karate drives the HTTP API and, through {@link KafkaSupport}, Kafka itself.
 * Reports: target/karate-reports/karate-summary.html
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://sun-kafka",
        "linking.connect-timeout=500ms",
        "linking.read-timeout=500ms"})
@EmbeddedKafka(partitions = 3, topics = {"orders", "orders.DLT"})
@ActiveProfiles("test")
class KarateTest {

    @LocalServerPort int port;
    @Autowired EmbeddedKafkaBroker kafka;

    @DynamicPropertySource
    static void linkingService(DynamicPropertyRegistry registry) {
        registry.add("linking.base-url", LinkingStub::baseUrl);
    }

    @Test
    void karateFeatures() {
        KafkaSupport.init(kafka.getBrokersAsString());
        System.setProperty("karate.baseUrl", "http://localhost:" + port);

        // one thread: scenarios share one fake linking service, so they must not overlap
        Results results = Runner.path("classpath:karate").parallel(1);

        assertThat(results.getFailCount()).as(results.getErrorMessages()).isZero();
        assertThat(results.getScenariosTotal()).isGreaterThan(0);
    }
}
