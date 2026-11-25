package com.sunkafka.bdd;

import com.sunkafka.support.LinkingStub;
import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/** Same environment as the JUnit E2E tests: embedded Kafka broker, in-memory Schema Registry and database, fake linking service. */
@CucumberContextConfiguration
@SpringBootTest(classes = com.sunkafka.SunKafkaApplication.class, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://sun-kafka",
        "linking.connect-timeout=500ms",
        "linking.read-timeout=500ms"})
@EmbeddedKafka(partitions = 3, topics = {"orders", "orders.DLT"})
@ActiveProfiles("test")
public class CucumberSpringConfiguration {

    @DynamicPropertySource
    static void linkingService(DynamicPropertyRegistry registry) {
        registry.add("linking.base-url", LinkingStub::baseUrl);
    }
}
