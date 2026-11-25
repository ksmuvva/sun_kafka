package com.sunkafka.bdd;

import io.cucumber.spring.CucumberContextConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/** Same environment as the JUnit E2E tests: embedded Kafka broker + in-memory Schema Registry. */
@CucumberContextConfiguration
@SpringBootTest(classes = com.sunkafka.SunKafkaApplication.class, properties = {
        "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.properties.schema.registry.url=mock://sun-kafka"})
@EmbeddedKafka(partitions = 3, topics = {"orders", "orders.DLT"})
public class CucumberSpringConfiguration {
}
