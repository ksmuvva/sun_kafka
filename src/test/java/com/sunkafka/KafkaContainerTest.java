package com.sunkafka;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.producer.OrderProducer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/** Same round trip against a real Kafka broker. Skipped automatically when Docker is unavailable. */
@SpringBootTest
@Testcontainers(disabledWithoutDocker = true)
class KafkaContainerTest {

    @Container
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;

    @Test
    void roundTripAgainstRealBroker() {
        var event = valid(uniqueId());

        producer.send(event).join();

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
    }
}
