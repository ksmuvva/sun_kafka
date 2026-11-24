package com.sunkafka;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.producer.OrderProducer;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.test.context.EmbeddedKafka;

/** End-to-end publisher -> broker -> consumer test using an in-JVM broker (no Docker needed). */
@SpringBootTest(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka(partitions = 3, topics = "orders")
class OrderMessagingTest {

    @Autowired OrderProducer producer;
    @Autowired OrderConsumer consumer;

    @Test
    void publishedEventIsConsumed() {
        var event = new OrderEvent("o-1", "alice", 42.5);

        producer.send(event).join();

        await().atMost(Duration.ofSeconds(15))
                .untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
    }
}
