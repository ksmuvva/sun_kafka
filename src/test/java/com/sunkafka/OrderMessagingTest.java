package com.sunkafka;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.consumer.OrderConsumer.Delivery;
import com.sunkafka.model.OrderEvent;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

/** Happy-path publisher -> broker -> consumer behaviour. */
class OrderMessagingTest extends KafkaE2ETestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    @Test
    void publishedEventIsConsumed() {
        var event = valid(uniqueId());

        producer.send(event).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
    }

    @Test
    void eventsWithSameKeyStayOrderedOnOnePartition() {
        var id = uniqueId();
        List<OrderEvent> sent = IntStream.rangeClosed(1, 20)
                .mapToObj(i -> withAmount(id, i)).toList();

        sent.forEach(e -> producer.send(e).join());

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getDeliveries()).hasSize(20));
        List<Delivery> deliveries = consumer.getDeliveries();
        assertThat(deliveries.stream().map(Delivery::event)).containsExactlyElementsOf(sent);
        assertThat(deliveries.stream().map(Delivery::partition).collect(Collectors.toSet())).hasSize(1);
    }

    @Test
    void differentKeysSpreadAcrossPartitions() {
        IntStream.range(0, 30).forEach(i -> producer.send(valid(uniqueId())).join());

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getDeliveries()).hasSize(30));
        Set<Integer> partitions = consumer.getDeliveries().stream()
                .map(Delivery::partition).collect(Collectors.toSet());
        assertThat(partitions).hasSizeGreaterThan(1);
    }
}
