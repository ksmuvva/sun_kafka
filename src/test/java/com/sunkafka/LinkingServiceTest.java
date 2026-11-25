package com.sunkafka;

import static com.sunkafka.support.OrderEvents.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.sunkafka.support.LinkingStub;
import com.sunkafka.support.TopicReader;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/** What happens between the consumer and the (fake) EU linking service. */
class LinkingServiceTest extends KafkaE2ETestBase {

    private static final Duration TIMEOUT = Duration.ofSeconds(20);

    @Test
    void validOrderCallsTheLinkingServiceExactlyOnce() {
        var event = valid(uniqueId());

        producer.send(event).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
        var calls = LinkingStub.requestsFor(event.getOrderId());
        assertThat(calls).hasSize(1);
        assertThat(calls.get(0).getBodyAsString()).contains("alice").contains("USD");
        // the idempotency key identifies the Kafka record: topic-partition-offset
        assertThat(calls.get(0).getHeader("Idempotency-Key")).matches("orders-\\d+-\\d+");
        // and the record is remembered in the database
        assertThat(store.countForOrder(event.getOrderId())).isEqualTo(1);
    }

    @Test
    void serverErrorIsRetriedThenDeadLettered() {
        var event = valid(uniqueId());
        LinkingStub.failWith(event.getOrderId(), 503);
        try (var dlt = new TopicReader(brokers, "orders.DLT")) {
            producer.send(event).join();

            assertThat(dlt.await(event.getOrderId(), 1, TIMEOUT)).hasSize(1);
        }
        // 1 initial attempt + 2 retries, each one reached the linking service
        assertThat(consumer.attemptsFor(event.getOrderId())).isEqualTo(3);
        assertThat(LinkingStub.requestsFor(event.getOrderId())).hasSize(3);
        assertThat(consumer.getReceived()).noneMatch(e -> e.getOrderId().equals(event.getOrderId()));
        assertThat(store.countForOrder(event.getOrderId())).isZero();
    }

    @Test
    void tooManyRequestsIsTreatedAsTemporary() {
        var event = valid(uniqueId());
        LinkingStub.failWith(event.getOrderId(), 429);
        try (var dlt = new TopicReader(brokers, "orders.DLT")) {
            producer.send(event).join();

            assertThat(dlt.await(event.getOrderId(), 1, TIMEOUT)).hasSize(1);
        }
        assertThat(LinkingStub.requestsFor(event.getOrderId())).hasSize(3);
    }

    @Test
    void temporaryFailureRecoversWithoutDeadLettering() {
        var event = valid(uniqueId());
        LinkingStub.failThenSucceed(event.getOrderId(), 503, 1);

        producer.send(event).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getReceived()).contains(event));
        assertThat(LinkingStub.requestsFor(event.getOrderId())).hasSize(2);
        assertThat(store.countForOrder(event.getOrderId())).isEqualTo(1);
    }

    @Test
    void rejectionIsNotRetriedAndGoesStraightToDeadLetter() {
        var event = valid(uniqueId());
        LinkingStub.failWith(event.getOrderId(), 400);
        try (var dlt = new TopicReader(brokers, "orders.DLT")) {
            producer.send(event).join();

            assertThat(dlt.await(event.getOrderId(), 1, TIMEOUT)).hasSize(1);
        }
        assertThat(consumer.attemptsFor(event.getOrderId())).isEqualTo(1);
        assertThat(LinkingStub.requestsFor(event.getOrderId())).hasSize(1);
    }

    @Test
    void slowLinkingServiceTimesOutAndIsRetried() {
        var event = valid(uniqueId());
        LinkingStub.delay(event.getOrderId(), 1500); // read timeout in tests is 500 ms
        try (var dlt = new TopicReader(brokers, "orders.DLT")) {
            producer.send(event).join();

            assertThat(dlt.await(event.getOrderId(), 1, Duration.ofSeconds(30))).hasSize(1);
        }
        assertThat(LinkingStub.requestsFor(event.getOrderId())).hasSize(3);
    }

    @Test
    void failingOrderDoesNotBlockLaterOrders() {
        var bad = valid(uniqueId());
        var good = valid(uniqueId());
        LinkingStub.failWith(bad.getOrderId(), 503);

        producer.send(bad).join();
        producer.send(good).join();

        await().atMost(TIMEOUT).untilAsserted(() -> assertThat(consumer.getReceived()).contains(good));
    }

    @Test
    void redeliveredRecordIsNotLinkedTwice() {
        var event = valid(uniqueId());
        long offset = 9_000_000L + (long) (Math.random() * 1_000_000);

        // the same Kafka record (topic, partition, offset) delivered twice, e.g. after a consumer restart
        consumer.onMessage(event, "orders", 0, offset);
        consumer.onMessage(event, "orders", 0, offset);

        assertThat(LinkingStub.requestsFor(event.getOrderId())).hasSize(1);
        assertThat(store.countForOrder(event.getOrderId())).isEqualTo(1);
    }
}
