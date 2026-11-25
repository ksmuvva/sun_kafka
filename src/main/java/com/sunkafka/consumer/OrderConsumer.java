package com.sunkafka.consumer;

import com.sunkafka.linking.LinkingClient;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.persistence.ProcessedMessageStore;
import com.sunkafka.validation.OrderValidator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    /** A successfully processed event plus the partition it came from. */
    public record Delivery(OrderEvent event, int partition) {
    }

    private final List<Delivery> deliveries = new CopyOnWriteArrayList<>();
    private final Map<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private final OrderValidator validator;
    private final LinkingClient linkingClient;
    private final ProcessedMessageStore store;

    public OrderConsumer(OrderValidator validator, LinkingClient linkingClient, ProcessedMessageStore store) {
        this.validator = validator;
        this.linkingClient = linkingClient;
        this.store = store;
    }

    /**
     * Validate, call the linking service, then remember the record as processed.
     * <ul>
     *   <li>invalid order or permanent linking rejection: not retried, goes to the dead-letter topic</li>
     *   <li>transient linking failure: retried, then dead-lettered</li>
     *   <li>record already processed (redelivery): skipped, no second linking call</li>
     * </ul>
     */
    @KafkaListener(topics = "${app.topic}")
    public void onMessage(@Payload OrderEvent event,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        attempts.computeIfAbsent(event.getOrderId(), k -> new AtomicInteger()).incrementAndGet();
        validator.validate(event);

        String messageKey = ProcessedMessageStore.keyOf(topic, partition, offset);
        if (store.isProcessed(messageKey)) {
            log.info("Skipping already processed record {}", messageKey);
            return;
        }
        // If we crash after this call but before record(), the redelivery calls again; the linking service
        // de-duplicates with the same Idempotency-Key.
        linkingClient.link(event, messageKey);
        store.record(messageKey, event.getOrderId());

        log.info("Consumed {} from partition {}", event, partition);
        deliveries.add(new Delivery(event, partition));
    }

    public List<Delivery> getDeliveries() {
        return deliveries;
    }

    public List<OrderEvent> getReceived() {
        return deliveries.stream().map(Delivery::event).toList();
    }

    /** How many times the listener was invoked for this order (initial try + retries). */
    public int attemptsFor(String orderId) {
        var count = attempts.get(orderId);
        return count == null ? 0 : count.get();
    }

    public void reset() {
        deliveries.clear();
        attempts.clear();
    }
}
