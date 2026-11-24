package com.sunkafka.consumer;

import com.sunkafka.model.OrderEvent;
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

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(@Payload OrderEvent event,
                          @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
        attempts.computeIfAbsent(event.getOrderId(), k -> new AtomicInteger()).incrementAndGet();
        if (event.getAmount() <= 0) {
            throw new InvalidOrderException("Order " + event.getOrderId() + " has non-positive amount");
        }
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
