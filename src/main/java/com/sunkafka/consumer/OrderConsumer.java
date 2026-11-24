package com.sunkafka.consumer;

import com.sunkafka.model.OrderEvent;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class OrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(OrderConsumer.class);

    private final List<OrderEvent> received = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "${app.topic}")
    public void onMessage(OrderEvent event) {
        log.info("Consumed {}", event);
        received.add(event);
    }

    /** Exposed so tests can assert on what was consumed. */
    public List<OrderEvent> getReceived() {
        return received;
    }
}
