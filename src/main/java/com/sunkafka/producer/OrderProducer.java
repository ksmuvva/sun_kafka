package com.sunkafka.producer;

import com.sunkafka.model.OrderEvent;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
public class OrderProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderProducer.class);

    private final KafkaTemplate<String, OrderEvent> template;
    private final String topic;

    public OrderProducer(KafkaTemplate<String, OrderEvent> template, @Value("${app.topic}") String topic) {
        this.template = template;
        this.topic = topic;
    }

    /** Keyed by orderId so events for one order stay in one partition (ordering). */
    public CompletableFuture<SendResult<String, OrderEvent>> send(OrderEvent event) {
        return template.send(topic, event.orderId(), event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish {}", event, ex);
                    } else {
                        log.info("Published {} to {}-{}@{}", event.orderId(),
                                result.getRecordMetadata().topic(),
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    }
                });
    }
}
