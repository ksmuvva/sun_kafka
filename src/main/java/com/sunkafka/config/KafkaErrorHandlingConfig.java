package com.sunkafka.config;

import com.sunkafka.model.OrderEvent;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Failed records are retried (2 retries, 200ms apart) and then published to {@code <topic>.DLT}
 * on the same partition. Undeserializable "poison pills" skip retries and go straight to the DLT.
 * Spring Boot wires the {@link DefaultErrorHandler} bean into the listener container factory.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    public static final String DLT_SUFFIX = ".DLT";

    @Bean
    NewTopic deadLetterTopic(@Value("${app.topic}") String topic) {
        return TopicBuilder.name(topic + DLT_SUFFIX).partitions(3).replicas(1).build();
    }

    @Bean
    DefaultErrorHandler kafkaErrorHandler(KafkaProperties props) {
        // The DLT may carry raw bytes (poison pills) or OrderEvents, so it needs a type-aware serializer.
        Map<Class<?>, Serializer<?>> delegates = Map.of(
                byte[].class, new ByteArraySerializer(),
                OrderEvent.class, new JsonSerializer<OrderEvent>());
        var factory = new DefaultKafkaProducerFactory<String, Object>(
                props.buildProducerProperties(null),
                new StringSerializer(),
                new DelegatingByTypeSerializer(delegates, true));
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(factory),
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2L));
    }
}
