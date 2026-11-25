package com.sunkafka.config;

import com.sunkafka.consumer.InvalidOrderException;
import com.sunkafka.linking.LinkingPermanentException;
import com.sunkafka.model.OrderEvent;
import java.util.Map;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.BeanUtils;
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
import org.springframework.util.backoff.FixedBackOff;

/**
 * Failed records are retried (2 retries, 200ms apart) and then published to {@code <topic>.DLT}
 * (invalid orders and permanent linking rejections are not retried)
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
        // The DLT may carry raw bytes (poison pills, i.e. bytes that are not valid Avro) or OrderEvents, so it needs a type-aware serializer.
        Map<String, Object> producerProps = props.buildProducerProperties(null);
        // The Avro serializer is whatever spring.kafka.producer.value-serializer says (Confluent today, so a
        // different registry only needs a config change). Producers built from instances are not auto-configured,
        // so give it the registry settings ourselves.
        @SuppressWarnings("unchecked")
        Serializer<Object> avro = (Serializer<Object>) BeanUtils.instantiateClass(props.getProducer().getValueSerializer());
        avro.configure(producerProps, false);
        Map<Class<?>, Serializer<?>> delegates = Map.of(
                byte[].class, new ByteArraySerializer(),
                OrderEvent.class, avro);
        var factory = new DefaultKafkaProducerFactory<String, Object>(
                producerProps,
                new StringSerializer(),
                new DelegatingByTypeSerializer(delegates, true));
        var recoverer = new DeadLetterPublishingRecoverer(new KafkaTemplate<>(factory),
                (record, ex) -> new TopicPartition(record.topic() + DLT_SUFFIX, record.partition()));
        var handler = new DefaultErrorHandler(recoverer, new FixedBackOff(200L, 2L));
        // Retrying cannot fix these: send them straight to the dead-letter topic.
        handler.addNotRetryableExceptions(InvalidOrderException.class, LinkingPermanentException.class);
        return handler;
    }
}
