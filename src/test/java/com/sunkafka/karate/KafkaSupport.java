package com.sunkafka.karate;

import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.sunkafka.model.OrderEvent;
import com.sunkafka.support.LinkingStub;
import com.sunkafka.support.TopicReader;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

/**
 * The bridge between Karate feature files and Kafka. Karate has no built-in Kafka support, so features call these
 * static methods through Java interop: {@code * def Kafka = Java.type('com.sunkafka.karate.KafkaSupport')}.
 * Everything returns plain maps and lists, which Karate can match against directly.
 */
public final class KafkaSupport {

    private static final String SCHEMA_REGISTRY = "mock://sun-kafka";
    private static final ObjectMapper JSON = new ObjectMapper();

    private static volatile String brokers;

    private KafkaSupport() {
    }

    /** Called once by the JUnit runner with the address of the embedded broker. */
    public static void init(String bootstrapServers) {
        brokers = bootstrapServers;
    }

    /** Publish an Avro order straight onto the topic, as an upstream producer would. {@code currency} may be null. */
    public static void publish(String orderId, String customer, double amount, String currency) throws Exception {
        var builder = OrderEvent.newBuilder().setOrderId(orderId).setCustomer(customer).setAmount(amount);
        if (currency != null) {
            builder.setCurrency(currency);
        }
        try (var producer = new KafkaProducer<String, OrderEvent>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class,
                "schema.registry.url", SCHEMA_REGISTRY))) {
            producer.send(new ProducerRecord<>("orders", orderId, builder.build())).get();
        }
    }

    /** Publish bytes that are not valid Avro (a "poison pill"). */
    public static void publishRaw(String key, String text) throws Exception {
        try (var producer = new KafkaProducer<String, String>(Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers),
                new StringSerializer(), new StringSerializer())) {
            producer.send(new ProducerRecord<>("orders", key, text)).get();
        }
    }

    /**
     * Avro events on a topic whose key is {@code key}. Waits up to {@code seconds} for at least one, then returns
     * what it found (possibly nothing), so a "nothing was published" check simply uses a short wait.
     */
    public static List<Map<String, Object>> readEvents(String topic, String key, int seconds) {
        var deserializer = new KafkaAvroDeserializer();
        deserializer.configure(Map.of("schema.registry.url", SCHEMA_REGISTRY, "specific.avro.reader", true), false);
        var found = new ArrayList<Map<String, Object>>();
        try (var consumer = new KafkaConsumer<String, byte[]>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                ConsumerConfig.GROUP_ID_CONFIG, "karate-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + Duration.ofSeconds(seconds).toNanos();
            while (System.nanoTime() < deadline && found.isEmpty()) {
                for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                    if (!key.equals(record.key())) {
                        continue;
                    }
                    try {
                        var event = (OrderEvent) deserializer.deserialize(topic, record.value());
                        var map = new HashMap<String, Object>();
                        map.put("orderId", event.getOrderId());
                        map.put("customer", event.getCustomer());
                        map.put("amount", event.getAmount());
                        map.put("currency", event.getCurrency());
                        map.put("partition", record.partition());
                        found.add(map);
                    } catch (RuntimeException notAvro) {
                        // a poison pill with the same key is not an order event: ignore it here
                    }
                }
            }
        }
        return found;
    }

    /** Records on the dead-letter topic that mention {@code text} (in the key or value). */
    public static List<Map<String, Object>> awaitDeadLetters(String text, int count, int seconds) {
        try (var reader = new TopicReader(brokers, "orders.DLT")) {
            var records = reader.await(text, count, Duration.ofSeconds(seconds));
            var result = new ArrayList<Map<String, Object>>();
            for (var record : records) {
                var header = record.headers().lastHeader("kafka_dlt-exception-fqcn");
                var map = new HashMap<String, Object>();
                map.put("key", record.key());
                map.put("exception", header == null ? null : new String(header.value(), StandardCharsets.UTF_8));
                result.add(map);
            }
            return result;
        }
    }

    /**
     * Calls the (fake) linking service received for this order. Waits up to {@code seconds} until there are at least
     * {@code count}; returns what it has either way.
     */
    public static List<Map<String, Object>> awaitLinkingCalls(String orderId, int count, int seconds) {
        try {
            await().atMost(Duration.ofSeconds(seconds)).until(() -> LinkingStub.requestsFor(orderId).size() >= count);
        } catch (org.awaitility.core.ConditionTimeoutException ignored) {
            // report whatever arrived; the feature's own match decides pass or fail
        }
        var calls = new ArrayList<Map<String, Object>>();
        for (LoggedRequest request : LinkingStub.requestsFor(orderId)) {
            try {
                var call = new HashMap<String, Object>();
                call.put("body", JSON.readValue(request.getBodyAsString(), new TypeReference<Map<String, Object>>() { }));
                call.put("idempotencyKey", request.getHeader("Idempotency-Key"));
                calls.add(call);
            } catch (Exception e) {
                throw new IllegalStateException("Unreadable linking request body", e);
            }
        }
        return calls;
    }
}
