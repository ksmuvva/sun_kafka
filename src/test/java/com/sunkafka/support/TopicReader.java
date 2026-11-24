package com.sunkafka.support;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Independent raw (String/String) consumer for asserting on any topic, e.g. the DLT. */
public final class TopicReader implements AutoCloseable {

    private final Consumer<String, String> consumer;
    private final List<ConsumerRecord<String, String>> seen = new ArrayList<>();

    public TopicReader(String bootstrapServers, String topic) {
        consumer = new KafkaConsumer<>(Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "test-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(List.of(topic));
    }

    /** Polls until {@code count} records matching {@code keyOrValueContains} were seen, or the timeout expires. */
    public List<ConsumerRecord<String, String>> await(String keyOrValueContains, int count, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            consumer.poll(Duration.ofMillis(200)).forEach(seen::add);
            var matches = seen.stream()
                    .filter(r -> (r.key() != null && r.key().contains(keyOrValueContains))
                            || (r.value() != null && r.value().contains(keyOrValueContains)))
                    .toList();
            if (matches.size() >= count) {
                return matches;
            }
        }
        return seen.stream()
                .filter(r -> (r.key() != null && r.key().contains(keyOrValueContains))
                        || (r.value() != null && r.value().contains(keyOrValueContains)))
                .toList();
    }

    @Override
    public void close() {
        consumer.close();
    }
}
