package com.sunkafka.health;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

/** Reports DOWN when the Kafka cluster cannot be reached within the timeout. Shows as "kafka" in /actuator/health. */
@Component("kafka")
public class KafkaHealthIndicator extends AbstractHealthIndicator {

    private final String bootstrapServers;
    private final Duration timeout;

    public KafkaHealthIndicator(@Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
                                @Value("${management.health.kafka.timeout:2s}") Duration timeout) {
        super("Kafka health check failed");
        this.bootstrapServers = bootstrapServers;
        this.timeout = timeout;
    }

    @Override
    protected void doHealthCheck(Health.Builder builder) throws Exception {
        try (Admin admin = Admin.create(Map.<String, Object>of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) timeout.toMillis(),
                AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, (int) timeout.toMillis()))) {
            var cluster = admin.describeCluster();
            int nodes = cluster.nodes().get(timeout.toMillis(), TimeUnit.MILLISECONDS).size();
            builder.up().withDetail("clusterId", cluster.clusterId().get(timeout.toMillis(), TimeUnit.MILLISECONDS))
                    .withDetail("nodes", nodes);
        }
    }
}
