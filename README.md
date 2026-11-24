# sun_kafka

End-to-end test framework for Kafka messaging: a **publisher** and a **consumer** with retry / dead-letter handling,
built with Spring Boot + Spring Kafka (Java 17, Maven).

## Layout
| Path | Purpose |
|---|---|
| `producer/OrderProducer` | Publishes `OrderEvent` JSON keyed by `orderId` (`acks=all`, idempotent) |
| `consumer/OrderConsumer` | `@KafkaListener` on `orders`; rejects non-positive amounts |
| `config/KafkaTopicConfig` | `orders` topic (3 partitions) |
| `config/KafkaErrorHandlingConfig` | 2 retries (200 ms), then publish to `orders.DLT`; poison pills skip retries |
| `test/.../support/` | Reusable helpers: `OrderEvents` (builders), `TopicReader` (raw consumer for any topic) |
| `test/.../KafkaE2ETestBase` | Spring context + embedded broker shared by E2E tests |

## Test suites
| Test | Proves |
|---|---|
| `OrderMessagingTest` | event delivered; same key stays ordered on one partition; different keys spread across partitions |
| `DeadLetterTest` | invalid event retried 3x total then dead-lettered; malformed JSON dead-lettered and consumer not blocked |
| `KafkaContainerTest` | round trip on a real broker (Testcontainers); auto-skipped without Docker |

```bash
mvn test        # embedded broker, no Docker needed
mvn verify      # same, plus the real-broker test if Docker is running
```
During the retry tests the log shows `Seek to current after exception` at ERROR: that is Spring's normal
redelivery mechanism, not a failure.

## Run the app against a local broker
```bash
docker compose up -d
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 mvn spring-boot:run
```

CI (`.github/workflows/ci.yml`) runs `mvn verify` on every push to `main` and every PR.
