# sun_kafka

End-to-end test framework for Kafka messaging: a **publisher** and a **consumer** with retry / dead-letter handling,
built with Spring Boot + Spring Kafka (Java 17, Maven). Messages are **Avro**, validated by **Confluent Schema Registry**.

## Layout
| Path | Purpose |
|---|---|
| `src/main/avro/OrderEvent.avsc` | The event schema; `OrderEvent` Java class is generated from it at build time |
| `producer/OrderProducer` | Publishes Avro `OrderEvent` keyed by `orderId` (`acks=all`, idempotent) |
| `consumer/OrderConsumer` | `@KafkaListener` on `orders`; rejects non-positive amounts |
| `config/KafkaTopicConfig` | `orders` topic (3 partitions) |
| `config/KafkaErrorHandlingConfig` | 2 retries (200 ms), then publish to `orders.DLT`; poison pills (bytes that are not valid Avro) skip retries |
| `test/.../support/` | Reusable helpers: `OrderEvents` (builders), `TopicReader` (raw consumer for any topic) |
| `test/.../KafkaE2ETestBase` | Spring context + embedded broker shared by E2E tests |

## Test suites
| Test | Proves |
|---|---|
| `OrderMessagingTest` | event delivered; same key stays ordered on one partition; different keys spread across partitions |
| `DeadLetterTest` | invalid event retried 3x total then dead-lettered; malformed JSON dead-lettered and consumer not blocked |
| `SchemaRegistryTest` | schema registered under `orders-value`; `currency` defaults to `USD`; compatible change accepted, breaking change rejected |
| `bdd/CucumberTest` | 9 Cucumber (BDD) scenarios over the same behaviour, written in plain Gherkin (see below) |
| `KafkaContainerTest` | Avro round trip on a real Kafka + real Schema Registry (Testcontainers); auto-skipped without Docker |

```bash
mvn test        # embedded broker + in-memory (mock://) Schema Registry, no Docker needed
mvn verify      # same, plus the real-broker test if Docker is running
```
During the retry tests the log shows `Seek to current after exception` at ERROR: that is Spring's normal
redelivery mechanism, not a failure.

## Run the app against a local broker
```bash
docker compose up -d          # Kafka on :9092, Schema Registry on :8081
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 SCHEMA_REGISTRY_URL=http://localhost:8081 mvn spring-boot:run
```

## BDD (Cucumber)
Scenarios live in `src/test/resources/features/*.feature` and read as plain English:

| Feature | Scenarios |
|---|---|
| `order_messaging.feature` | delivery; same order stays ordered on one partition; different orders spread across partitions |
| `failure_handling.feature` | invalid order retried then dead-lettered; malformed message dead-lettered without blocking later orders |
| `schema_evolution.feature` | subject registered; currency defaults to USD; compatible change accepted / breaking change rejected |

Step definitions are in `src/test/java/com/sunkafka/bdd/` and reuse the producer, consumer and `support/` helpers.
`mvn test` runs them; the HTML report is written to `target/cucumber-reports/report.html` (CI uploads it as the
`cucumber-report` artifact). To add a scenario, write it in a `.feature` file and add any missing step definition.

## Evolving the schema
Edit `OrderEvent.avsc`. A new field **must have a default** (like `currency`) so old data stays readable; the registry
rejects breaking changes (see `SchemaRegistryTest.schemaEvolutionRulesAreEnforced`).

## Build note
The Confluent Avro serializer is published only on `https://packages.confluent.io/maven/` (declared in `pom.xml`).
If your network blocks that host the build cannot resolve dependencies.

CI (`.github/workflows/ci.yml`) runs `mvn verify` on every push to `main` and every PR.
