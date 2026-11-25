# Design decisions

Short records of choices made while building the framework. Change one by editing the entry and saying why.

## D1. Maven is the build tool
**Why:** declarative and already working in CI; the project needs no custom build logic beyond the Avro generator,
JaCoCo and the Confluent repository. **Trade-off:** less customisable than Gradle. **Revisit if:** the client requires
Gradle or the build needs scripting (Q9).

## D2. Java 21 with Spring Boot 3.3.5
**Why:** the team starts on Java 21. **Not decided:** Java 25 / Spring 7 / Jackson 3 (Q8); an optional CI job on Java 25
can show what breaks when that decision is made.

## D3. Tests use an embedded broker, an in-memory registry and H2; Docker tests cover the real thing
**Why:** most tests then run in seconds with no Docker, on a laptop or CI. `KafkaContainerTest` and
`PostgresMigrationTest` run the same behaviour on real Kafka, Schema Registry and Postgres, and CI fails if they are
skipped. **Trade-off:** H2 in Postgres mode is only a stand-in; that is why the migrations also run on real Postgres.

## D4. Failure policy: retry only what can recover
Validation failures, linking 4xx (except 408/429) and non-Avro bytes go straight to `orders.DLT`. Linking 5xx, 408, 429,
timeouts and connection errors are retried twice (200 ms apart) and then dead-lettered. **Why:** retrying cannot fix a
bad message, and retrying a bad message delays the good ones behind it. **Assumption:** what to do with dead-lettered
messages (alert, replay) is not decided (Q6).

## D5. Idempotency: skip already-processed records, and send an `Idempotency-Key` downstream
The consumer records `topic-partition-offset` in Postgres after a successful link and skips a record it has seen. If it
crashes between the link and the record, the redelivery calls the linking service again with the *same*
`Idempotency-Key`, so the linking service can ignore the repeat (assumes it supports that; Q1).
**Known weakness:** offsets restart if a topic is recreated or consumers move to another cluster, which would wrongly
skip new records. A business key would be safer (Q10).

## D6. One test database per Spring context
Each Spring test context starts its own embedded Kafka broker, whose offsets start at 0. A shared database made the
second context skip a brand-new record as "already processed". The test profile therefore uses a random in-memory
database name per context.

## D7. The schema registry is a configuration choice
Serializer classes and the registry URL are in `application.yml`; the dead-letter publisher reuses the configured value
serializer. Moving to AWS Glue Schema Registry (a different message format) should need a dependency and configuration
change, not code changes. Not tried (Q4).

## D8. Karate is a proof of concept next to Cucumber, not a replacement
Karate drives the running app over HTTP and talks to Kafka through one small Java helper (`KafkaSupport`), because it has
no built-in Kafka support. Cucumber (Gherkin + Java steps) tests the same behaviour from inside the application. Both stay
for now so the team can compare them. **Revisit:** keep the one that the team finds easier to write and review, and delete
the other.

## D9. Developer tooling is committed as configuration only, and is opt-in in practice
Ponytail (plugin) and graphify (code knowledge graph) are declared in `.claude/settings.json` and `CLAUDE.md`. The graph
itself is not committed (about 1 MB, rewritten whole on every build). The graphify hooks call `graphify` from `PATH` and
silently do nothing when it is missing or no graph exists, so contributors who do not use them are unaffected. Absolute
paths are avoided on purpose: graphify's own installer writes the path of whichever virtualenv ran it.
