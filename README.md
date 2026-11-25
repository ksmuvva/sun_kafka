# Karate_kafka

A proof of concept that uses the **Karate** test framework (Cucumber-style `Given / When / Then` files, running on Java
and JUnit 5) to check a **Kafka producer, a Kafka consumer, and the whole event flow end to end**.

The service under test takes an order, puts it on Kafka as an Avro message, reads it back, and calls a downstream
"linking service". Karate tests it from the outside, the way a real client would. **You do not need to install Kafka,
Docker or a database**: the test starts everything it needs, inside the test run, on your machine.

```
 Karate test ──HTTP──► app: POST /api/orders ──► Kafka "orders" ──► consumer ──► linking service (fake)
      │                                              ▲                │
      └── publishes/reads Kafka directly ────────────┘                └──► "orders.DLT" (dead-letter topic) on failure
```

**What you will do in this README:** run the tests (steps 1 to 3), understand them (4), run one feature (5), make a test
fail on purpose (6), write your own (7), and run everything (8). About 15 minutes.

---

## Before you start

| You need | Version | Check with |
|---|---|---|
| Java (JDK) | **21** | `java -version` |
| Maven | 3.9 or newer | `mvn -version` |
| Git | any | `git --version` |
| Internet, first run only | to download dependencies from Maven Central and `packages.confluent.io` | |

Not needed: Docker, Kafka, a database, any open port (everything uses random ports). Tested on Linux with Java 21.0.13 and
Maven 3.9.11; macOS and Windows were not tested.

---

## Step 1: Get the code

```bash
git clone https://github.com/ksmuvva/sun_kafka
cd sun_kafka
```

## Step 2: Run the Karate tests

```bash
mvn test -Dtest=KarateTest
```

The first run downloads dependencies and can take several minutes. After that it takes about 40 seconds.

You will see **a lot of log lines** (a Kafka broker starting, the application starting). That is normal, and so are lines
containing `ERROR ... Seek to current after exception`: that is the consumer retrying a message on purpose.

**It worked if the end of the output contains these lines:**

```
scenarios:  4 | passed:  4 | failed:  0 | time: ...
scenarios:  5 | passed:  5 | failed:  0 | time: ...
...
features:     2 | skipped:    0 | efficiency: ...
scenarios:    9 | passed:     9 | failed: 0
...
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, ...
[INFO] BUILD SUCCESS
```

## Step 3: Open the report

Open this file in a web browser:

```
target/karate-reports/karate-summary.html
```

Click a feature to see every scenario, every step, and each HTTP request and response. `karate-timeline.html` (same
folder) shows when each scenario ran.

## Step 4: Understand what ran

Two feature files in `src/test/resources/karate/`, 9 scenarios:

| Feature | Scenario | What it proves |
|---|---|---|
| `orders-e2e` | An order posted over HTTP is published to Kafka and linked exactly once | **Producer:** the HTTP request becomes an Avro event on the `orders` topic (default currency `USD` applied). **End to end:** the consumer then calls the linking service once, with an idempotency key |
| `orders-e2e` | The service reports healthy | `/actuator/health` shows `UP`, including Kafka and the database |
| `orders-e2e` | Invalid order: negative amount / zero amount / blank customer (3 rows of one table) | The service answers **400** with the reason and **nothing reaches Kafka** |
| `kafka-consumer` | An order published straight to Kafka is linked exactly once | **Consumer:** Karate publishes to Kafka itself, and the consumer calls the linking service once |
| `kafka-consumer` | A temporary linking failure is retried, then dead-lettered | The linking service answers 503: the consumer tries 3 times, then the order lands on `orders.DLT` |
| `kafka-consumer` | A rejected order is dead-lettered without retries | The linking service answers 400: 1 attempt, then `orders.DLT` |
| `kafka-consumer` | A malformed message is dead-lettered and does not block the next order | Garbage bytes go to `orders.DLT`; the order after it is still processed |

How it runs: `KarateTest.java` starts the real application on a random port, with an embedded Kafka broker, an in-memory
Schema Registry and database, and a fake linking service (WireMock). It then runs every feature. The features talk
to the app over HTTP, and to Kafka through a small Java helper (`KafkaSupport.java`), because Karate has no built-in
Kafka support that I found.

## Step 5: Run just one feature

```bash
mvn test -Dtest=KarateTest -Dkarate.options="classpath:karate/kafka-consumer.feature"
```

Expect `features: 1` and `scenarios: 4 | passed: 4 | failed: 0`. Use `classpath:karate/orders-e2e.feature` for the
other one (5 scenarios). Filtering by scenario name with `--name` did **not** work in our setup; pick a feature file.

## Step 6: Make a test fail on purpose

A test you have never seen fail is a test you cannot trust. Break one:

1. Open `src/test/resources/karate/kafka-consumer.feature`.
2. In the first scenario, change `amount: 10.5` to `amount: 99` (it is on the `match calls[0].body contains` line).
3. Run it again:
   ```bash
   mvn test -Dtest=KarateTest -Dkarate.options="classpath:karate/kafka-consumer.feature"
   ```
4. Expect `scenarios: 4 | passed: 3 | failed: 1` and `BUILD FAILURE`, with a message like:
   ```
   match failed: CONTAINS
     $ | actual does not contain expected | ... expected has un-matched keys - [amount]
     {"orderId":"karate-...","customer":"bob","amount":10.5,"currency":"USD"}
   ```
   Karate prints the **actual** event (`"amount":10.5`) next to what you expected.
5. Undo your change:
   ```bash
   git checkout -- src/test/resources/karate/kafka-consumer.feature
   ```

## Step 7: Write your own scenario

1. Create `src/test/resources/karate/my-first.feature` with this content:
   ```gherkin
   Feature: My first scenario

     Background:
       * def Kafka = Java.type('com.sunkafka.karate.KafkaSupport')
       * def Linking = Java.type('com.sunkafka.support.LinkingStub')
       * eval Linking.reset()
       * url baseUrl

     Scenario: A large order is accepted, published to Kafka and linked once
       * def id = 'my-first-' + java.util.UUID.randomUUID()
       Given path 'api/orders'
       And request { orderId: '#(id)', customer: 'me', amount: 1234.5 }
       When method post
       Then status 202
       * def calls = Kafka.awaitLinkingCalls(id, 1, 15)
       * match calls == '#[1]'
       * match calls[0].body.amount == 1234.5
   ```
2. Run only your file:
   ```bash
   mvn test -Dtest=KarateTest -Dkarate.options="classpath:karate/my-first.feature"
   ```
3. Expect `features: 1` and `scenarios: 1 | passed: 1 | failed: 0`.

What the lines do: the `Background` loads the two helpers, resets the fake linking service and sets the app's address
(`baseUrl`). The `Given ... Then status 202` lines are a normal HTTP call. `Kafka.awaitLinkingCalls(id, 1, 15)` waits up
to 15 seconds for 1 call to reach the linking service and returns it, and the two `match` lines check it.

Things to try next (change, run, observe):
- Change `status 202` to `status 200` and watch it fail.
- Add `* eval Linking.failWith(id, 503)` before `Given path` and change the expected calls to 3: the order is now retried.
- Post `amount: -1` and expect `status 400`.

## Step 8: Run everything

```bash
mvn test
```

Expect `Tests run: 53, Failures: 0, Errors: 0, Skipped: 3` and `BUILD SUCCESS` (about a minute). The 3 skipped tests need
Docker (real Kafka and Postgres containers) and skip themselves without it.

This also runs the repository's **Cucumber** suite, which tests the same behaviour from the inside, with a Java step
definition for every phrase. To run only that: `mvn test -Dtest=CucumberTest` (14 scenarios, report at
`target/cucumber-reports/report.html`). Karate is Cucumber-style but has its own engine; the two are kept side by side so
you can compare them. See the guide below.

---

## Cheat sheet

Helpers available inside any feature (put the `Background` from step 7 at the top):

| Call | What it does |
|---|---|
| `Kafka.publish(orderId, customer, amount, currency)` | Publish an Avro order to Kafka (`currency` may be `null`) |
| `Kafka.publishRaw(key, text)` | Publish a message that is not valid Avro |
| `Kafka.readEvents('orders', key, seconds)` | Read the events on a topic for one key |
| `Kafka.awaitLinkingCalls(orderId, count, seconds)` | Wait for calls to the linking service, then list them |
| `Kafka.awaitDeadLetters(text, count, seconds)` | Wait for records on the dead-letter topic |
| `Linking.failWith(id, 503)` | Make the linking service answer 503 for that order |
| `Linking.failThenSucceed(id, 503, 1)` | Fail once, then succeed |
| `Linking.delay(id, 1500)` | Answer after 1.5 seconds (test timeout is 0.5 s) |
| `Linking.reset()` | Everything succeeds again |

## Troubleshooting

| Problem | Fix |
|---|---|
| `"baseUrl" is not defined` | `karate-config.js` must sit in `src/test/resources/` (not in the `karate/` folder) |
| `release version 21 not supported` or similar | Your JDK is older than 21. Install JDK 21 and check `java -version` |
| Cannot download dependencies (mentions `packages.confluent.io`) | Your network blocks that site. The Avro serializer is only published there |
| Lots of `ERROR` lines but `BUILD SUCCESS` | Normal: deliberate retries. Trust the last lines (step 2) |
| `--name` runs more scenarios than expected | Known: select by feature file instead (step 5) |
| A scenario fails only sometimes | Run it again and open the report. Scenarios must not run in parallel (they share one fake linking service) |

## Limits of this proof of concept

- Runs on an **embedded** Kafka broker and in-memory registry and database. It has not been run through Karate against
  real Kafka or a deployed environment (the repo's Docker-based JUnit tests cover real Kafka in CI).
- Proving that something was *not* published means waiting out a short window (2 seconds), so those scenarios are slower.
- Karate's Kafka access is hand-written (`KafkaSupport.java`), which is the main cost of choosing Karate here.



| Document | What it covers |
|---|---|
| [docs/KARATE-GUIDE.md](docs/KARATE-GUIDE.md) | Writing scenarios, every helper in detail, Karate versus Cucumber |
| [docs/KARATE-POC-RESULTS.md](docs/KARATE-POC-RESULTS.md) | Measured results: timings, repeated runs, deliberate-breakage checks, findings |
| [docs/PROJECT-OVERVIEW.md](docs/PROJECT-OVERVIEW.md) | The application being tested and every other test layer (JUnit, Cucumber, Docker) |
| [docs/REQUIREMENTS.md](docs/REQUIREMENTS.md), [docs/DECISIONS.md](docs/DECISIONS.md) | Requirement status and design decisions |
