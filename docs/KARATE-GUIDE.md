# Karate guide: testing the Kafka flow

How to run, read and extend the Karate proof of concept. What it measured: [KARATE-POC-RESULTS.md](KARATE-POC-RESULTS.md).

## What it is

[Karate](https://github.com/karatelabs/karate) is an API-testing framework. Tests are `.feature` files in a
Gherkin-like language (`Given / When / Then`, plus `*` for plain steps). Here it checks the whole event flow of the
application:

```
HTTP POST /api/orders ──► Kafka "orders" ──► consumer ──► linking service (fake)
        Karate publishes to Kafka itself ──┘                  └─► "orders.DLT" on failure
```

Karate drives the HTTP API directly. It has **no built-in Kafka support that I found** (version 1.4.1), so a small Java helper,
`karate/KafkaSupport`, lets features publish to and read from Kafka.

Karate is Cucumber-*like* but does **not** use the Cucumber runtime. This repository has both: Cucumber (`bdd/`,
`src/test/resources/features/`) tests from inside the application; Karate (`karate/`, `src/test/resources/karate/`)
tests the running application from outside.

## Run it

```bash
mvn test -Dtest=KarateTest                                   # all Karate features (about 25 s, no Docker)
mvn test -Dtest=KarateTest -Dkarate.options="classpath:karate/kafka-consumer.feature"   # one feature
mvn test                                                     # everything, Karate included
```
Report (open in a browser): `target/karate-reports/karate-summary.html`; per-feature pages sit beside it.

`KarateTest` starts the real application on a random port with an embedded Kafka broker, an in-memory Schema Registry
and database, and a fake linking service, then hands the port to Karate. Nothing external is needed.

## Files

| File | Role |
|---|---|
| `src/test/resources/karate/*.feature` | The tests |
| `src/test/resources/karate-config.js` | Sets `baseUrl`. Must sit at the classpath root, or Karate will not find it |
| `src/test/java/.../karate/KarateTest.java` | JUnit runner: starts the app, then runs the features |
| `src/test/java/.../karate/KafkaSupport.java` | Kafka helper called from features |
| `src/test/java/.../support/LinkingStub.java` | The fake linking service (WireMock), shared with the other test layers |

## Writing a scenario

Every feature starts with the same background: get the helpers, reset the fake linking service, make unique ids.

```gherkin
Background:
  * def Kafka = Java.type('com.sunkafka.karate.KafkaSupport')
  * def Linking = Java.type('com.sunkafka.support.LinkingStub')
  * eval Linking.reset()
  * def newId = function(){ return 'karate-' + java.util.UUID.randomUUID() }
  * url baseUrl
```

**Call the HTTP API and check the answer**
```gherkin
Given path 'api/orders'
And request { orderId: '#(id)', customer: 'alice', amount: 42.5 }
When method post
Then status 202
And match response == { orderId: '#(id)' }
```

**Publish to Kafka yourself** (acts as the upstream producer)
```gherkin
* eval Kafka.publish(id, 'bob', 10.5, null)          # null currency -> schema default USD
* eval Kafka.publishRaw(poisonKey, 'this-is-not-avro')  # a message that is not valid Avro
```

**Check what is on a topic, what reached the linking service, what was dead-lettered**
```gherkin
* def events = Kafka.readEvents('orders', id, 15)
* match events[0] contains { orderId: '#(id)', customer: 'alice', amount: 42.5, currency: 'USD' }

* def calls = Kafka.awaitLinkingCalls(id, 1, 15)
* match calls == '#[1]'
* match calls[0].idempotencyKey == '#regex orders-\\d+-\\d+'

* def dead = Kafka.awaitDeadLetters(id, 1, 20)
* match dead == '#[1]'
```

**Script the linking service** (per order id, before publishing)
```gherkin
* eval Linking.failWith(id, 503)               # always answers 503
* eval Linking.failThenSucceed(id, 503, 1)     # fails once, then succeeds
* eval Linking.delay(id, 1500)                 # answers after 1.5 s (the read timeout in tests is 500 ms)
```

**Many cases in one scenario**: use `Scenario Outline` with an `Examples:` table (see `orders-e2e.feature`).

## Helper reference

| Call | Does | Returns |
|---|---|---|
| `Kafka.publish(orderId, customer, amount, currency)` | Publish an Avro order to `orders`; `currency` may be `null` | nothing |
| `Kafka.publishRaw(key, text)` | Publish non-Avro bytes to `orders` | nothing |
| `Kafka.readEvents(topic, key, seconds)` | Avro events with this key; waits up to `seconds` for the first one | list of `{orderId, customer, amount, currency, partition}` |
| `Kafka.awaitLinkingCalls(orderId, count, seconds)` | Waits for at least `count` linking calls, then 500 ms more so extras show up | list of `{body, idempotencyKey}` |
| `Kafka.awaitDeadLetters(text, count, seconds)` | Dead-letter records whose key or value contains `text` | list of `{key, exception}` |
| `Linking.reset()` / `failWith` / `failThenSucceed` / `delay` | Script the fake linking service | nothing |

Use `* eval` for calls whose result you ignore, `* def x = ...` when you need the result.

## Things to know

- **Scenarios must not run in parallel.** They share one fake linking service, so `KarateTest` runs on one thread.
- **"Never published" checks cost time.** Proving something is absent means waiting out a window (2 s in the
  invalid-order scenarios).
- **Exact counts are checked after a short pause.** `awaitLinkingCalls` waits 500 ms after reaching the target so a
  third retry or a duplicate call cannot slip in unnoticed. Without it a wrong retry count passed the tests.
- **`--name` did not narrow scenarios** in our setup (a whole feature ran). Select by feature file instead.
- **Only the embedded setup is wired.** To run Karate against a deployed environment you would point `baseUrl` at it and
  replace `KafkaSupport.init(...)` and the fake linking service; that has not been built.
- If a feature fails with `"baseUrl" is not defined`, `karate-config.js` is in the wrong place.

## Karate or Cucumber?

| | Karate | Cucumber (this repo) |
|---|---|---|
| Tests from | outside: real HTTP calls to the running app | inside: calls Java code and beans directly |
| HTTP testing | built in (`path`, `request`, `method`, `match`, JSON matching) | would need a library and step code |
| Kafka | helper class needed | helper classes needed (`TopicReader`, steps) |
| Steps | written in the feature file; no step classes | every phrase needs a Java step definition |
| Reads like | scripted checks with JSON matching | plain-English business wording |
| Needs from the app | a running server | nothing; works on beans |

Use Karate when the contract you care about is HTTP plus what happens after it; use Cucumber when you want
business-readable scenarios or to test below the HTTP layer. The repo keeps both for now (decision D8 in
[DECISIONS.md](DECISIONS.md)).
