# spring-boot-messaging-outbox

Three ways a message and the state it describes end up disagreeing, and the fix for each.

Spring Boot 4.1 · Spring for Apache Kafka · Kotlin 2.3 · Java 21

```bash
./mvnw test      # seven tests run anywhere; three need a broker and skip without Docker
```

None of these raise anything. The system is simply inconsistent from that point on, and the
evidence is a message with no matching row, a row nobody was told about, or one customer's
events stopping while every dashboard stays green.

---

## Publishing from the service body is wrong in both directions

```kotlin
@Transactional
fun place(order: Order) {
    repository.save(order)
    kafka.send("orders", order.id, order.json())   // pick your failure
}
```

Send **before** the commit and a rollback leaves a message describing a change that never
happened. Send **after** it and a crash in between leaves a change nobody was told about.
There is no ordering of those two lines that is safe, because they are not in the same
system.

The outbox puts them in the same system: the message is written to the database, in the
transaction that produced it, and published from there afterwards.

```kotlin
@Transactional
fun place(id: String) {
    jdbc.update("INSERT INTO orders (id) VALUES (?)", id)
    outbox.record(OutboxMessage(UUID.randomUUID(), id, "orders", """{"id":"$id"}"""))
}
```

The tests hold both halves: a rollback leaves neither the order nor the message, and a
commit leaves both.

**Publish, then mark.** The other order loses messages: mark first and a failure to publish
leaves a row that will never be retried — the one outcome the outbox exists to prevent.
Between publishing and marking the process can die, which is where at-least-once comes from,
and it is why the next section exists.

## At-least-once is the contract, not a caveat

A consumer that is not idempotent passes every test and processes a payment twice the first
time a broker redelivers after a slow acknowledgement.

The claim is a primary-key insert, not a read-then-write:

```kotlin
fun claim(messageId: UUID, consumer: String): Boolean = try {
    jdbc.update("INSERT INTO inbox (message_id, consumer, processed_at) VALUES (?, ?, ?)", …)
    true
} catch (_: DuplicateKeyException) {
    false
}
```

Two instances will pass the same `SELECT` at the same moment and both conclude the message
is new. The database is the only participant here that can decide that race.

**The key is `(message_id, consumer)`, not `message_id`.** Idempotency is per consumer:
billing having handled a message says nothing about whether shipping has. A single-column
key silently drops the second consumer's work, and the test that caught it is in the repo.

And the claim comes **before** the work, not after — claiming after leaves a window where a
crash repeats an effect that already happened.

## A poison message blocks a partition, not a topic

This is the one that reads as a mystery. A record that always fails is retried forever, and
everything queued behind it on that partition stops. The topic is fine. The consumer is
fine. One customer's events simply stop arriving, and the graphs stay green.

Retries are therefore bounded and the failure is parked:

```kotlin
DefaultErrorHandler(recoverer, ExponentialBackOff(200, 2.0).apply {
    maxInterval = 5_000
    maxAttempts = 3
}).apply {
    addNotRetryableExceptions(IllegalArgumentException::class.java)
}
```

Two details that are easy to leave out:

**Not everything is worth retrying.** A deserialization or validation failure cannot succeed
on the third attempt; retrying it just delays the dead-letter topic by the whole backoff for
no chance of recovery.

**Keep the partition.** The recoverer routes to `<topic>.DLT` on the *same* partition the
record came from, so a parked message can be traced back to the stream it belonged to.

The integration test proves the property that matters: a healthy record queued behind a
poison one still arrives.

---

## What runs where

H2 and a bounded-backoff assertion cover the outbox, the inbox and the retry budget on any
machine. Kafka runs under Testcontainers and skips without Docker, because the three
behaviours that need a real broker — delivery, redelivery, and parking — cannot be honestly
demonstrated with a mock.

## Licence

MIT.
