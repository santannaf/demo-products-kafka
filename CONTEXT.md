# Domain model

The vocabulary this codebase uses. A term here is a name you can grep for and find one module behind it.
When a name in code and a name here disagree, one of the two is a bug.

## Domain

**Product** — a named thing the system publishes an event about. Has an `Id` and a `Name`. The sample has
no persistence: a `Product` lives just long enough to build the event, so `Product.Create` is the only
gate a name ever passes. Nothing upstream re-checks its rules — an endpoint that copied them would make
the domain's copy unreachable and let the two drift.

**Product name** — required, trimmed, at most `Product.MaxNameLength` (200) characters, measured after
trimming. A name that breaks a rule raises `InvalidProductNameException`, which carries the offending
**field** so a caller can answer with a field-scoped error without re-deriving which rule broke.

**ProductCreated event** — what a successful creation publishes: `EventId`, `ProductId`, `Name`,
`OccurredAtUtc`. `OccurredAtUtc` always carries `DateTimeKind.Utc`; any other kind silently shifts the
instant on the wire, so it is pinned rather than trusted.

**ProductCreated topic** — the Kafka topic the event travels on, named by
`Kafka:Topics:ProductCreated`. Its wire contract is the Avro schema in
`Infrastructure/Messaging/Kafka/Avro/Schemas/product-created.avsc`, and nothing outside Infrastructure
names an Avro type.

## Seams

**Outbound port** — `ISendProductCreatedEventProvider`. Application publishes through it and never sees
Kafka, Avro or Schema Registry. Its Kafka adapter is `KafkaProductCreatedProducer`, which owns the
producer for the life of the process and translates broker and registry failures into
`EventPublishFailedException`.

**Subscription seam** — `IProductCreatedSubscription`. The four broker operations the delivery protocol
needs: `TryRead`, `Commit`, `SeekBack`, `PauseBeforeRetry`. Subscribing and leaving the consumer group
are deliberately *not* on it — an adapter subscribes when constructed and leaves when disposed, so the
protocol never learns that a topic name or a group id exists. Two adapters:
`KafkaProductCreatedSubscription` in production, a fake in the tests.

## Delivery

**At-least-once delivery** — the protocol in `AtLeastOnceDelivery`: read, hand to the application, and
commit only if that succeeded; otherwise rewind and pause. It knows nothing about Kafka.

**Rewind (`SeekBack`)** — after a failed handler, not committing is *not enough*: the subscription's read
position has already moved past the message, so without an explicit rewind the failed message is skipped
for the rest of the session and only reappears on restart or rebalance. This is the invariant with no
loud failure mode — delete the call and every build stays clean.

**Received delivery** — `ReceivedProductCreated`: one delivery attempt, carrying the event plus a
**position** that is opaque to the protocol and meaningful only to the subscription that issued it.
Typing the position as `object` is what keeps broker types out of the protocol.

## Configuration

**Producer options / consumer options** — `KafkaProducerOptions` and `KafkaConsumerOptions`, each bound
from the same `Kafka` section and each covering only its own subtree. A host binds and validates only the
keys it actually reads; the key *names* are shared with the configuration file and did not change when
the options split.
