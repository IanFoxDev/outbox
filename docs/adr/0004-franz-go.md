# 0004. franz-go as the Kafka client

Date: 2026-09-29. Status: accepted.

## Context

The relay needs a Kafka producer that keeps records of one key in order across retries,
partitions keys the same way the Java client does, and speaks TLS and SASL for managed
clusters. The relay ships as a small static binary in a distroless image, so a
dependency on C libraries is a real cost.

Three Go clients are in use:

- `confluentinc/confluent-kafka-go`: a wrapper around librdkafka. Mature, but needs cgo,
  which rules out a static binary and `distroless/static`, and pins the build to a C
  toolchain on every platform.
- `segmentio/kafka-go`: pure Go and simple, but its producer is not idempotent, and a
  retry after a timeout can reorder or duplicate records within a partition. Still 0.x
  and slow to change.
- `twmb/franz-go`: pure Go, actively maintained, idempotent producer on by default,
  full protocol coverage including KRaft-era APIs, TLS, SASL PLAIN and SCRAM.

## Decision

Use `franz-go` (`github.com/twmb/franz-go/pkg/kgo`). The producer is configured with:

- `RequiredAcks(AllISRAcks())` and the default idempotent producer, so client retries
  cannot reorder a partition;
- `StickyKeyPartitioner(nil)`, which hashes keys with murmur2 exactly as the Java client
  does. A test compares the chosen partition with a port of the Java hash;
- `RecordDeliveryTimeout` from `OUTBOX_KAFKA_DELIVERY_TIMEOUT`, so a batch cannot hang
  forever on a missing topic or a dead cluster.

`pkg/kadm` is used only in tests to create topics.

## Consequences

- The relay stays a static binary, one Go module with no cgo.
- `ProduceSync` returns results in completion order, not input order. The publisher maps
  results back to rows by record pointer.
- The record timestamp is the produce time. The event time travels in `ce_time`, so
  events that waited out a long outage do not fall under topic retention at once.
