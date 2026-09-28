# 0003. CloudEvents in Kafka binary mode, without the SDK

Date: 2026-09-28. Status: accepted.

## Context

Consumers need a few fields besides the payload: event id for deduplication, event type
to pick a handler, time, and where the event came from. Every team invents its own
envelope for that, and every consumer has to learn it.

CloudEvents 1.0 already defines these fields and how to put them into a Kafka message.
It has two modes:

- **Structured**: the Kafka value is a JSON document with the attributes and the payload
  inside `data`.
- **Binary**: the attributes go into Kafka headers (`ce_id`, `ce_type`, ...), the value
  is the payload as is.

## Decision

Use binary mode. Each message gets these headers:

| Header | Taken from |
|---|---|
| `ce_specversion` | `1.0` |
| `ce_id` | `event_id` |
| `ce_source` | `source` (set in the PHP config, for example `urn:shop:orders`) |
| `ce_type` | `event_type` |
| `ce_time` | `created_at`, RFC 3339 |
| `ce_subject` | `aggregate_id` |
| `ce_partitionkey` | `aggregate_id` (CloudEvents partitioning extension) |
| `content-type` | `content_type` |

Headers from the `headers` column (for example `traceparent`) are added as they are.

The PHP side writes these columns itself. It does not depend on `cloudevents/sdk-php`:
the envelope is a handful of fields, and the SDK would be one more dependency with its own
release pace for very little code.

## Consequences

- A consumer that knows nothing about CloudEvents reads the value and gets the plain
  payload. That is the main reason for binary over structured mode.
- Payloads can be Avro or Protobuf, the relay does not parse them.
- Consumers that use a CloudEvents SDK (Java, Go, .NET, Python) read the headers without
  extra code.
- Structured mode can be added later as a relay option if someone needs it, for example
  for a sink that drops headers.
