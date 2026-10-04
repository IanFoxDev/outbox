# Relay

## Run

```sh
cd relay && go build -o bin/outbox-relay ./cmd/outbox-relay
OUTBOX_DATABASE_URL=postgres://app:secret@localhost:5432/app \
OUTBOX_KAFKA_BROKERS=localhost:9092 \
bin/outbox-relay
```

The relay does not create topics. With the default template an `order` aggregate goes
to `order.events`, create it with as many partitions as you need before starting.

For RabbitMQ, set `OUTBOX_PUBLISHER=rabbitmq`, `OUTBOX_RABBITMQ_URL` and
`OUTBOX_RABBITMQ_EXCHANGE` instead of the Kafka settings. The relay does not declare
the exchange, queues or bindings either; see [RabbitMQ messages](#rabbitmq-messages).

With `OUTBOX_PUBLISHER=stdout` every event is printed as one JSON line to stdout instead,
logs go to stderr. Use it to see what the PHP side writes before Kafka is involved.

## Docker

The image is built from `relay/Dockerfile`: a static binary on `distroless/static`,
running as `nonroot`, for `linux/amd64` and `linux/arm64`, about 33 MB. It is published
as `ghcr.io/ianfoxdev/outbox-relay` with the tags `0.4.0`, `0.4` and `latest`; build it
locally with `make relay-image`.

```sh
docker run --rm -p 8080:8080 \
  -e OUTBOX_DATABASE_URL=postgres://app:secret@db:5432/app \
  -e OUTBOX_KAFKA_BROKERS=kafka:9092 \
  ghcr.io/ianfoxdev/outbox-relay:0.4
```

The image has no shell. Its `HEALTHCHECK` runs `/outbox-relay healthcheck`, which asks
the relay's own `/healthz`. For Kubernetes there are kustomize manifests in
[deploy/kubernetes](../deploy/kubernetes/README.md).

### Try it with compose

`compose.yaml` in the repository root starts Postgres with the outbox table, Kafka with
an `order.events` topic, and two relay replicas:

```sh
docker compose up --build -d          # RELAY_PORT=18080 if 8080 is taken
docker compose logs relay relay-standby | grep -E 'leader|standing'
```

Write an event the way the PHP package would, and read it from Kafka:

```sh
docker compose exec postgres psql -U app -c "INSERT INTO outbox
  (event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
  VALUES (gen_random_uuid(), '/orders', 'OrderPlaced', 'order', '42',
          'application/json', '{\"total\": 1999}')"

docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic order.events --from-beginning \
  --formatter-property print.key=true --formatter-property print.headers=true
```

Stop the replica that logged `became leader` with `docker compose stop`, insert another
row, and the other one publishes it a few seconds later. Metrics are on
`http://localhost:8080/metrics`, Kafka is reachable from the host on `localhost:9094`.

### The same with RabbitMQ

`compose.rabbitmq.yaml` replaces Kafka with RabbitMQ 4.3. The exchange `events` and a
quorum queue `order.events` bound to `order.#` come from
[examples/rabbitmq/definitions.json](../examples/rabbitmq/definitions.json), the way a
broker team would set them up:

```sh
docker compose -f compose.yaml -f compose.rabbitmq.yaml up --build -d
```

Insert the row as above. The message shows up in the management UI on
`http://localhost:15672` (user `app`, password `app`): open the `order.events` queue and
use Get messages.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `OUTBOX_DATABASE_URL` | required | Database with the outbox table: `postgres://` or `mysql://user:pass@host:3306/db`. The scheme picks the driver. |
| `OUTBOX_LOCK_DATABASE_URL` | `OUTBOX_DATABASE_URL` | Connection for the leader lock, same database. Must reach the server directly, see below. |
| `OUTBOX_TABLE` | `outbox` | Table name, optionally with a schema. |
| `OUTBOX_LOCK_ID` | derived from the table name | Advisory lock key. Replicas with the same key elect one leader. |
| `OUTBOX_BATCH_SIZE` | `500` | Rows per query, 1 to 10000. |
| `OUTBOX_POLL_INTERVAL` | `500ms` | Pause after a batch that was not full. A full batch is followed by the next one at once. |
| `OUTBOX_LOCK_RETRY_INTERVAL` | `5s` | How often a standby replica tries to take the lock. |
| `OUTBOX_RETENTION` | `24h` | How long published rows stay in the table. `0s` deletes them on the next cleanup run. |
| `OUTBOX_CLEANUP_INTERVAL` | `1m` | How often the leader deletes rows past the retention. |
| `OUTBOX_HTTP_ADDR` | `:8080` | Where `/metrics`, `/healthz` and `/readyz` are served. |
| `OUTBOX_LOG_LEVEL` | `info` | `debug`, `info`, `warn` or `error`. At `debug` every published batch is logged with its first and last id. |
| `OUTBOX_LOG_FORMAT` | `json` | `json`, or `text` for reading logs by eye. Logs go to stderr. |
| `OUTBOX_PUBLISHER` | `kafka` | `kafka`, `rabbitmq`, or `stdout` for debugging. |
| `OUTBOX_KAFKA_BROKERS` | required for kafka | Seed brokers, comma-separated `host:port`. |
| `OUTBOX_KAFKA_TOPIC` | `{aggregate_type}.events` | Topic template, `{aggregate_type}` and `{event_type}` are replaced. |
| `OUTBOX_KAFKA_CLIENT_ID` | `outbox-relay` | Client id seen by the brokers. |
| `OUTBOX_KAFKA_DELIVERY_TIMEOUT` | `30s` | How long one batch may wait for acknowledgements. |
| `OUTBOX_KAFKA_TLS` | `false` | TLS with the system root certificates. |
| `OUTBOX_KAFKA_SASL_MECHANISM` | none | `PLAIN`, `SCRAM-SHA-256` or `SCRAM-SHA-512`. |
| `OUTBOX_KAFKA_SASL_USER`, `OUTBOX_KAFKA_SASL_PASSWORD` | | Credentials for SASL. |
| `OUTBOX_RABBITMQ_URL` | required for rabbitmq | `amqp://user:pass@host:5672/vhost`, or `amqps://` for TLS with the system root certificates. Percent-encode special characters in the password. |
| `OUTBOX_RABBITMQ_EXCHANGE` | required for rabbitmq | Exchange that receives every message. It must exist. |
| `OUTBOX_RABBITMQ_ROUTING_KEY` | `{aggregate_type}.{event_type}` | Routing key template, `{aggregate_type}` and `{event_type}` are replaced. At most 255 bytes. |
| `OUTBOX_RABBITMQ_CONFIRM_TIMEOUT` | `30s` | How long one batch may wait for publisher confirms. |

## Kafka records

Each row becomes one record in CloudEvents binary mode
([ADR 0003](adr/0003-cloudevents-binary-mode.md)): the key is `aggregate_id`, the value
is the payload as the application wrote it, and the attributes are headers:

```
ce_specversion: 1.0
ce_id: e00e1b78-c10d-4540-8f2e-f78217ab60c3
ce_source: /orders
ce_type: OrderPlaced
ce_time: 2026-09-29T13:44:30.269972Z
ce_subject: 42
ce_partitionkey: 42
content-type: application/json
traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

The producer is idempotent with `acks=all` and partitions keys the way the Java client
does, so one aggregate always lands in one partition in order
([ADR 0004](adr/0004-franz-go.md)).

## RabbitMQ messages

Each row becomes one persistent message to `OUTBOX_RABBITMQ_EXCHANGE`
([ADR 0007](adr/0007-rabbitmq.md)). With the default template, `OrderPlaced` of an
`order` goes out with the routing key `order.OrderPlaced`, so a topic exchange lets a
queue subscribe to `order.#` or `*.OrderPaid`. The body is the payload as the
application wrote it. The properties and headers carry the CloudEvents attributes:

```
message_id:   e00e1b78-c10d-4540-8f2e-f78217ab60c3
type:         OrderPlaced
app_id:       /orders
timestamp:    2026-09-29T13:44:30Z
content_type: application/json
delivery_mode: 2 (persistent)
headers:
  cloudEvents_specversion: 1.0
  cloudEvents_id: e00e1b78-c10d-4540-8f2e-f78217ab60c3
  cloudEvents_source: /orders
  cloudEvents_type: OrderPlaced
  cloudEvents_time: 2026-09-29T13:44:30.269972Z
  cloudEvents_subject: 42
  cloudEvents_partitionkey: 42
  traceparent: 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01
```

The `cloudEvents_` prefix is the one the CloudEvents AMQP binding uses. RabbitMQ turns
these headers into AMQP 1.0 application properties, so a CloudEvents SDK on AMQP 1.0
reads them as attributes.

The relay declares nothing. Create the exchange, the queues and the bindings before it
starts; `/readyz` reports a missing exchange. Messages are published `mandatory`: one
that no queue is bound for comes back, counts as failed, and holds back the rest of its
aggregate. A missing binding does not lose events, it makes them wait.

**Order.** RabbitMQ keeps the messages of one channel in order inside a queue, but it
has no idempotent producer: if message N fails and N+1 is already in the queue, the
retry of N lands behind it. So the relay publishes a batch in waves. Wave i holds the
i-th event of every aggregate in the batch, and the next wave goes out only after the
broker confirmed the previous one. With many aggregates per batch that is one wave and
costs nothing; a backlog of one busy aggregate goes out at one confirm per event. The
numbers are in [benchmarks.md](benchmarks.md#rabbitmq).

The relay keeps order up to the queue. Several consumers on one queue lose it again;
[consuming.md](consuming.md#from-rabbitmq) shows how to keep it.

## Metrics

`/metrics` on `OUTBOX_HTTP_ADDR`:

| Metric | Type | Meaning |
|---|---|---|
| `outbox_lag_seconds` | gauge | Age of the oldest unpublished row, 0 when there is none. |
| `outbox_pending_rows` | gauge | Unpublished rows in the table. |
| `outbox_backlog_up` | gauge | 1 if the query behind the two above succeeded. |
| `outbox_published_total` | counter | Rows published and marked, by `aggregate_type`. |
| `outbox_publish_errors_total` | counter | Batches that were not fully published. |
| `outbox_deleted_total` | counter | Rows deleted by the cleanup. |
| `outbox_leader` | gauge | 1 on the replica that holds the lock. |
| `outbox_relay_info` | gauge | Always 1, with the `version` label. |

Go runtime and process metrics are there too.

Lag and pending rows are read from the table on every scrape, by every replica. If the
leader dies and no standby takes over, they keep growing, which is exactly when you want
the alert. The query reads the partial index on unpublished rows, so it stays cheap
while the backlog is small; with millions of pending rows the count takes longer, and
the scrape with it.

Alerts to start with. What to do when one fires is in [runbook.md](runbook.md).

```yaml
groups:
  - name: outbox
    rules:
      - alert: OutboxLagging
        expr: max(outbox_lag_seconds) > 60
        for: 2m
        annotations:
          summary: Events wait in the outbox for more than a minute
      - alert: OutboxNoLeader
        # absent: when every replica is down there is no outbox_leader series at all,
        # sum() returns nothing, and without this neither alert would fire.
        expr: sum(outbox_leader) < 1 or absent(outbox_leader)
        for: 1m
        annotations:
          summary: No relay replica holds the leader lock
      - alert: OutboxPublishErrors
        expr: increase(outbox_publish_errors_total[5m]) > 0
        annotations:
          summary: Some outbox batches failed to publish, see the relay log
```

## Health

- `/healthz` answers 200 while the process runs. Use it as the liveness probe.
- `/readyz` pings the database, checks that the outbox table exists, and pings the
  broker: the Kafka brokers, or the RabbitMQ connection and the exchange (a passive
  declare). A relay started before the application's migration is not ready until the
  table appears. It answers 503 with the failed
  check, for example `kafka: unable to dial ...` or `rabbitmq: exchange "events": ...
  NOT_FOUND`. A standby replica is ready too: it publishes nothing, but can take over
  at any moment.

A broken broker makes the relay not ready, not unhealthy: restarting it would not fix
the broker, and the lag alert already says that events are waiting.

## Cleanup

The leader deletes rows published more than `OUTBOX_RETENTION` ago, every
`OUTBOX_CLEANUP_INTERVAL`, in statements of 10000 rows so no single transaction holds
locks or writes WAL for long. Unpublished rows are never deleted, however old.

Keeping a day of published rows answers "did this event go out, and when" with a
query instead of a Kafka search. If the table grows too fast for that, lower the
retention: every published row costs one update now and one delete later, and both
leave dead tuples for autovacuum.

## When publishing fails

A row is marked published only when the broker acknowledged it and every earlier row of
the same aggregate. Once a row of an aggregate fails, the later rows of that aggregate are
not sent in this batch, or stay unmarked if they were already sent, and go out again
after it. Other aggregates in the batch are not affected.

The relay keeps the lock and retries. The pause after a failed batch starts at
`OUTBOX_POLL_INTERVAL` and doubles up to 30 seconds, and drops back after the first
clean batch.

Each failure is logged as `batch not fully published`. When the same error comes back
on every retry, as it does while a broker is down, it is logged again at most once a
minute, with `repeats` counting the retries in between. A different error is logged at
once. The first clean batch after failures logs `publishing recovered` with
`failed_batches`. Another replica would meet the same broker or the same bad row, so
publish errors do not end the leader term. Database errors do.

Typical cases:

| Failure | What happens |
|---|---|
| Kafka is down | Every batch fails after `OUTBOX_KAFKA_DELIVERY_TIMEOUT`, rows wait in the table, the relay retries every 30 seconds at most. |
| Topic of one aggregate type is missing | That aggregate type waits, the others are published. Once the topic is created, the waiting rows go out in order. |
| A row builds an invalid topic name | That aggregate is stuck until the row is fixed or deleted, the log names the row id. |
| RabbitMQ is down | Every batch fails to connect, rows wait, the relay retries every 30 seconds at most and reconnects. |
| The RabbitMQ exchange is missing | Nothing is published and `/readyz` says why. Once the exchange exists, the relay reconnects and goes on. |
| No queue is bound for one routing key | The broker returns those messages. Their aggregates wait, the others are published. Once the binding exists, the waiting rows go out in order. |
| A RabbitMQ queue rejects messages (`x-overflow: reject-publish` and full) | The broker answers with `basic.nack`. The aggregate waits until the queue has room. |

## Replicas

Run two or three replicas for availability. One of them takes a session lock and
publishes, the others log `standing by` and wait. On PostgreSQL the lock is
`pg_try_advisory_lock(OUTBOX_LOCK_ID)`, on MySQL `GET_LOCK('outbox-relay:<OUTBOX_LOCK_ID>', 0)`.
When the leader stops or its database session dies, the database frees the lock and a
standby takes over within `OUTBOX_LOCK_RETRY_INTERVAL`.

The leader checks its lock session five times per retry interval (every second with the
defaults, each check with a one second timeout). A new leader waits two such checks
before it starts, so the old one has stopped by then. A leader whose database answers
slower than that steps down and competes again. Why a single leader at all is in
[ADR 0002](adr/0002-single-active-relay.md).

`TestKillTheLeaderUnderLoad` (in `relay/cmd/outbox-relay`) checks this with real
processes. Two replicas share a lock while 20 writers insert about 100000 events in 15
seconds, and the leader is killed with SIGKILL four times. Every event reaches Kafka, and
each aggregate's events, with duplicates dropped by `ce_id`, arrive in the order they
were written. Duplicates vary from 0 to about 200 per run, depending on whether a kill
lands between producing a batch and marking it. The same test fails, with hundreds of
events lost, against a relay that marks rows before producing them.

Through PgBouncer in transaction mode, or ProxySQL with multiplexing, a session lock is
taken on whatever server connection happens to serve the query, and every replica can
"win". Point `OUTBOX_LOCK_DATABASE_URL` at the database server itself, or at a pool in
session mode, and keep `OUTBOX_DATABASE_URL` on the transaction pool if you like.
