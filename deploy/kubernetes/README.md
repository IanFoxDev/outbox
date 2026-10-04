# Kubernetes

Kustomize manifests for the relay. The PHP side needs nothing here: it writes to the
outbox table through the application's own connection.

| Path | What it is |
|---|---|
| `base/` | Deployment with two replicas, Service for the metrics port, PodDisruptionBudget |
| `components/monitoring/` | ServiceMonitor and PrometheusRule for the Prometheus Operator |
| `test/` | What CI runs on a kind cluster: Postgres in the cluster and the stdout publisher. Not for production. |

## Use it

The base reads its settings from a ConfigMap and a Secret, both named `outbox-relay`.
An overlay in your own repository supplies them:

```yaml
# kustomization.yaml
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: shop
resources:
  - https://github.com/IanFoxDev/outbox//deploy/kubernetes/base?ref=v0.4.0
components:
  # Only with the Prometheus Operator CRDs installed.
  - https://github.com/IanFoxDev/outbox//deploy/kubernetes/components/monitoring?ref=v0.4.0
images:
  - name: ghcr.io/ianfoxdev/outbox-relay
    newTag: "0.4"
configMapGenerator:
  - name: outbox-relay
    literals:
      - OUTBOX_KAFKA_BROKERS=kafka-0.kafka:9092,kafka-1.kafka:9092
      - OUTBOX_KAFKA_TOPIC={aggregate_type}.events
```

The Secret holds what contains a password, usually from your secret store:

```sh
kubectl -n shop create secret generic outbox-relay \
  --from-literal=OUTBOX_DATABASE_URL='postgres://relay:...@pgbouncer:6432/shop' \
  --from-literal=OUTBOX_LOCK_DATABASE_URL='postgres://relay:...@postgres:5432/shop'
```

Every setting is in [docs/relay.md](../../docs/relay.md#configuration). Two matter more
on Kubernetes than elsewhere:

- `OUTBOX_LOCK_DATABASE_URL` must reach PostgreSQL or MySQL directly. The leader lock
  belongs to a database session, and PgBouncer in transaction mode or ProxySQL with
  multiplexing hands that session to someone else.
- `OUTBOX_LOCK_ID` must be the same for all replicas of one relay, and different for
  relays of different outbox tables in one database. The default, derived from the
  table name, does both.

## What the manifests decide

- **Two replicas, one publishing.** More replicas do not publish faster: one holds the
  lock, the rest wait to take over (ADR 0002). Do not add a HorizontalPodAutoscaler.
- **Probes.** Liveness is `/healthz`: the process runs. Readiness is `/readyz`: the
  database and the broker answer. A broker outage makes the pods not ready, not
  restarted, because a restart would not fix the broker. The `OutboxLagging` alert is
  what tells you events are waiting.
- **Rollouts** replace one pod at a time (`maxUnavailable: 0`), so a standby is up
  while the leader is replaced. Leadership moves within `OUTBOX_LOCK_RETRY_INTERVAL`
  (5 seconds by default) after the old leader's session closes.
- **PodDisruptionBudget** `minAvailable: 1` keeps one replica through a node drain.
- **Security context.** Non-root user 65532, read-only root filesystem, no capabilities,
  no service account token: the relay needs none of them.
- **Resources.** An idle relay used about 20 MiB and next to no CPU (0.03 s of CPU in
  40 s of polling), so the requests are 50m CPU and 32 MiB. A leader draining a backlog
  took about a quarter of a core on a laptop
  ([benchmarks](../../docs/benchmarks.md)). Memory grows with `OUTBOX_BATCH_SIZE` and the
  payload size; the 256 MiB limit leaves room for batches of a few thousand rows of a
  few kilobytes.

## Test it

```sh
deploy/kubernetes/test/run.sh
```

It creates a kind cluster, builds the relay image, deploys `test/`, and checks that
both replicas become ready, exactly one leads, a row gets published, and after the
leader's pod is deleted the other replica takes over and publishes the next row. It
needs Docker, `kind` and `kubectl`. `KIND="go run sigs.k8s.io/kind@v0.33.0"` runs kind
without installing it, `RELAY_IMAGE=ghcr.io/ianfoxdev/outbox-relay:0.4` tests a
released image instead of a build, `KEEP=1` leaves the cluster running.
