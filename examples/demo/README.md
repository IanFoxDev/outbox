# Demo: a shop that runs for weeks

The [Laravel example](../laravel) under a steady stream of orders, with two relay
replicas from a released image and a dashboard that shows whether any event was lost
or counted twice. A public instance ran this stack as it is for six days in October 2026.

| Service | What it does |
|---|---|
| `app` | The Laravel shop. `POST /orders` records `OrderPlaced`, `POST /orders/{id}/pay` records `OrderPaid`, each in the transaction that writes the order. |
| `relay-1`, `relay-2` | `ghcr.io/ianfoxdev/outbox-relay`, the tag in `RELAY_TAG` (default `1.0.0-rc1`). One publishes to Kafka, the other waits for the lock. |
| `consumer` | `php artisan orders:consume`: adds up what each customer paid from `OrderPaid`, skipping events it has applied before. |
| `traffic` | [traffic.sh](traffic.sh): 1 to 4 orders a second from 500 customers. About 2 in 100 checkouts fail and roll back, 9 in 10 orders are paid, 1 in 14 is paid twice. |
| `stats` | Runs [stats.sql](stats.sql) once: per-customer totals on the shop side, kept by a trigger, and the view the dashboard reads. |
| `prometheus`, `grafana` | Relay metrics and the dashboard, open to anyone without a login, read-only. |

## Run

```bash
docker compose up -d
```

The first start installs the Composer packages of the example, which takes a few
minutes. Then open http://localhost:3000.

```bash
./kill-leader.sh        # SIGKILL the replica that leads; the other takes over
docker compose down -v
```

## The check

If the relay lost an event, or the consumer applied one twice, the money would no
longer add up. The shop keeps per-customer totals of what was paid (`demo_stats.shop_paid`,
written by a trigger in the same transaction as the payment). The consumer keeps its own
per-customer totals from Kafka (`customer_spend`). *Customers whose totals disagree* on
the dashboard counts customers whose last payment is more than a minute old and whose
two totals differ; anything else means the consumer is still behind. It should stay at 0
through relay failovers, broker restarts and redeployments.

The trigger and the view exist only for this dashboard. They are not part of the
pattern, and an application does not need them.

## Public instance

From 15 to 21 October 2026 a public instance ran on a small VPS under a separate account
with rootless Docker, behind a reverse proxy that sent HTTPS to Grafana only. It used
relay `1.0.0-rc1` and this stack unchanged. Its `.env` set:

```bash
GRAFANA_BIND=172.17.0.1          # the address the reverse proxy reaches
GRAFANA_PORT=8310
GRAFANA_ROOT_URL=https://<host>/
GRAFANA_COOKIE_SECURE=true
GRAFANA_ADMIN_PASSWORD=<random>  # the login form is off; this only guards the API
GRAFANA_DB_PASSWORD=<random>
```

A timer ran `./kill-leader.sh` every 6 hours. Grafana reached Postgres as `grafana_ro`,
which can read the one view in `demo_stats` and nothing else, for at most two seconds a
query. Prometheus limited a query to 10 seconds.

The result, read after the traffic was stopped and the consumer had caught up:

| | |
|---|---|
| Paid orders | 775,045 |
| Paid in the shop | EUR 37,763,969.02 |
| Counted by the consumer | EUR 37,763,969.02 |
| Customers whose totals disagree | 0 |
| Leader killed with SIGKILL | 25 times |

The instance has been shut down.
