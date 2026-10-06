#!/bin/sh
# Kills the relay replica that holds the lock with SIGKILL and starts it again, so the
# other one takes over. The public demo runs it from cron every 6 hours.
#
#   ./kill-leader.sh
set -eu
cd "$(dirname "$0")"

leader=$(docker compose exec -T prometheus \
    wget -qO- 'http://localhost:9090/api/v1/query?query=outbox_leader%3D%3D1' \
    | sed -n 's/.*"instance":"\(relay-[0-9]*\):8080".*/\1/p')

if [ -z "$leader" ]; then
    echo "no leader in Prometheus, nothing killed" >&2
    exit 1
fi

# Plain docker, not compose: compose start would also run the one-off dependencies.
id=$(docker compose ps -q "$leader")
docker kill -s KILL "$id" > /dev/null
docker start "$id" > /dev/null
echo "$(date -u +%FT%TZ) killed $leader"
