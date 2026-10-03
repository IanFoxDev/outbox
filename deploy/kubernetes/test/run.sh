#!/usr/bin/env bash
# Deploys the relay to a kind cluster and checks what the manifests promise: both
# replicas become ready, exactly one leads, a row gets published, and after the
# leader's pod is deleted the other replica takes over.
#
#   deploy/kubernetes/test/run.sh             # needs docker, kind and kubectl
#   KIND="go run sigs.k8s.io/kind@v0.33.0" deploy/kubernetes/test/run.sh
#   KEEP=1 ...                                # leave the cluster running afterwards
#   RELAY_IMAGE=ghcr.io/ianfoxdev/outbox-relay:0.3 ...   # a released image, not a build
set -euo pipefail

cd "$(dirname "$0")/../../.."
KIND=${KIND:-kind}
CLUSTER=${CLUSTER:-outbox-test}
NS=outbox-test

if ! $KIND get clusters | grep -qx "$CLUSTER"; then
	$KIND create cluster --name "$CLUSTER" --wait 120s
fi
if [ -z "${KEEP:-}" ]; then
	trap '$KIND delete cluster --name "$CLUSTER"' EXIT
fi
kubectl config use-context "kind-$CLUSTER" >/dev/null

# A released image is pulled by the node itself: kind load fails on multi-arch images
# taken from a registry.
image=outbox-relay:ci
if [ -n "${RELAY_IMAGE:-}" ]; then
	image=$RELAY_IMAGE
else
	docker build -t outbox-relay:ci relay
	$KIND load docker-image outbox-relay:ci --name "$CLUSTER"
fi

kubectl kustomize --load-restrictor LoadRestrictionsNone deploy/kubernetes/test |
	sed "s#image: outbox-relay:ci#image: $image#" | kubectl apply -f -
kubectl -n $NS rollout status deploy/postgres --timeout 180s
kubectl -n $NS rollout status deploy/outbox-relay --timeout 180s

# The relay image has no shell, so metrics go through the API server's pod proxy.
leader_of() {
	kubectl get --raw "/api/v1/namespaces/$NS/pods/$1:8080/proxy/metrics" 2>/dev/null |
		awk '$1 == "outbox_leader" { print $2 }'
}
leaders() {
	for pod in $(kubectl -n $NS get pods -l app.kubernetes.io/name=outbox-relay \
		--field-selector=status.phase=Running -o name | cut -d/ -f2); do
		[ "$(leader_of "$pod")" = 1 ] && echo "$pod"
	done
	return 0
}
wait_for_one_leader() {
	local found=""
	for _ in $(seq 60); do
		found=$(leaders)
		if [ -n "$found" ] && [ "$(echo "$found" | wc -l | tr -d ' ')" = 1 ]; then
			echo "$found"
			return
		fi
		sleep 2
	done
	echo "want exactly one leader, got: ${found:-none}" >&2
	exit 1
}
psql() {
	kubectl -n $NS exec deploy/postgres -- psql -U app -tAc "$1"
}
publish_one() {
	local id
	id=$(psql "INSERT INTO outbox (event_id, source, event_type, aggregate_type, aggregate_id, content_type, payload)
		VALUES (gen_random_uuid(), '/k8s', 'OrderPlaced', 'order', '42', 'application/json', '{}') RETURNING id" | head -1)
	for _ in $(seq 30); do
		if [ "$(psql "SELECT published_at IS NOT NULL FROM outbox WHERE id = $id")" = t ]; then
			echo "row $id published"
			return
		fi
		sleep 1
	done
	echo "row $id was not published" >&2
	exit 1
}

leader=$(wait_for_one_leader)
echo "leader: $leader"
kubectl -n $NS wait pod -l app.kubernetes.io/name=outbox-relay --for=condition=Ready --timeout 60s
publish_one

kubectl -n $NS delete pod "$leader" --wait=false
for _ in $(seq 60); do
	next=$(wait_for_one_leader)
	[ "$next" != "$leader" ] && break
	sleep 2
done
echo "new leader: $next"
[ "$next" != "$leader" ] || { echo "leadership did not move" >&2; exit 1; }
publish_one
kubectl -n $NS rollout status deploy/outbox-relay --timeout 120s
echo "ok"
