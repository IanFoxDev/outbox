# Go targets use the local toolchain when available, otherwise the same commands run in Docker.
GO_IMAGE ?= golang:1.27
ifeq ($(shell command -v go 2>/dev/null),)
GO = docker run --rm -v $(CURDIR)/relay:/src -v outbox-gocache:/root/.cache -w /src $(GO_IMAGE)
else
GO = cd relay &&
endif
LINT_IMAGE ?= golangci/golangci-lint:v2.14.0
PG_IMAGE ?= postgres:18-alpine
OUTBOX_PG_DSN ?= pgsql:host=127.0.0.1;port=55432;dbname=outbox;user=outbox;password=outbox
MYSQL_IMAGE ?= mysql:9
# root: the tests create and drop a second database.
OUTBOX_MYSQL_DSN ?= mysql:host=127.0.0.1;port=53306;dbname=outbox;user=root;password=root
OUTBOX_TEST_DATABASE_URL ?= postgres://outbox:outbox@127.0.0.1:55432/outbox
KAFKA_IMAGE ?= apache/kafka:4.3.1
OUTBOX_TEST_KAFKA_BROKERS ?= 127.0.0.1:59092

.PHONY: test php-test php-stan postgres-up postgres-down mysql-up mysql-down kafka-up kafka-down relay-image loadtest relay-test relay-vet relay-lint relay-build

test: php-test relay-test

# Integration tests are skipped unless OUTBOX_PG_DSN and OUTBOX_MYSQL_DSN point to databases:
# run postgres-up and mysql-up first.
php-test:
	OUTBOX_PG_DSN="$(OUTBOX_PG_DSN)" OUTBOX_MYSQL_DSN="$(OUTBOX_MYSQL_DSN)" vendor/bin/phpunit

php-stan:
	vendor/bin/phpstan analyse

postgres-up:
	docker run -d --rm --name outbox-pg -p 55432:5432 \
		-e POSTGRES_USER=outbox -e POSTGRES_PASSWORD=outbox -e POSTGRES_DB=outbox $(PG_IMAGE)
	# The init server listens on the socket only, so wait for TCP.
	until docker exec outbox-pg pg_isready -h 127.0.0.1 -U outbox -q; do sleep 0.5; done

postgres-down:
	docker rm -f outbox-pg

mysql-up:
	docker run -d --rm --name outbox-mysql -p 53306:3306 \
		-e MYSQL_ROOT_PASSWORD=root -e MYSQL_DATABASE=outbox $(MYSQL_IMAGE)
	until docker exec outbox-mysql mysql -h127.0.0.1 -uroot -proot -e 'SELECT 1' outbox >/dev/null 2>&1; do sleep 1; done

mysql-down:
	docker rm -f outbox-mysql

kafka-up:
	docker run -d --rm --name outbox-kafka -p 59092:9092 \
		-e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
		-e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
		-e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:59092 \
		-e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
		-e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
		-e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
		-e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
		-e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 \
		$(KAFKA_IMAGE)
	until docker logs outbox-kafka 2>&1 | grep -q "Kafka Server started"; do sleep 1; done

kafka-down:
	docker rm -f outbox-kafka

# Relay integration tests need the local go toolchain, postgres-up and kafka-up. In Docker they are skipped.
relay-test:
	$(GO) env OUTBOX_TEST_DATABASE_URL="$(OUTBOX_TEST_DATABASE_URL)" OUTBOX_TEST_KAFKA_BROKERS="$(OUTBOX_TEST_KAFKA_BROKERS)" go test -race -count=1 ./...

relay-vet:
	$(GO) go vet ./...

relay-lint:
	docker run --rm -v $(CURDIR)/relay:/src -v outbox-lintcache:/root/.cache -w /src $(LINT_IMAGE) golangci-lint run

relay-build:
	$(GO) go build -o bin/outbox-relay ./cmd/outbox-relay

relay-image:
	docker build -t outbox-relay:dev --build-arg VERSION=$$(git describe --tags --always --dirty) relay

# Needs postgres-up and kafka-up. Prints Markdown rows, see docs/benchmarks.md.
loadtest:
	cd relay && go build -o bin/outbox-loadtest ./cmd/outbox-loadtest
	for b in 100 500 2000; do relay/bin/outbox-loadtest -mode drain -rows 200000 -batch $$b; done
	for w in 8 32; do relay/bin/outbox-loadtest -mode steady -writers $$w -duration 30s; done

