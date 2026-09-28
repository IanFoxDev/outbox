# Go targets use the local toolchain when available, otherwise the same commands run in Docker.
GO_IMAGE ?= golang:1.27
ifeq ($(shell command -v go 2>/dev/null),)
GO = docker run --rm -v $(CURDIR)/relay:/src -v outbox-gocache:/root/.cache -w /src $(GO_IMAGE)
else
GO = cd relay &&
endif
LINT_IMAGE ?= golangci/golangci-lint:v2.14.0

.PHONY: test relay-test relay-vet relay-lint relay-build

test: relay-test

relay-test:
	$(GO) go test -race ./...

relay-vet:
	$(GO) go vet ./...

relay-lint:
	docker run --rm -v $(CURDIR)/relay:/src -v outbox-lintcache:/root/.cache -w /src $(LINT_IMAGE) golangci-lint run

relay-build:
	$(GO) go build -o bin/outbox-relay ./cmd/outbox-relay
