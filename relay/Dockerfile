# syntax=docker/dockerfile:1

# Cross-compiles on the build machine for each target platform, no emulation needed.
FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS TARGETARCH
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/outbox-relay ./cmd/outbox-relay

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/outbox-relay /outbox-relay
EXPOSE 8080
USER nonroot:nonroot
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s CMD ["/outbox-relay", "healthcheck"]
ENTRYPOINT ["/outbox-relay"]
