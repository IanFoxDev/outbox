# 0001. One repository for the PHP package and the relay

Date: 2026-09-28. Status: accepted.

## Context

The project has two parts written in two languages. The PHP package inserts rows, the Go
relay reads them. Neither is useful alone, and the table layout is the contract between
them: a new column or a changed index has to land in the migration, in the PHP writer
and in the relay query at the same time.

People install the PHP part with Composer and the relay as a Docker image. Neither of
them should need the other language's toolchain.

Why the relay is in Go and not a PHP worker: it is a long-running process that holds a
database lock, keeps a Kafka producer open and serves metrics. Go gives one static
binary in a small image, and the team using the package never has to touch Go.
A PHP worker for teams that do not want a second container can come later.

## Decision

One repository. The Composer package lives at the root, so Packagist reads
`composer.json` directly and no split repository is needed. The relay is a separate Go
module in `relay/` (`github.com/ianfoxdev/outbox/relay`) with its own `go.mod`.

`relay/`, `docs/`, `examples/` and `tests/` are marked `export-ignore` in
`.gitattributes`, so `composer require` downloads only the PHP code.

One version number covers both parts. Tag `v0.1.0` releases the Composer package and
the image `ghcr.io/ianfoxdev/outbox-relay:0.1.0`. The relay is not meant to be imported
as a Go library (everything is under `internal/`), so it does not get its own
`relay/vX.Y.Z` tags.

## Consequences

- A change to the table is one pull request with the migration, the PHP code and the
  relay query, tested together in CI.
- A release of the package also releases the relay even when only one of them changed.
  Acceptable while the project is small.
- CI runs PHP and Go jobs separately, filtered by path, so a README change does not
  build the image.
