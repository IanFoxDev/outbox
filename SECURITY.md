# Security

The package writes into your database through your connection, and the relay reads that
table and writes to your Kafka. Both run with whatever credentials you give them.

If you find a vulnerability, for example a way to inject SQL through a message or a
setting, or to make the relay publish rows from another table, do not open a public
issue. Report it privately through
[GitHub](https://github.com/IanFoxDev/outbox/security/advisories/new), or write to
ianfoxdeveloper@gmail.com.

Some things are by design and are not vulnerabilities:

- The relay's HTTP port serves metrics and health checks without authentication. Do not
  expose it outside your network.
- Event payloads are stored and published as they are. Do not put secrets or card data
  into events.

## Supported versions

Fixes go into the latest release only. Until 1.0 that is the latest `0.x` tag.
