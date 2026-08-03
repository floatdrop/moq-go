# relay

A single-instance MOQT relay (draft-ietf-moq-transport-19) over QUIC.

Accepts publisher and subscriber sessions, routes objects between them via the
track registry, and caches recent objects per track for late-joining subscribers.
No authentication — suitable for local development and testing.

## Usage

```
relay [-addr host:port] [-cert file] [-key file] [-webtransport [-webtransport-path /moq]]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `0.0.0.0:4433` | UDP address to listen on |
| `-cert` | — | PEM certificate file. If omitted, an ephemeral self-signed cert is generated. |
| `-key` | — | PEM private key file. Required when `-cert` is set. |
| `-webtransport` | `false` | Serve MOQT over WebTransport (HTTP/3) instead of raw QUIC. |
| `-webtransport-path` | `/moq` | HTTP/3 path for the WebTransport CONNECT (only used with `-webtransport`). |
| `-catalog-track-name` | `catalog` | Track name whose object cache uses `-catalog-ttl` instead of the default; empty disables the override. |
| `-catalog-ttl` | `0` | Per-object TTL for tracks matching `-catalog-track-name`; `0` means infinite retention (FIFO size cap still applies). |
| `-max-subscriptions` | `0` | Per-session cap on concurrent subscriptions (§13.1); `0` = unlimited. |
| `-max-namespace-requests` | `0` | Per-session cap on concurrent namespace requests (§13.7.1); `0` = unlimited. |

## Quick start

```sh
# Start the relay (ephemeral self-signed cert, port 4433)
go run ./cmd/relay

# Publish the current time every second to moq-example/clock
go run ./cmd/clock publish

# Subscribe and print each tick
go run ./cmd/clock subscribe
```

Clients connect with `InsecureSkipVerify: true` so no extra cert setup is needed
for the ephemeral cert. For a persistent cert (e.g. from Let's Encrypt or
`mkcert`):

```sh
go run ./cmd/relay -cert server.crt -key server.key
```

## Shutdown

The relay handles `SIGINT` and `SIGTERM`. On receipt it sends `GOAWAY` to all
active sessions with a 5-second grace period, then force-closes any that have
not drained.

## Interop testing

This directory ships a `Dockerfile` and `entrypoint-relay.sh` that package the
relay for the [moq-interop-runner](https://github.com/englishm/moq-interop-runner)
framework. The entrypoint maps the runner's `MOQT_*` environment variables onto
the relay's flags:

| Env | Default | Maps to |
|-----|---------|---------|
| `MOQT_PORT` | `4443` | `-addr 0.0.0.0:$MOQT_PORT` |
| `MOQT_CERT` / `MOQT_KEY` | `/certs/cert.pem`, `/certs/priv.key` | `-cert` / `-key` |
| `MOQT_TRANSPORT` | `quic` | `quic` → raw QUIC; `webtransport` → `-webtransport` |
| `MOQT_WEBTRANSPORT_PATH` | `/moq` | `-webtransport-path` (WebTransport only) |

### Run the suite locally

From the repository root, against a third-party test client (defaults to
`moq-dev-rs`, draft-18), over both transports:

```sh
make interop                     # raw QUIC + WebTransport
make interop-quic                # raw QUIC only
make interop-webtransport        # WebTransport only
make interop CLIENT_IMAGE=ghcr.io/englishm/moq-interop-runner-moq-test-client-draft-18:latest
make interop-matrix              # several draft-18 clients × transports, pass/skip/fail table
```

These build the relay image from source, generate a self-signed cert pair under
`interop/certs/`, and run [`interop/docker-compose.yml`](../../interop/docker-compose.yml)
(relay + client on a Compose bridge network, so it works on both Linux and
Docker Desktop for macOS). `interop-matrix` is driven by
[`interop/matrix.sh`](../../interop/matrix.sh); edit its `CLIENTS` list (or set
the `CLIENTS` env var) to add or mark clients.

### Client direction

The inverse — our own client ([`cmd/interop-client`](../interop-client)) against
a relay — is `make interop-client` (loopback by default; override
`CLIENT_RELAY_IMAGE` / `CLIENT_RELAY_URL` for a third-party relay). See that
command's README for details.

### Run via the runner itself

The relay is registered as `moq-go` in the runner's `implementations.json` (both a
`relay` and a `client` role). After building the images (`make interop-build`,
`make interop-client-build`):

```sh
cd ../moq-interop-runner
make interop-relay RELAY=moq-go       # our relay ← every registered client
make interop-client CLIENT=moq-go     # our client → every registered relay
```
