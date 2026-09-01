# relay

A single-instance MOQT relay (draft-ietf-moq-transport-20).

One UDP port serves both transport mappings: raw QUIC for clients dialing
`moqt://host:port`, and WebTransport (HTTP/3) at `-webtransport-path` for
browsers and anything dialing the `https://host:port/path` form of the same URI
(§3.1.4). Both are QUIC over UDP — the `https` scheme names an HTTP origin, not
a TCP transport — and the listener picks per connection from the negotiated
ALPN, so there is no transport to select.

Accepts publisher and subscriber sessions, routes objects between them via the
track registry, and caches recent objects per track for late-joining subscribers.
No authentication — suitable for local development and testing.

## Usage

```
relay [-addr host:port] [-cert file] [-key file] [-webtransport-path /moq]
      [-health-addr host:port] [-health-path /healthz]
```

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `0.0.0.0:4433` | UDP address to listen on |
| `-cert` | — | PEM certificate file. If omitted, an ephemeral self-signed cert is generated. |
| `-key` | — | PEM private key file. Required when `-cert` is set. |
| `-webtransport-path` | `/moq` | HTTP/3 path browsers use for the WebTransport CONNECT. Raw QUIC ignores it. |
| `-catalog-track-name` | `catalog` | Track name whose object cache uses `-catalog-ttl` instead of the default; empty disables the override. |
| `-catalog-ttl` | `0` | Per-object TTL for tracks matching `-catalog-track-name`; `0` means infinite retention (FIFO size cap still applies). |
| `-max-subscriptions` | `0` | Per-session cap on concurrent subscriptions (§13.1); `0` = unlimited. |
| `-max-namespace-requests` | `0` | Per-session cap on concurrent namespace requests (§13.7.1); `0` = unlimited. |
| `-health-addr` | — | TCP address for the HTTP health endpoint. Empty (the default) disables it. |
| `-health-path` | `/healthz` | Path on `-health-addr` that answers `200 OK`. |
| `-metrics` | `true` | Serve Prometheus metrics at `<health-path>/metrics`. Requires `-health-addr`. |
| `-metrics-tracks` | `catalog` | Comma-separated track names that keep their own `track` label; all others report as `other`. |

## Health endpoint

The MOQT port is UDP, so a TCP-only load-balancer probe or a Kubernetes
`httpGet` cannot reach it. `-health-addr` opts into a plain HTTP endpoint over
TCP that they can:

```sh
relay -health-addr 0.0.0.0:8080
curl -i http://localhost:8080/healthz   # 200 OK, body "ok"
```

It is off by default because the port is unauthenticated, and only a deployment
with a probe to satisfy needs it. Anything other than `-health-path` answers
404.

It reports **process liveness only** — that this handler is answering. It says
nothing about whether media is actually moving, and a relay looks healthy right
up until it stops forwarding. Use it as a liveness probe, not a readiness one.
That gap is what the metrics below are for.

## Metrics

Prometheus exposition rides the same port, one path below the health check:

```sh
relay -health-addr 0.0.0.0:8080
curl -s http://localhost:8080/healthz/metrics
```

Same port because the decision is the same one — plain HTTP over TCP,
unauthenticated — and an operator who exposed the health check has already made
it for both. A sub-path means a single ingress rule covers them. `-metrics=false`
turns the exposition off, and the path then 404s with everything else rather
than becoming a hole in it.

The exporter is written against the standard library rather than
`prometheus/client_golang`, which would cost seven modules and, measured on this
binary, 5.86 MiB — a 44% increase, most of it a protobuf runtime for an
exposition format this endpoint never emits. The trade is that promhttp's Go
runtime and process collectors (`go_goroutines`, `process_resident_memory_bytes`
and friends) are **not** exposed here; only the relay's own families are.

| metric | type | labels |
|--------|------|--------|
| `moqt_relay_sessions` | gauge | `leg` |
| `moqt_relay_subscriptions` | gauge | `track`, `leg` |
| `moqt_relay_objects_received_total` | counter | `track`, `leg`, `subgroup` |
| `moqt_relay_objects_forwarded_total` | counter | `track`, `leg`, `subgroup` |
| `moqt_relay_objects_dropped_total` | counter | `track`, `leg`, `subgroup` |
| `moqt_relay_subgroup_stream_resets_total` | counter | `track`, `leg`, `subgroup`, `cause` |
| `moqt_relay_subscription_resets_total` | counter | `track`, `leg`, `cause` |
| `moqt_relay_fetches_served_total` | counter | `track`, `leg` |
| `moqt_relay_fetch_objects_served_total` | counter | `track`, `leg` |
| `moqt_relay_upstream_dial_failures_total` | counter | — |
| `moqt_relay_namespace_lookups_total` | counter | `result` |

**Label cardinality is bounded on purpose.** Track names and Subgroup IDs both
come off the wire and are chosen by the publisher, so an unbounded label would
let any client mint time series at will — and unlike a log line, a series
persists for the whole retention period. Only `-metrics-tracks` names keep their
own `track` label; everything else reports as `other`. Subgroup IDs above 3 fold
into `3+`, keeping the low IDs separate because that is where a layered
publisher puts the base layer, whose loss is what actually breaks the picture.

Two readings worth knowing:

- `objects_dropped_total` rising on `subgroup="0"` is the picture breaking; on
  higher subgroups it may be a publisher's disposable enhancement layer being
  shed as designed.
- `fetch_objects_served_total` flat against a rising `fetches_served_total`
  means late joiners are asking for ranges the cache no longer holds.

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

The relay handles `SIGINT` and `SIGTERM`. On receipt it sends `GOAWAY` (§10.4) to
all active sessions with a 5-second grace period, then force-closes any that have
not drained — and it does not exit until that drain finishes, so peers actually
receive the GOAWAY rather than losing the connection under them.

The health endpoint, if enabled, comes down at the *start* of that sequence
rather than the end. Otherwise the relay would keep reporting itself healthy for
the whole drain, and a load balancer would keep sending it connections it is
about to refuse.

## Interop testing

This directory ships a `Dockerfile` and `entrypoint-relay.sh` that package the
relay for the [moq-interop-runner](https://github.com/englishm/moq-interop-runner)
framework. The entrypoint maps the runner's `MOQT_*` environment variables onto
the relay's flags:

| Env | Default | Maps to |
|-----|---------|---------|
| `MOQT_PORT` | `4443` | `-addr 0.0.0.0:$MOQT_PORT` |
| `MOQT_CERT` / `MOQT_KEY` | `/certs/cert.pem`, `/certs/priv.key` | `-cert` / `-key` |
| `MOQT_TRANSPORT` | `webtransport` | Accepted and logged only — both mappings are always served, so it selects nothing. An unknown value is still rejected. |
| `MOQT_WEBTRANSPORT_PATH` | `/` | `-webtransport-path` |

### Run the suite locally

From the repository root, against a third-party test client (defaults to
`moq-dev-rs`, draft-18), over both transports:

```sh
make interop-loopback            # our client -> our relay, both transports (CI gate)
make interop                     # raw QUIC + WebTransport vs a third-party client
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

`make interop` is **not** run by CI: every published third-party client still
speaks draft-18, while this relay advertises only the `moqt-20` ALPN, so the
handshake cannot complete and every case fails with "failed to connect to
server". The CI job is kept but gated to manual `workflow_dispatch`; drop the
`if:` in [`ci.yml`](../../.github/workflows/ci.yml) once those images move to
draft-20. `make interop-loopback` is what gates PRs meanwhile — same six cases
against our own relay image over both `moqt://` and `https://`, so the container's
WebTransport path stays covered.

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
