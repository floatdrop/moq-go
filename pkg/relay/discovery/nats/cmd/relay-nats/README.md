# relay-nats

A MOQT relay whose cross-instance discovery is backed by a
[NATS](https://nats.io) JetStream Key/Value bucket, so several relays sharing one
NATS system route across each other. Each relay advertises its local tracks and
namespaces into the bucket and follows peers' advertisements on demand. It is the
NATS counterpart of [`relay-etcd`](../../../etcd/cmd/relay-etcd) — same
`discovery.DiscoveryStore` contract, different backend.

It is a separate binary from `cmd/relay`, and lives in its own Go module
(`pkg/relay/discovery/nats`), because the NATS client pulls in a dependency tree
the core `moq-go` module deliberately excludes. Only operators who opt into
NATS-backed discovery pay for that weight. Run it from inside the module:

```sh
cd pkg/relay/discovery/nats
go run ./cmd/relay-nats [flags]
```

## Requirements

The liveness mechanism relies on JetStream per-key TTL expiry markers
(`LimitMarkerTTL`), which require **nats-server 2.11 or newer** with JetStream
enabled (`nats-server -js`).

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `0.0.0.0:4433` | UDP address to listen on; serves raw QUIC and WebTransport both. |
| `-cert` | — | PEM certificate file. If omitted, an ephemeral self-signed dev cert is generated. |
| `-key` | — | PEM private key file. Required when `-cert` is set. |
| `-webtransport-path` | `/moq` | HTTP/3 path browsers use for the WebTransport CONNECT. Raw QUIC ignores it. |
| `-nats-url` | `nats://127.0.0.1:4222` | NATS server URL. |
| `-nats-bucket` | `moqt_discovery` | JetStream KV bucket scoping all of this relay's discovery data. |
| `-nats-ttl` | `15s` | Liveness TTL bounding how long this relay's advertisements survive after it stops heartbeating. |
| `-relay-addr` | — | Address peers use to dial this relay, advertised in NATS. Empty = single-instance (not reachable by peers). Must be directly dialable — never a load-balancer address, since it is also the self-exclusion key that stops a relay dialing its own advertisements. |

## Transports

One UDP port serves both MOQT transport mappings, so clients choose per
connection and nothing is configured deployment-wide:

| Client URL | Transport | ALPN |
|---|---|---|
| `moqt://host:port` | raw QUIC | `moqt-19` |
| `https://host:port/moq` | WebTransport over HTTP/3 | `h3` |

Both are QUIC over UDP — the `https` scheme names an HTTP origin, not a TCP
transport (§3.1.3 dereferences the URI, §3.1.4 derives the https form). Peer
relays always dial each other over raw QUIC, whatever clients use.

Behind a load balancer this needs **L4 UDP** forwarding; a TCP-terminating HTTPS
load balancer cannot carry MOQT under either scheme. Sessions are long-lived and
stateful, so route on QUIC connection ID rather than the 5-tuple, or connection
migration and NAT rebinding will land packets on an instance that holds no state
for them.

## Quick start: a two-relay mesh

The mesh is two `relay-nats` processes sharing one NATS system under the same
`-nats-bucket`. Each listens on its own `-addr` and advertises itself under a
distinct `-relay-addr` that its peers can dial.

**1. Start NATS with JetStream** (Docker is the quickest):

```sh
docker run --rm -p 4222:4222 nats:2.11 -js
```

**2. Start relay A and relay B** (each in its own terminal, from
`pkg/relay/discovery/nats`):

```sh
go run ./cmd/relay-nats -addr 0.0.0.0:4433 -relay-addr localhost:4433
```

```sh
go run ./cmd/relay-nats -addr 0.0.0.0:4434 -relay-addr localhost:4434
```

Both default to `-nats-url nats://127.0.0.1:4222` and
`-nats-bucket moqt_discovery`, so they share one discovery fabric.

**3. What happens on the wire.** When a publisher connected to relay B advertises
a namespace (`PUBLISH_NAMESPACE`), B writes that advertisement to the KV bucket
keyed by its `-relay-addr`. A subscriber connected to relay A that `SUBSCRIBE`s a
track under that namespace with no local publisher makes A read the advertisement
back from NATS, dial `localhost:4433` (B's advertised address), and subscribe
upstream — objects then flow publisher → B → A → subscriber. A
`SUBSCRIBE_NAMESPACE` on A is likewise seeded with namespaces already advertised
across the mesh and then followed live.

Cross-relay routing keys on **namespace advertisements**: a publisher must
`PUBLISH_NAMESPACE` for its tracks to be discoverable across the mesh (a bare
`PUBLISH` registers the track only on its local relay). The `clock` and `msfdemo`
demo clients publish a track without advertising its namespace, so they exercise
a single relay rather than the mesh.

## Scoping a shared system

Every key this relay reads, writes, or watches lives in `-nats-bucket` (default
`moqt_discovery`). One NATS system can therefore host several independent relay
meshes without collisions — give each mesh its own bucket:

```sh
# mesh "east": only these relays see each other
go run ./cmd/relay-nats -nats-bucket moqt_east -relay-addr localhost:4433
# mesh "west": a separate discovery fabric on the same NATS system
go run ./cmd/relay-nats -nats-bucket moqt_west -relay-addr localhost:5433
```

## Liveness

Each relay re-writes its own advertisements into the bucket every `-nats-ttl`/3
(the heartbeat), resetting the bucket TTL so live advertisements never expire.
The re-write is byte-identical, so watchers dedup it away rather than re-emitting
it. If the process dies (or is partitioned longer than `-nats-ttl`), the
heartbeat stops and JetStream expires the keys; because the bucket sets a limit
marker TTL, the expiry emits a delete marker the peers' watchers turn into an
"unpublish", so they stop routing to an instance that can no longer serve — the
same outcome as an expired etcd lease. A graceful shutdown deletes the
advertisements outright so they disappear at once rather than lingering for the
rest of the TTL.

On a graceful shutdown (`SIGINT` / `SIGTERM`) this happens **before** the relay
stops accepting and before it sends `GOAWAY`, which is the point: peers stop
resolving this instance while it is still draining, instead of discovering it and
dialing a listener that has already closed. See `DiscoveryStore.Withdraw`.

## Security

The TLS and cross-relay dial paths here are **development-grade**: with no
`-cert`/`-key` an ephemeral self-signed certificate is generated, and relays dial
each other without verifying peer certificates. A production deployment should
supply real certificates and a verifying dial path.
