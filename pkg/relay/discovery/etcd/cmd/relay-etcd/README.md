# relay-etcd

A MOQT relay whose cross-instance discovery is backed by an
[etcd](https://etcd.io) v3 cluster, so several relays sharing one cluster route
across each other. Each relay advertises its local tracks and namespaces into
etcd and follows peers' advertisements on demand.

It is a separate binary from `cmd/relay`, and lives in its own Go module
(`pkg/relay/discovery/etcd`), because the etcd client pulls in a large
dependency tree (gRPC, bbolt, zap, protobuf) that the core `moq-go` module
deliberately excludes. Only operators who opt into etcd-backed discovery pay for
that weight. Run it from inside the module:

```sh
cd pkg/relay/discovery/etcd
go run ./cmd/relay-etcd [flags]
```

## Identifying a build

The first line the relay logs identifies the binary (wrapped here; it is one
line):

```
time=2026-08-07T08:20:30.114Z level=INFO msg="relay-etcd build"
  version=v0.0.0-20260807082028-602bd1c1432c
  commit=602bd1c1432cb789e312aa7c86b75a0657c8a053
  commit_time=2026-08-07T08:20:28Z dirty=false
```

It comes from the metadata the Go toolchain stamps at link time, so there is
nothing to pass at build time. Three things to know when reading it:

- **The commit fields need a VCS checkout on disk.** `go build` and local-path
  `go install ./cmd/relay-etcd` have one. `go install <pkg>@<version>` builds
  from the module cache, which has no `.git`, so it reports the real `version`
  with `commit` and `commit_time` `unknown`.
- **`go run` omits the commit fields by default**, reporting `version=(devel)`.
  Pass `-buildvcs=true` to stamp them anyway; `-buildvcs=false` always omits
  them.
- **`commit_time` is when the commit was made**, not when the binary was built;
  the toolchain records the former and has no stamp for the latter. `dirty=true`
  means the tree had uncommitted changes at build time, in which case `version`
  carries a `+dirty` suffix as well.

## Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `0.0.0.0:4433` | UDP address to listen on; serves raw QUIC and WebTransport both. |
| `-cert` | — | PEM certificate file. If omitted, an ephemeral self-signed dev cert is generated. |
| `-key` | — | PEM private key file. Required when `-cert` is set. |
| `-webtransport-path` | `/moq` | HTTP/3 path browsers use for the WebTransport CONNECT. Raw QUIC ignores it. |
| `-etcd-endpoints` | `127.0.0.1:2379` | Comma-separated etcd client endpoints. |
| `-etcd-prefix` | `/moqt/discovery/` | Root key prefix scoping all of this relay's etcd data. |
| `-etcd-lease-ttl` | `15s` | Lease TTL bounding how long this relay's advertisements survive after it stops heartbeating. |
| `-relay-addr` | — | Address peers use to dial this relay, advertised in etcd. Empty = single-instance (not reachable by peers). Must be directly dialable — never a load-balancer address, since it is also the self-exclusion key that stops a relay dialing its own advertisements. |
| `-health-addr` | — | TCP address for the HTTP health endpoint. Empty (the default) disables it. |
| `-health-path` | `/healthz` | Path on `-health-addr` that answers `200 OK`; anything else there gets `404`. Ignored unless `-health-addr` is set. |
| `-metrics` | `true` | Serve Prometheus metrics at `<health-path>/metrics`. Requires `-health-addr`. |
| `-metrics-track-names` | `catalog,video,audio` | Track names that keep their own `track` label. Every other name is counted as `track="other"`, so a publisher cannot choose this process's metric cardinality. |

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

The mesh is two `relay-etcd` processes sharing one etcd cluster under the same
`-etcd-prefix`. Each listens on its own `-addr` and advertises itself under a
distinct `-relay-addr` that its peers can dial.

**1. Start etcd** (any single node works; Docker is the quickest):

```sh
docker run --rm -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.15 \
  etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
```

**2. Start relay A and relay B** (each in its own terminal, from
`pkg/relay/discovery/etcd`):

```sh
go run ./cmd/relay-etcd -addr 0.0.0.0:4433 -relay-addr localhost:4433
```

```sh
go run ./cmd/relay-etcd -addr 0.0.0.0:4434 -relay-addr localhost:4434
```

Both default to `-etcd-endpoints 127.0.0.1:2379` and
`-etcd-prefix /moqt/discovery/`, so they share one discovery fabric.

**3. What happens on the wire.** When a publisher connected to relay B
advertises a namespace (`PUBLISH_NAMESPACE`), B writes that advertisement to
etcd keyed by its `-relay-addr`. A subscriber connected to relay A that
`SUBSCRIBE`s a track under that namespace with no local publisher makes A read
the advertisement back from etcd, dial `localhost:4433` (B's advertised
address), and subscribe upstream — objects then flow publisher → B → A →
subscriber. A `SUBSCRIBE_NAMESPACE` on A is likewise seeded with namespaces
already advertised across the mesh and then followed live.

Cross-relay routing keys on **namespace advertisements**: a publisher must
`PUBLISH_NAMESPACE` for its tracks to be discoverable across the mesh (a bare
`PUBLISH` registers the track only on its local relay). The `clock` and
`msfdemo` demo clients publish a track without advertising its namespace, so
they exercise a single relay rather than the mesh. For a runnable end-to-end
example of the full cross-relay path — two relays, separate etcd-backed stores,
publisher on one and subscriber on the other — see `TestCrossRelayEtcd_OnDemandSubscribe`
in [`../../cross_relay_test.go`](../../cross_relay_test.go).

## Scoping a shared cluster

Every key this relay reads, writes, or watches is rooted at `-etcd-prefix`
(default `/moqt/discovery/`). A single etcd cluster can therefore host unrelated
data, or several independent relay meshes, without collisions — give each mesh
its own prefix:

```sh
# mesh "east": only these relays see each other
go run ./cmd/relay-etcd -etcd-prefix /moqt/east/ -relay-addr localhost:4433
# mesh "west": a separate discovery fabric on the same cluster
go run ./cmd/relay-etcd -etcd-prefix /moqt/west/ -relay-addr localhost:5433
```

## Liveness

Each relay's advertisements are attached to a single etcd lease that it renews
in the background. If the process dies (or is partitioned longer than
`-etcd-lease-ttl`), etcd expires the lease and drops all of that relay's
advertisements, so peers stop routing to an instance that can no longer serve. A
graceful shutdown revokes the lease so the advertisements disappear at once
rather than lingering for the rest of the TTL.

On a graceful shutdown (`SIGINT` / `SIGTERM`) this happens **before** the relay
stops accepting and before it sends `GOAWAY`, which is the point: peers stop
resolving this instance while it is still draining, instead of discovering it and
dialing a listener that has already closed. See `DiscoveryStore.Withdraw`.

## Health endpoint

Off by default. Setting `-health-addr` opts into a plain HTTP-over-**TCP**
endpoint on its own port, answering `200 OK` with the body `ok` plus a trailing
newline at `-health-path`:

```sh
go run ./cmd/relay-etcd -health-addr 127.0.0.1:9000 -health-path /ready
curl -i http://localhost:9000/ready
```

```
HTTP/1.1 200 OK
Date: Fri, 07 Aug 2026 09:10:39 GMT
Content-Length: 3
Content-Type: text/plain; charset=utf-8

ok
```

The body is there so a human running `curl` sees a confirmation rather than
nothing; probes should key on the status code, not on the body text.

It is separate from `-addr` because that port is UDP-only — an orchestrator or
TCP load-balancer probe has nothing to talk to there.

The check is **process liveness**, not readiness. It goes up once the MOQT
listener is bound, and on a `SIGINT`/`SIGTERM` shutdown it comes down at the
*start* of the shutdown, before the GOAWAY drain — so a load balancer stops
steering new connections here while the relay is still draining. That ordering
holds for signal-initiated shutdown only: if the relay stops because its listener
failed, the drain has already run by the time the health port closes, and probes
keep succeeding for up to `GoawayTimeout` after the relay stopped accepting.

It says nothing about whether etcd is reachable — a relay partitioned from etcd
still serves its local sessions and still reports healthy.

`-health-path` must begin with `/`; the relay logs and exits otherwise, rather
than starting up and 404ing every probe. The comparison is exact, so a probe must
request the path as given (a trailing slash is significant).

The port is unauthenticated, which is the other reason it is opt-in: when you do
enable it, prefer an internal interface (`-health-addr 127.0.0.1:8080`) over
`0.0.0.0`.

## Metrics

Prometheus exposition rides the same port, one level below `-health-path`:

```sh
go run ./cmd/relay-etcd -health-addr 127.0.0.1:9000
curl -s http://localhost:9000/healthz/metrics
```

It is on that port because the decision is the same one: both are plain HTTP
over TCP, both are unauthenticated, and an operator who exposed one has already
chosen for the other. `-metrics=false` turns it off and restores the `404`.

The scrape carries the standard Go runtime and process collectors, the etcd
client's own gRPC instrumentation (so a relay partitioned from etcd is visible
here even though the health check still reports `200`), and:

| Metric | Type | Labels | What it answers |
|---|---|---|---|
| `moqt_relay_build_info` | gauge | `version`, `commit`, `commit_time`, `dirty` | Always 1. Which build — so a change in any series below can be attributed to a rollout. |
| `moqt_relay_sessions` | gauge | `leg` | Live sessions. `leg="upstream"` is the cross-relay hop. |
| `moqt_relay_subscriptions` | gauge | `track`, `leg` | Subscriptions being fanned out. |
| `moqt_relay_objects_received_total` | counter | `track`, `leg`, `subgroup` | Objects the relay took responsibility for, after §2.1 duplicate suppression. |
| `moqt_relay_objects_forwarded_total` | counter | `track`, `leg`, `subgroup` | Objects enqueued for a subscriber, once **per subscriber**. |
| `moqt_relay_objects_dropped_total` | counter | `track`, `leg`, `subgroup` | Objects shed because a subscriber's send queue was full (§8). |
| `moqt_relay_subgroup_stream_resets_total` | counter | `track`, `leg`, `subgroup`, `cause` | Subgroup streams abandoned mid-group, by cause. |
| `moqt_relay_subscription_resets_total` | counter | `track`, `leg`, `cause` | Subscriptions terminated for falling behind (§3.3.4). |
| `moqt_relay_fetches_served_total` | counter | `track`, `leg` | FETCHes answered from the object cache. |
| `moqt_relay_fetch_objects_served_total` | counter | `track`, `leg` | Objects returned across those FETCHes. |
| `moqt_relay_upstream_dial_failures_total` | counter | — | Failed relay-to-relay dials to a peer advertised in etcd. |
| `moqt_relay_namespace_lookups_total` | counter | `result` | Discovery lookups on the cross-relay path; `result="empty"` means nobody advertised the namespace. |

### Reading a mesh

`leg` records **who dialled whom**, because that is the only thing a relay knows
for certain about a peer. A peer relay that dials *in* is `leg="local"`, exactly
like a browser — the MOQT implementation string a peer volunteers is not
something to build an operator-facing label on.

That is why a hop is read from both ends. The consuming relay reports it as
`leg="upstream"`; the producing relay reports the same hop inside its
`leg="local"` traffic. Objects that leave one and never arrive at the other went
missing in between:

```promql
# cross-relay ingress, per instance
sum by (instance, track) (rate(moqt_relay_objects_received_total{leg="upstream"}[1m]))
```

### When the picture breaks up between keyframes

That symptom is a group losing objects after its keyframe, so start with the two
counters that mean exactly that:

```promql
# subgroup streams abandoned mid-group, by cause
sum by (cause, track, subgroup) (rate(moqt_relay_subgroup_stream_resets_total[1m]))

# objects shed under send-queue pressure
sum by (track, subgroup) (rate(moqt_relay_objects_dropped_total[1m]))
```

`subgroup` is the discriminator worth reading first. A publisher striping
temporal layers across subgroups puts the base layer in subgroup 0 and the
disposable enhancement layers above it, so drops on `subgroup="1"` may be the
design working while drops on `subgroup="0"` are the picture breaking.

`cause` then says which mechanism fired: `delivery_timeout` is §8 giving up on a
stream, `too_far_behind` is a subscriber losing the whole subscription, `gap` is
a §11.4.3 reopen (correct behaviour, but it is the *consequence* of an earlier
drop and the subscriber still sees the hole), and `inbound_reset` means the loss
happened upstream of this relay — chase it on the instance one hop closer to the
publisher.

### Cardinality

Both `track` and `subgroup` come off the wire, chosen by the publisher, so
neither reaches a label unfiltered. Track names outside `-metrics-track-names`
are counted as `track="other"`; Subgroup IDs of 3 and above are counted as
`subgroup="3+"`. The peer address in a failed upstream dial is deliberately not
a label at all — it is in the `debug` log instead.

## Security

The TLS and cross-relay dial paths here are **development-grade**: with no
`-cert`/`-key` an ephemeral self-signed certificate is generated, and relays
dial each other without verifying peer certificates. A production deployment
should supply real certificates and a verifying dial path.
