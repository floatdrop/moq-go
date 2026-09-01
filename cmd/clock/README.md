# clock

A minimal MOQT (draft-ietf-moq-transport-20) publish/subscribe demo.

One binary, two modes:

- `clock publish` — emits the current wall-clock time once per second on the
  `moq-example/clock` track.
- `clock subscribe` — connects, retrieves the latest cached time on a fill
  fetch stream, and prints every live tick that follows.

The publisher and subscriber both speak native QUIC against a relay (see
[`cmd/relay`](../relay/)). Together they exercise the §5.1.6 "join an ongoing
track" pattern end-to-end: the live SUBSCRIBE delivers future objects while a
fill fetch stream (§5.1.3) backfills cached state, so the subscriber sees the
current time immediately on connect rather than waiting for the next tick.

## Usage

```
clock [-addr host:port] publish | subscribe
```

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `localhost:4433` | Relay address to dial |

Connections use `InsecureSkipVerify: true`, which matches the relay's default
ephemeral self-signed cert. `SIGINT` / `SIGTERM` triggers a clean shutdown:
the publisher sends `PUBLISH_DONE`; the subscriber closes its request stream.
A second signal force-exits.

## Publisher

Each second the publisher opens a fresh §11.4.2 SUBGROUP_HEADER stream, writes
a single object whose payload is the current time as an RFC 3339 string, and
closes the stream. Group ID advances by one per tick; each subgroup holds
exactly one object at `ObjectID=0`.

```
clock publish              relay                       (subscribers via relay)
     │
     │  SETUP                       │
     │ ───────────────────────────► │
     │                              │
     │  PUBLISH (moq-example/clock) │
     │ ───────────────────────────► │
     │  REQUEST_OK                  │
     │ ◄─────────────────────────── │
     │                              │
     ├─ tick (1s) ──────────────────│
     │  SUBGROUP_HEADER {group=N}   │
     │ ───────────────────────────► │── fanout ──► subgroup streams
     │  OBJECT {id=0, payload="2026-…"}            to every active subscriber
     │ ───────────────────────────► │── cache ──► entry.Cache (replay buffer)
     │  FIN                         │
     │ ───────────────────────────► │
     │                              │
     ├─ tick (1s) ──────────────────│
     │  SUBGROUP_HEADER {group=N+1} │
     │ ───────────────────────────► │
     │  OBJECT {id=0, payload="2026-…"}
     │ ───────────────────────────► │
     │  FIN                         │
     │ ───────────────────────────► │
     │  …                           │
     │                              │
     │  SIGINT / SIGTERM            │
     │  PUBLISH_DONE                │
     │ ───────────────────────────► │
```

## Subscriber

The subscriber issues a single request carrying both halves of the pattern:

1. A **Next Object** Location Filter — delivers every object *after* the
   relay's current LARGEST_OBJECT (i.e. all future ticks).
2. **FILL_PARAMETERS** whose Location filter is `StartGroup=1` — fills the
   current group from its start on a fill fetch stream (§5.1.3), contiguous
   with where the subscription itself begins.

draft-20 replaced draft-19's Relative Joining FETCH with this; the fill arrives
on a unidirectional stream whose FETCH_HEADER carries the SUBSCRIBE's Request
ID.

```
clock subscribe              relay
     │
     │  SETUP                       │
     │ ───────────────────────────► │
     │                              │
     │  SUBSCRIBE (LOCATION_FILTER=NextObject,
     │             FILL_PARAMETERS{LOCATION_FILTER StartGroup=1})
     │ ───────────────────────────► │
     │  SUBSCRIBE_OK {alias, LARGEST_OBJECT={G,0}}
     │ ◄─────────────────────────── │   (G is the latest tick the relay cached)
     │                              │
     │  fill fetch stream           │
     │  FETCH_HEADER{RequestID = SUBSCRIBE.RequestID}
     │ ◄─────────────────────────── │   1 cached object at {G, 0} — current time
     │  FIN                         │
     │ ◄─────────────────────────── │
     │                              │
     ├─ live ───────────────────────│
     │  SUBGROUP_HEADER {group=G+1} │
     │ ◄─────────────────────────── │
     │  OBJECT, FIN                 │
     │ ◄─────────────────────────── │
     │  SUBGROUP_HEADER {group=G+2} │
     │ ◄─────────────────────────── │
     │  …                           │
```

The subscriber tracks the highest `(group, object)` it has printed and ignores
any earlier object — this filters the harmless overlap between the fill
(which may include an object the live stream is about to deliver) and the
subscription itself (which may race ahead of the fill).

## Quick start

In three terminals:

```sh
# 1. Start the relay (ephemeral self-signed cert, port 4433)
go run ./cmd/relay

# 2. Publish the current time
go run ./cmd/clock publish

# 3. Subscribe — prints the current cached time, then a new tick every second
go run ./cmd/clock subscribe
```

A subscriber that joins mid-stream sees the most recent cached tick
immediately (via the fill fetch stream) followed by every subsequent tick on
the live subscription.
