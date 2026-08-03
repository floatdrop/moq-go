# clock

A minimal MOQT (draft-ietf-moq-transport-19) publish/subscribe demo.

One binary, two modes:

- `clock publish` — emits the current wall-clock time once per second on the
  `moq-example/clock` track.
- `clock subscribe` — connects, retrieves the latest cached time via a Joining
  FETCH, and prints every live tick that follows.

The publisher and subscriber both speak native QUIC against a relay (see
[`cmd/relay`](../relay/)). Together they exercise the §5.1.3 "join an ongoing
track" pattern end-to-end: the live SUBSCRIBE delivers future objects while the
Joining FETCH backfills cached state, so the subscriber sees the current time
immediately on connect rather than waiting for the next publisher tick.

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

The subscriber issues two requests on the same session:

1. **SUBSCRIBE** with `FilterLargestObject` — delivers every object *after*
   the relay's current LARGEST_OBJECT (i.e. all future ticks).
2. **Relative Joining FETCH** with `JoiningStart=0` — backfills the current
   group's cached objects, contiguous with where the SUBSCRIBE starts
   (§10.12.2.1).

```
clock subscribe              relay
     │
     │  SETUP                       │
     │ ───────────────────────────► │
     │                              │
     │  SUBSCRIBE (filter=LargestObject)
     │ ───────────────────────────► │
     │  SUBSCRIBE_OK {alias, LARGEST_OBJECT={G,0}}
     │ ◄─────────────────────────── │   (G is the latest tick the relay cached)
     │                              │
     │  FETCH (RelativeJoining, JoiningStart=0,
     │         JoiningRequestID = SUBSCRIBE.RequestID)
     │ ───────────────────────────► │
     │  FETCH_OK {EndLocation={G,1}}│
     │ ◄─────────────────────────── │
     │                              │
     │  FETCH_HEADER stream         │
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
any earlier object — this filters the harmless overlap between the Joining
FETCH (which may include an object the live stream is about to deliver) and
the live SUBSCRIBE (which may race ahead of the FETCH response).

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
immediately (via the Joining FETCH) followed by every subsequent tick on the
live subscription.
