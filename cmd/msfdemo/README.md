# msfdemo

A minimal end-to-end demo of the MOQT Streaming Format
(draft-ietf-moq-msf-01) layered on top of LOC
(draft-ietf-moq-loc-04) and MoQ Transport
(draft-ietf-moq-transport-19).

One binary, two modes:

- `msfdemo publish` — emits an MSF catalog declaring one LOC-packaged
  `video` track, then pushes a synthetic LOC video frame every second.
- `msfdemo subscribe` — joins the broadcast, decodes the catalog,
  subscribes to the video track, and prints every frame's LOC
  metadata (timestamp, timescale) and payload size.

Payloads are filler bytes, not real codec data — the point is to
exercise the catalog, LOC object packaging, and the relay's
SUBSCRIBE / FETCH semantics together. For a working media player you
would feed the per-object payload into a WebCodecs (or equivalent)
decoder per the codec declared in the catalog.

## Usage

```
msfdemo [-addr host:port] publish | subscribe
```

| Flag | Default | Description |
|------|---------|-------------|
| `-addr` | `localhost:4433` | Relay address to dial |

Connections use `InsecureSkipVerify: true` so they work against the
relay's ephemeral self-signed cert. `SIGINT` / `SIGTERM` triggers a
clean shutdown: the publisher sends `PUBLISH_DONE` on both tracks; the
subscriber closes its request streams. A second signal force-exits.

## Tracks

Both tracks live under the namespace `moq-example/msf`:

| Track name | Packaging      | Contents                                   |
|------------|----------------|--------------------------------------------|
| `catalog`  | (MSF catalog)  | One JSON object describing the broadcast.  |
| `video`    | `loc`          | One LOC object per tick, 1 Hz, opaque payload. |

The catalog object is emitted once when the publisher starts and is
not re-published — §5 of the MSF draft says a catalog object SHOULD
be published only when track availability changes.

## Publisher

```
msfdemo publish              relay
     │
     │  SETUP                       │
     │ ───────────────────────────► │
     │                              │
     │  PUBLISH (moq-example/msf, catalog)
     │ ───────────────────────────► │
     │  REQUEST_OK                  │
     │ ◄─────────────────────────── │
     │  SUBGROUP_HEADER {group=0}   │
     │  OBJECT (catalog JSON), FIN  │
     │ ───────────────────────────► │  cached at entry.Cache
     │                              │
     │  PUBLISH (moq-example/msf, video)
     │ ───────────────────────────► │
     │  REQUEST_OK                  │
     │ ◄─────────────────────────── │
     │                              │
     ├─ tick (1s) ──────────────────│
     │  SUBGROUP_HEADER {group=N, Properties=1}
     │  OBJECT {                    │
     │    Properties: LOC KV pairs  │       Timestamp = t.UnixMilli()
     │                              │       Timescale = 1000
     │    Payload: "synthetic-frame-N"
     │  }                           │
     │  FIN                         │
     │ ───────────────────────────► │── fanout ──► subgroup streams
     │                              │── cache ──► replay buffer
     │  …                           │
     │                              │
     │  SIGINT / SIGTERM            │
     │  PUBLISH_DONE  (catalog)     │
     │ ───────────────────────────► │
     │  PUBLISH_DONE  (video)       │
     │ ───────────────────────────► │
```

Group IDs come from
[`msf.GroupSequencer`](../../pkg/moqt/msf/groupid.go), which seeds
from `time.Now().UnixMilli()` per §6.1 of the MSF draft so a
republish after restart cannot reuse a Group ID.

Per-object LOC properties are encoded with
[`loc.Object.Encode`](../../pkg/moqt/loc/object.go), which returns
the bytes to drop into `message.SubgroupObject.Properties` and
`.Payload`. The Subgroup header carries `Properties: true` so the
relay knows the per-object Properties block is present (§11.4.2).

## Subscriber

The subscriber is **catalog-driven**: it subscribes to the catalog
track first, parses the cached catalog object, picks a track based on
its declared properties, and only then subscribes to media. This
mirrors how a real MSF player would discover tracks rather than
hardcoding names.

Flow:

1. **SUBSCRIBE** on the catalog track with `FilterLargestObject` —
   delivers any future catalog (delta) updates.
2. **Relative Joining FETCH** with `JoiningStart=0` on the catalog
   subscription — backfills the cached catalog object the publisher
   emitted at broadcast start. The relay's `handleSubscribe` does not
   replay cached objects on a fresh SUBSCRIBE (the draft permits both
   patterns; §5.1.3 documents this one), so the cached catalog
   arrives via the FETCH stream.
3. **Wait for the first catalog object**, then parse it with
   `msf.Catalog.Validate` and pick the first track whose `packaging`
   is `loc` and `role` is `video` (see `selectVideoTrack`). The
   track's `Namespace` field is honoured if present, otherwise the
   subscriber inherits the catalog track's namespace per §5.1.10.
4. **SUBSCRIBE** on the discovered track with `FilterLargestObject` —
   delivers every video frame that arrives after the subscription is
   established. The demo skips a video-track Joining FETCH because
   the publisher emits a frame every second; the next live tick lands
   almost immediately.

```
msfdemo subscribe              relay
     │
     │  SETUP                       │
     │ ───────────────────────────► │
     │                              │
     │  SUBSCRIBE catalog (filter=LargestObject)
     │ ───────────────────────────► │
     │  SUBSCRIBE_OK {alias_c, LARGEST_OBJECT={0,0}}
     │ ◄─────────────────────────── │
     │  FETCH (RelativeJoining, JoiningStart=0,
     │         JoiningRequestID = SUBSCRIBE.RequestID)
     │ ───────────────────────────► │
     │  FETCH_OK                    │
     │ ◄─────────────────────────── │
     │  FETCH_HEADER stream         │
     │  OBJECT (catalog JSON), FIN  │
     │ ◄─────────────────────────── │
     │                              │
     ├─ catalog parsed, pick track ─│
     │  (selectVideoTrack: first track with packaging=loc, role=video)
     │                              │
     │  SUBSCRIBE <track-name>       (filter=LargestObject)
     │ ───────────────────────────► │
     │  SUBSCRIBE_OK {alias_v, …}   │
     │ ◄─────────────────────────── │
     │                              │
     ├─ live (each publisher tick) ─│
     │  SUBGROUP_HEADER {alias_v, group=N+1, Properties=1}
     │  OBJECT {Properties: …, Payload: …}, FIN
     │ ◄─────────────────────────── │
```

On each received catalog object the subscriber runs
[`msf.Catalog.Validate`](../../pkg/moqt/msf/validate.go) and logs the
declared tracks. On each video object it calls
[`loc.Decode`](../../pkg/moqt/loc/object.go) to recover the LOC
properties and log them alongside the (group, object) location.

The accept loop dispatches by stream type and TrackAlias:

| Stream type                     | Routed to              |
|---------------------------------|------------------------|
| `*IncomingSubgroupStream` (catalog alias) | live catalog updates |
| `*IncomingSubgroupStream` (video alias)   | live video frames    |
| `*IncomingFetchStream`          | catalog backfill from the Joining FETCH |

## Quick start

In three terminals:

```sh
# 1. Start the relay (ephemeral self-signed cert, port 4433)
go run ./cmd/relay

# 2. Publish the catalog + a synthetic video frame every second
go run ./cmd/msfdemo publish

# 3. Subscribe — prints the cached catalog, then a video frame each tick
go run ./cmd/msfdemo subscribe
```

A late-joining subscriber sees the catalog immediately (via the
Joining FETCH), then every subsequent video frame on the live
subscription.

## What this demo intentionally skips

- **Real codec data.** Payloads are ASCII strings, not encoded
  frames. A real publisher feeds `EncodedVideoChunk.copyTo` output
  (per WebCodecs) into `loc.Object.Payload`.
- **Multiple tracks / ABR.** The catalog declares one video track
  with no alternates. The MSF library supports simulcast/SVC catalogs
  (`altGroup`, `depends`, `temporalId`, `spatialId`) but the demo
  doesn't exercise them.
- **Delta updates.** The publisher emits a single independent catalog
  at startup and never updates it. The MSF library supports
  add/remove/clone deltas (`msf.Apply`) but the demo doesn't use
  them.
- **End-to-end encryption.** LOC §3 and MSF §4.3 are explicit TODOs
  in the drafts; this implementation defers them.
