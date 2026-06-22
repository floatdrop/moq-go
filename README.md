# moq

[![CI](https://github.com/floatdrop/moq-go/actions/workflows/ci.yml/badge.svg?branch=draft-18)](https://github.com/floatdrop/moq-go/actions/workflows/ci.yml?query=branch%3Adraft-18)
[![Go Reference](https://pkg.go.dev/badge/github.com/floatdrop/moq-go.svg)](https://pkg.go.dev/github.com/floatdrop/moq-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/floatdrop/moq-go)](https://goreportcard.com/report/github.com/floatdrop/moq-go)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)

A Go implementation of the **Media over QUIC** IETF drafts: a
transport-agnostic session library, a single-instance reference relay, media
packaging libraries, and demo publisher/subscriber CLIs.

- [Media over QUIC Transport (MoQT)](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) — `draft-ietf-moq-transport-18`
- [Low Overhead Media Container (LOC)](https://datatracker.ietf.org/doc/draft-ietf-moq-loc/) — `draft-ietf-moq-loc-02`
- [MoQ Streaming Format (MSF)](https://datatracker.ietf.org/doc/draft-ietf-moq-msf/) — `draft-ietf-moq-msf-01`

This is library + reference-relay code, not a media player. Payloads are opaque
to every layer — applications plug their own codec stack in at the LOC boundary.

> **Status:** tracks moving IETF drafts. The wire format follows the draft
> versions above and the API is pre-1.0 — both change as the specs evolve.

## Install

```sh
go get github.com/floatdrop/moq-go
```

Requires Go 1.26 or newer.

## Quick start

Run the stack locally, each command in its own terminal:

```sh
go run ./cmd/relay              # ephemeral self-signed cert on :4433
go run ./cmd/msfdemo publish    # MSF catalog + LOC video frames
go run ./cmd/msfdemo subscribe  # discovers the video track from the catalog
```

For the simpler raw-MOQT case (no LOC/MSF), swap `msfdemo` for `clock`.

## Using the library

The mental model:

- A **`Session`** is one MOQT connection after the SETUP handshake. You get one
  from `session.Client` or `session.Server` over a transport `Conn`.
- A **publisher** opens a `PUBLISH` request stream, then pushes objects on
  **subgroup uni-streams**.
- A **subscriber** opens a `SUBSCRIBE` request stream, then reads objects via
  `Session.AcceptDataStream`.
- A **track** is named by a `(Namespace, Name)` pair; a per-session **Track
  Alias** is the compact integer that data streams carry.

A minimal publisher looks like this:

```go
sess, err := session.Client(ctx, quicconn.New(qconn),
	session.WithImplementation("my-app/0.1"))
if err != nil {
	return err
}
defer sess.Close(moqt.SessionNoError, "bye")

pub, err := sess.Publish(ctx, &message.Publish{
	Namespace: wire.Namespace("moq-example"),
	Name:      []byte("clock"),
})
if err != nil {
	return err
}
defer pub.Close()

// Publish assigned the Track Alias; the returned Publication carries it, so
// pub.OpenSubgroup fills it in for you. To manage aliases yourself, set
// Publish's TrackAlias (via sess.AllocOutboundTrackAlias) and use
// sess.OpenSubgroup directly.
sg, _ := pub.OpenSubgroup(ctx, message.SubgroupHeader{
	SubgroupIDMode: message.SubgroupIDImplicitZero,
	GroupID:        0,
})
// WriteObjectAt takes absolute Object IDs and computes the §11.4.2 delta
// encoding for you; WriteObject is the lower-level form that takes the delta.
_ = sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("hello")})
_ = sg.Close()
```

A subscriber reads objects off inbound data streams via `Session.AcceptDataStream`,
type-switching the result to `*IncomingSubgroupStream` / `*IncomingFetchStream`.
When you subscribe to several tracks on one session, `session.Demux` removes the
hand-rolled accept loop: register a handler per track by its Track Alias (and per
FETCH by Request ID), then call `Demux.Run`.

```go
demux := session.NewDemux()
demux.HandleTrack(sub.TrackAlias(), func(s *session.IncomingSubgroupStream) {
	for {
		obj, err := s.ReadDecoded() // absolute IDs; deltas resolved for you
		if err != nil {
			return // io.EOF on clean FIN
		}
		_ = obj
	}
})
go demux.Run(ctx, sess) // HandleTrack is safe to call after Run starts
```

### Examples

Worked, compile-checked examples for each part of the API live as Go example
functions — browse them on
[pkg.go.dev](https://pkg.go.dev/github.com/floatdrop/moq-go) or read the source in the
linked files:

| Topic | Example function(s) | Source |
|---|---|---|
| Open a session | `ExampleClient` | [session](pkg/moqt/session/example_test.go) |
| Publish a track | `ExampleSession_Publish` | " |
| Subscribe to a track | `ExampleSession_Subscribe` | " |
| Route many tracks' data streams | `ExampleDemux` | " |
| Joining / standalone FETCH | `ExampleSession_Fetch`, `ExampleSession_Fetch_standalone`, `ExampleIncomingFetchStream` | " |
| Update a live request | `ExampleSession_UpdateRequest` | " |
| End a publication | `Example_endingAPublication` | " |
| Stream exhaustion (PUBLISH_BLOCKED) | `ExampleSession_OpenPublish`, `ExampleSession_ReadPublishBlocked` | " |
| Announce / discover namespaces | `ExampleSession_PublishNamespace`, `ExampleSession_SubscribeNamespace` | " |
| Accept requests (server side) | `ExampleSession_AcceptRequest` | " |
| Graceful shutdown (GOAWAY) | `ExampleSession_SendGoaway`, `ExampleSession_OnGoaway` | " |
| Run / authorize the relay | `ExampleNew`, `ExampleNew_authorizer` | [relay](pkg/relay/example_test.go) |
| LOC media packaging | `ExampleObject_Encode` | [loc](pkg/moqt/loc/example_test.go) |
| MSF catalogs (build / parse / delta) | `ExampleBeginBroadcast`, `Example_subscribeCatalog`, `ExampleApply` | [msf](pkg/moqt/msf/example_test.go) |

The two demo commands — [`cmd/clock`](cmd/clock) and
[`cmd/msfdemo`](cmd/msfdemo) — are complete, runnable versions of these patterns
end to end; each has its own README with sequence diagrams.

## Repo layout

```
pkg/moqt/                 MOQT protocol implementation
├── wire/                 Wire-format primitives (varint, KV pairs, namespaces, framing)
├── message/              Typed control- and request-stream messages
├── track/                Full track name + canonical map keys
├── session/              SETUP handshake, control multiplexing, GOAWAY, alias mgmt
│   ├── quicconn/         Native-QUIC Conn adapter (quic-go)
│   ├── wtconn/           WebTransport Conn adapter (webtransport-go)
│   └── sessiontest/      In-process pipe-backed Conn for tests
├── loc/                  Low-Overhead Container per draft-ietf-moq-loc-02
├── msf/                  MOQT Streaming Format per draft-ietf-moq-msf-01
└── errors.go             Session / request / publish-done / stream-reset codes

pkg/relay/                Single-instance MOQT relay
├── cache/                Per-track object cache
└── discovery/            Cross-instance discovery interface + in-memory impl

cmd/
├── relay/                MOQT relay binary
├── interop-client/       moq-interop-runner test client (drives the session library)
├── clock/                Wall-clock publish/subscribe demo (raw MOQT)
└── msfdemo/              MSF catalog + LOC video frame demo

apps/
└── tlmst/                Wails3 desktop app (separate Go module, isolated deps)
```

The root module (`github.com/floatdrop/moq-go`) holds the protocol libraries and the
relay. `apps/tlmst` is a separate Go module so its desktop-app dependencies
(Wails3, CGO, WebKit) don't leak into the root build; it resolves the parent
module via a `replace` directive, so plain `go` commands work without a
workspace (there is no committed `go.work`).

## What's implemented

- **`wire`** — byte-level codec: MoQT leading-ones varints (§1.4.1, distinct
  from QUIC's RFC 9000 varints), length-prefixed bytes, KV pairs
  with delta-encoded types (§1.4.3), track namespaces (§2.4.1), reason phrases,
  and both an in-memory `Reader` and a streaming `Decoder` over the same
  control-frame interface.
- **`message`** — typed control, request-stream, and data-stream messages with
  parameter negotiation: SETUP, GOAWAY, SUBSCRIBE, PUBLISH (+ DONE/BLOCKED),
  FETCH (standalone, relative/absolute joining), TRACK_STATUS, REQUEST_UPDATE,
  the namespace messages, the §11 object framing (subgroup, fetch, datagram)
  with absence markers, subscription filters, GREASE handling, and a `Validate`
  hook the decoder runs on parse to reject structurally-malformed messages
  (FETCH End < Start, REQUEST_ERROR redirect consistency, object status/flags,
  …).
- **`session`** — the SETUP handshake with version negotiation, control
  multiplexing and request-ID allocation, Track-Alias management with §3.5
  collision detection, the request openers (`Publish`/`Subscribe`/`Fetch`/…)
  and the `AcceptRequest` responder, typed inbound data streams that resolve
  §11.4.2/§11.4.4 deltas to absolute IDs, GOAWAY, the §10.20 token cache, and
  pluggable transport via the `Conn` interface (`quicconn` + `wtconn` adapters).
- **`loc`** — `Object.Encode`/`Decode` producing the bytes that drop into a
  `SubgroupObject`: typed Timestamp/Timescale/VideoConfig/VideoFrameMarking/
  AudioLevel properties with `Extras` passthrough for unknown IDs, an RFC 6464
  audio-level codec, and AVC/HEVC NAL framing detection.
- **`msf`** — `Catalog`/`Track` JSON (independent and delta catalogs, with
  `Apply` replaying delta operations in document order), group-ID sequencing,
  the Media and Event Timeline record formats, the `BeginBroadcast` /
  `EndBroadcast*` workflow helpers, and `Catalog.Validate` enforcing the §5.1/
  §5.2 invariants.
- **`relay`** — accepts publisher and subscriber sessions, routes objects
  through a track registry with per-subscription live fanout under a §7.3
  latency-window slow-reader policy, serves FETCHes from a per-track 
  object cache and stitches the evicted part of a range from an upstream
  FETCH, issues on-demand upstream SUBSCRIBEs via a `DiscoveryStore`, forwards
  namespace interest, gates each request through an `Authorizer` hook, emits
  telemetry through a `Metrics` hook, and drains sessions with GOAWAY.
- **CLIs** — `cmd/relay` (with cert flags + self-signed fallback), `cmd/clock`
  (raw subgroup demo), and `cmd/msfdemo` (the LOC + MSF stack end to end).

## Limitations

This is a **single-instance** reference relay. The multi-instance work
(relay-to-relay sessions, an upstream pool, production `DiscoveryStore`
backends, cross-relay subscribe routing, loop detection, and GOAWAY cascading) 
is out of scope here. Known gaps in the current code, roughly ordered by how
load-bearing they are:

- **Multiple publishers per track** (§9.5) — the relay keeps exactly one
  upstream per track; the "subscribe to each matching publisher"
  fault-tolerance mode is not implemented (see the upstream-subscribe block in
  [`pkg/relay/handler_subscribe.go`](pkg/relay/handler_subscribe.go)).
- **Subscriber-priority scheduling** (§10.2.7) — `SUBSCRIBER_PRIORITY` is
  parsed and stored but currently advisory; subgroup streams expose no
  per-stream priority knob.
- **LOC encryption / SecureObjects** (LOC §3) and **Private Properties** —
  intentionally out of scope pending a chosen SecureObjects revision. Property
  IDs are draft-tentative (`PropAudioLevel = 0x0A` deviates from the draft's
  suggested `6` to avoid colliding with `PropTimestamp`).
- **MSF** — no timeline GZIP compression, content protection (§4.3), token
  authorization, or logs/analytics (each is a TODO or unspecified in the
  draft). There is no built-in ABR helper: the library surfaces every catalog
  field a selector needs (AltGroup, Width/Height, Bitrate, RenderGroup,
  Depends, TemporalID, SpatialID), but variant-selection policy is the
  application's job.

## Building and testing

```sh
go build ./...
go test ./...                          # full suite — hermetic, no fixtures or network
go test -race ./pkg/moqt/session/...   # race detector for goroutine/stream code
golangci-lint run                      # lint + format check (.golangci.yml)
```

`go test ./...` from the root does not include `apps/tlmst` (a separate module
with CGO/WebKit deps). For the benchmark suite and the `benchstat`
regression-comparison workflow, see
[`benchmarks/README.md`](benchmarks/README.md).

### Interoperability tests

Interop is tested in both directions against independent implementations:

- **Relay direction** — `make interop` runs a third-party MoQT test client
  (from the [moq-interop-runner](https://github.com/englishm/moq-interop-runner))
  against our relay over both transports; `make interop-matrix` runs several
  clients and prints a pass/skip/fail matrix.
- **Client direction** — `make interop-client` runs our own test client
  ([`cmd/interop-client`](cmd/interop-client)) against a relay (loopback by
  default; override `CLIENT_RELAY_IMAGE`/`CLIENT_RELAY_URL` for a third-party
  relay).

See [`cmd/relay/README.md`](cmd/relay/README.md) for the targets and options;
current results are tracked in [`STATUS.md`](STATUS.md).

CI runs on every push and pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)): `go build ./...`,
`go test ./...`, `go test -race ./...`, `golangci-lint run`, a `govulncheck`
scan, and the interop suite (`make interop` and `make interop-client`). The
interop run is not redundant with `go test`: the unit tests round-trip through
our own codec, so a wire-encoding regression (e.g. emitting QUIC varints instead
of the §1.4.1 leading-ones encoding) passes every unit test yet breaks interop —
only a run against an independent implementation catches it. The advisory
`make interop-matrix` is not gated, as it has known cross-implementation
divergences (see [`STATUS.md`](STATUS.md)).

## License

Licensed under either:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
