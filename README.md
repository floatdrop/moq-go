# moq-go

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
sg, _ := pub.OpenSubgroup(message.SubgroupHeader{
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
// sess here is the subscriber's session (from session.Client/Server).
sub, err := sess.Subscribe(ctx, &message.Subscribe{
	Namespace:  wire.Namespace("moq-example"),
	Name:       []byte("clock"),
	Parameters: message.Parameters{message.LargestObjectFilter()},
})
if err != nil {
	return err
}
defer sub.Close()

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
[pkg.go.dev](https://pkg.go.dev/github.com/floatdrop/moq-go) or read the source,
grouped here by the file they live in:

**[`session`](pkg/moqt/session/example_test.go)**

| Topic | Example function(s) |
|---|---|
| Open a session | `ExampleClient` |
| Publish a track | `ExampleSession_Publish` |
| Subscribe to a track | `ExampleSession_Subscribe` |
| Route many tracks' data streams | `ExampleDemux` |
| Route inbound requests (server side) | `ExampleRequestMux` |
| Joining / standalone FETCH | `ExampleSession_Fetch`, `ExampleSession_Fetch_standalone`, `ExampleIncomingFetchStream` |
| Update a live request | `ExampleSession_UpdateRequest` |
| End a publication | `Example_endingAPublication` |
| Stream exhaustion (PUBLISH_BLOCKED) | `ExampleSession_OpenPublish`, `ExampleSession_ReadPublishBlocked` |
| Announce / discover namespaces | `ExampleSession_PublishNamespace`, `ExampleSession_SubscribeNamespace` |
| Accept requests + reply (`Accept*` helpers) | `ExampleSession_AcceptRequest` |
| Graceful shutdown (GOAWAY) | `ExampleSession_SendGoaway`, `ExampleSession_OnGoaway` |

**[`relay`](pkg/relay/example_test.go)**

| Topic | Example function(s) |
|---|---|
| Run / authorize the relay | `ExampleNew`, `ExampleNew_authorizer` |

**[`loc`](pkg/moqt/loc/example_test.go)**

| Topic | Example function(s) |
|---|---|
| LOC media packaging | `ExampleObject_Encode` |

**[`msf`](pkg/moqt/msf/example_test.go)**

| Topic | Example function(s) |
|---|---|
| MSF catalogs (build / parse / delta) | `ExampleBeginBroadcast`, `Example_subscribeCatalog`, `ExampleApply` |

The two demo commands — [`cmd/clock`](cmd/clock) and
[`cmd/msfdemo`](cmd/msfdemo) — are complete, runnable versions of these patterns
end to end; each has its own README with sequence diagrams.

A per-feature breakdown of draft-18 completeness, the full list of what's
implemented per package, and known limitations live in
[`STATUS.md`](STATUS.md).

## Building and testing

```sh
go build ./...
go test ./...                          # full suite — hermetic, no fixtures or network
go test -race ./pkg/moqt/session/...   # race detector for goroutine/stream code
golangci-lint run                      # lint + format check (.golangci.yml)
```

For the benchmark suite and the `benchstat` regression-comparison workflow, see
[`benchmarks/README.md`](benchmarks/README.md).

### Interoperability tests

This implementation is registered (as `moq-go`) in the
[moq-interop-runner](https://github.com/englishm/moq-interop-runner), which
exercises it in both directions against independent draft-18 implementations.
See [`cmd/relay/README.md`](cmd/relay/README.md) for the local `make interop`
targets.

CI runs on every push and pull request
([`.github/workflows/ci.yml`](.github/workflows/ci.yml)): `go build ./...`,
`go test ./...`, `go test -race ./...`, `golangci-lint run`, a `govulncheck`
scan, and the interop suite. The interop run is not redundant with `go test`:
the unit tests round-trip through our own codec, so a wire-encoding regression
(e.g. emitting QUIC varints instead of the §1.4.1 leading-ones encoding) passes
every unit test yet breaks interop — only a run against an independent
implementation catches it.

## License

Licensed under either:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)
