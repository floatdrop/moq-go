# moq-go

<!-- Badges take a single branch and no globs, so this one names the current
     draft explicitly; update it on a draft bump. The CI push filter itself
     globs draft-*, so builds need no edit. -->
[![CI](https://github.com/floatdrop/moq-go/actions/workflows/ci.yml/badge.svg?branch=draft-19)](https://github.com/floatdrop/moq-go/actions/workflows/ci.yml?query=branch%3Adraft-19)
[![Go Reference](https://pkg.go.dev/badge/github.com/floatdrop/moq-go.svg)](https://pkg.go.dev/github.com/floatdrop/moq-go)
[![License](https://img.shields.io/badge/license-Apache--2.0%20OR%20MIT-blue.svg)](#license)

A Go implementation of the **Media over QUIC** IETF drafts: a
transport-agnostic session library, a reference relay that runs standalone or as
several instances routing across each other, media packaging libraries, and demo
publisher/subscriber CLIs.

- [Media over QUIC Transport (MoQT)](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) — `draft-ietf-moq-transport-19`
- [Low Overhead Media Container (LOC)](https://datatracker.ietf.org/doc/draft-ietf-moq-loc/) — `draft-ietf-moq-loc-04`
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

## Running a relay

### Transports and URLs

A client addresses a relay with a `moqt` URI (§3.1.1). Which transport that
resolves to depends on the scheme it is dereferenced as (§3.1.3):

| URL | transport | ALPN | on the wire |
|---|---|---|---|
| `moqt://host[:port]/path` | raw QUIC | `moqt-19` | UDP, default port 443 |
| `https://host[:port]/path` | WebTransport over HTTP/3 | `h3` | UDP, default port 443 |

`https://` is the §3.1.4 conversion of the same `moqt` URI — it names an HTTP
origin, **not** a TCP transport. Both mappings are QUIC, so both are UDP end to
end; there is no TCP phase and nothing "downgrades". Browsers need the
WebTransport form; native clients can use either. `pkg/moqt/uri` parses `moqt`
URIs and performs the conversion.

Every relay binary serves **both** mappings on one UDP port: the listener
advertises both ALPNs and decides per connection, so clients pick their transport
by URL scheme and there is no transport flag to set. `-webtransport-path` (default
`/moq`) only chooses where the CONNECT handler mounts. `relaynet.Listen` is the
library entry point; `relaynet.ListenQUIC` and `relaynet.ListenWebTransport`
remain for a single-transport listener.

### Graceful shutdown

Use `Relay.Run`, not `Start`, in anything driven by a signal:

```go
ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
defer stop()
if err := r.Run(ctx, 10*time.Second); err != nil { /* ... */ }
```

`Run` serves until ctx is cancelled, then withdraws from Discovery, stops
accepting, sends GOAWAY (§10.4), waits out `Config.GoawayTimeout`, and
force-closes — returning only once that drain is done. ctx is the shutdown
*trigger*: `Start` hands its context to every session handler, so a signal
context wired straight into it kills the sessions before their peers can be told
to migrate.

### Multiple instances

`pkg/relay/discovery` defines the `DiscoveryStore` that lets relays route across
each other; `MemoryStore` is in-process, and the `etcd` and `nats` submodules are
distributed backends with their own binaries (`relay-etcd`, `relay-nats`) and
their own `go.mod`, so the core module never pulls in those clients.

Deploying behind a load balancer: MOQT is UDP, so it needs **L4 UDP** load
balancing — a TCP-terminating HTTPS load balancer cannot carry it, whichever URL
scheme clients use. Sessions are long-lived and stateful, so route on QUIC
connection ID rather than the 5-tuple, or connection migration and NAT rebinding
will land packets on an instance holding no state for them. Clients can come in
over either transport through that load balancer while peer relays keep dialing
each other over raw QUIC directly — one port serves both, so the two planes need
no coordination. Keep `-relay-addr` pointing at a directly dialable address, never
the load balancer: it is the peer-facing address *and* the self-exclusion key that
stops a relay dialing its own advertisements. See the `relay-etcd` package doc for
the details.

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
| Serve a long-lived request stream (updates + follow-ups) | `ExampleRequestBroker` |
| End a publication | `Example_endingAPublication` |
| Stream exhaustion (PUBLISH_SKIPPED) | `ExampleSession_OpenPublish`, `ExampleTrackSubscription_ReadPublishSkipped` |
| Announce / discover namespaces | `ExampleSession_PublishNamespace`, `ExampleSession_SubscribeNamespace` |
| Accept requests + reply (`Accept*` helpers) | `ExampleSession_AcceptRequest` |
| Graceful shutdown (GOAWAY) | `ExampleSession_SendGoaway`, `ExampleSession_OnGoaway` |

**[`relay`](pkg/relay/example_test.go)**

| Topic | Example function(s) |
|---|---|
| Run the relay / graceful shutdown | `ExampleNew` |
| Authorize requests | `ExampleNew_authorizer` |
| Relay metrics | `ExampleMetrics` |

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

A per-feature breakdown of draft-19 completeness, the full list of what's
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

### Development environment (devenv + direnv)

The repo ships a [devenv](https://devenv.sh) config (`devenv.nix`) that pins the
Go toolchain (matching `go.mod`) plus `golangci-lint`, `golines`, `goimports`,
`gopls`, and `dlv` — so everyone builds against the same versions. It is
optional: a plain `go` install works fine. With [Nix](https://nixos.org)
installed:

```sh
# One-time: install devenv and direnv.
nix profile add nixpkgs#devenv nixpkgs#direnv
```

Then either enter the shell on demand:

```sh
devenv shell        # drops you into a shell with the full toolchain on PATH
devenv test         # sanity-check the toolchain wiring
```

…or let [direnv](https://direnv.net) load it automatically on `cd` (recommended).
Hook direnv into your shell once (see the
[direnv setup guide](https://direnv.net/docs/hook.html)), e.g. for zsh:

```sh
echo 'eval "$(direnv hook zsh)"' >> ~/.zshrc
```

then trust this repo's `.envrc` once:

```sh
direnv allow        # run from the repo root; re-run if .envrc changes
```

After that the environment activates whenever you enter the directory. Inside
the shell, convenience scripts wrap the canonical commands: `build`, `test`,
`test-race`, `lint`, `bench`, and `modernize`.

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
