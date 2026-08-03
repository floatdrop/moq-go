# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

A Go implementation of the Media over QUIC Transport (MoQT), LOC, and MSF IETF
drafts: a transport-agnostic session library, a single-instance relay, and demo
CLIs. `README.md` has an extensive, example-driven usage guide for the public
API — read it when you need to know *how to call* something; this file covers
*how the code is organized and how to work on it*.

## Commands

```sh
go build ./...                              # build everything
go test ./...                               # full suite (hermetic — no fixtures/network)
go test ./pkg/moqt/session/ -run TestName   # single test (or a -run regex)
go test -race ./pkg/moqt/session/...        # race detector — use for anything touching goroutines/streams
go test -run='^$' -bench=. -benchmem ./...  # benchmarks (wire codec + session/relay hot paths)
golangci-lint run                           # lint/format check (.golangci.yml)
```

The lint config's enabled formatters are `goimports` and `golines`, so format
with those (not plain `gofmt`).

The `modernize` linter is enabled, which covers `errorsastype` (rewrite
`errors.As(err, &t)` → `errors.AsType[*T](err)`). golangci-lint v2.12.2 bundles
a modernize snapshot that predates that analyzer, so until it catches up, run
the check standalone (no go.mod changes — pinning gopls as a tool would perturb
the transport deps):

```sh
go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest \
  -errorsastype ./...        # add -fix to apply
```

Run the stack locally (each in its own terminal):

```sh
go run ./cmd/relay              # self-signed cert on :4433
go run ./cmd/msfdemo publish    # or ./cmd/clock for the raw-MOQT (no LOC/MSF) demo
go run ./cmd/msfdemo subscribe
```

## Architecture

The packages form a strict bottom-up dependency stack — each layer only knows
about the ones below it, which is the fastest way to find where a change belongs:

```
wire      byte-level codec: varints, KV pairs, namespaces, frame reader/decoder
  └─ message   typed control + request-stream + data-stream messages (Append/Parse over wire)
       └─ session   one MOQT connection: SETUP, control mux, requests, data streams, aliases
            └─ relay   single-instance routing/caching/fanout across many sessions
```

`track`, `loc`, and `msf` are siblings used at the edges (`track` = `(Namespace,
Name)` keys; `loc`/`msf` = payload packaging the session treats as opaque bytes).

**The `Conn`/`Stream` interface is the central abstraction**
(`pkg/moqt/session/conn.go`). The session layer never imports a QUIC library — it
operates on `Conn` (open/accept bidi+uni streams, datagrams) and `Stream` (a bidi
QUIC stream). Three adapters implement it: `quicconn` (quic-go), `wtconn`
(WebTransport), and `sessiontest` (in-process pipe-backed conn used by nearly
every test). Transport behavior added to the interface must land in all three.

**Request streams are the core session pattern.** SUBSCRIBE, PUBLISH, FETCH,
TRACK_STATUS, and the namespace requests each open a bidi stream whose *first
message* is the request; the stream then stays open for the response and
follow-ups (REQUEST_UPDATE, PUBLISH_DONE, subgroup data). Outbound openers live
in `pkg/moqt/session` (`Publish`/`Subscribe`/`Fetch`/…); the inbound counterpart
is `AcceptRequest` → `Request.Reply` / `Request.RejectError`, which also enforces
§10.1 Request-ID parity/monotonicity and resolves AUTHORIZATION_TOKEN params.

**Data streams** carry objects, not control: outbound via `OpenSubgroup` +
`WriteObject` (§11.4.2 delta-encoded Object IDs), inbound via `AcceptDataStream`,
which returns a typed `IncomingSubgroupStream` / `IncomingFetchStream` whose
`ReadDecoded` resolves the deltas back to absolute IDs.

**The relay** (`pkg/relay`) wires many sessions together: a track registry routes
objects, a per-track FIFO ring cache with read-side TTL serves joining FETCHes, an `Authorizer`
hook gates each request once before state mutation, and a `DiscoveryStore`
triggers on-demand upstream SUBSCRIBE. `session_handler.go` is the per-session
dispatch hub.

## Reference Documents

If you need to reference the spec, fetch it from:

- [MOQT Transport](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/)
- [MOQT LOC](https://datatracker.ietf.org/doc/draft-ietf-moq-loc/)
- [MOQT MSF](https://datatracker.ietf.org/doc/draft-ietf-moq-msf/)

Only retrieve the sections relevant to the task at hand.

## Conventions

- **Spec-driven, with citations.** Comments cite draft sections (`§10.1`,
  `§11.4.2`) referencing the pinned draft versions
  (`draft-ietf-moq-transport-19`, `-loc-02`, `-msf-01`) — the source of truth
  when implementing or changing protocol behavior. See the Reference Documents
  section above for the datatracker links.
- **Message types** implement `Append(*wire.Writer)` + `Parse(*wire.Reader)`;
  request-stream messages additionally satisfy `message.WithRequestID`
  (`GetRequestID`/`SetRequestID`).
- **Error/reset codes** are centralized in `pkg/moqt/errors.go` (session, request,
  publish-done, and per-stream `StreamReset*` codes) — use the named constant.
- Targets a recent Go (1.26.3); prefer modern idioms (the `use-modern-go` skill
  encodes the specifics).

## Before committing

Run `/moqt-review` on the pending change before creating a commit. It checks
IETF draft compliance (correct `§X.Y` citations and matching wire behavior),
reinvented standard-library logic, and adherence to the conventions above —
distinct from `/code-review` (general bugs/simplification) and `use-modern-go`
(Go-version idioms), which cover their own concerns and shouldn't be
duplicated by it.
