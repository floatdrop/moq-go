# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

A Go implementation of the Media over QUIC Transport (MoQT), LOC, and MSF IETF
drafts: a transport-agnostic session library, a single-instance relay, and demo
CLIs. `README.md` has an extensive, example-driven usage guide for the public
API — read it when you need to know *how to call* something; this file covers
*how the code is organized and how to work on it*.

## Ask, don't assume

**Ask when the answer changes what gets built.** Do not fill a gap with a
plausible guess and keep going. In protocol code a wrong assumption is not a
detour you can back out of later — it is a wire-format bug that passes every
test in this repo, because the same assumption sits on both sides of the
round-trip. The unit suite cannot catch it and neither can review, since the
assumption is invisible once it's written as code.

Ask before writing code when:

- **The request is ambiguous about scope** and the readings differ in what
  ships. "Fix the FETCH path" — the joining FETCH, the standalone one, or both?
- **The draft is ambiguous**, or it says MAY/SHOULD and the choice is observable
  on the wire or to a peer implementation.
- **The change touches a contract**: an exported API in `pkg/`, an error or
  reset code, a default, or behavior another implementation relies on for
  interop.
- **Two designs are both defensible** and picking wrong means rewriting the
  slice rather than editing it.
- **The request and the code disagree.** Report the discrepancy — don't quietly
  implement whichever one you decided they meant.

Don't ask what the repo already answers. Read it instead: the draft itself
(fetched from datatracker — never recalled from memory, and never inferred from
an existing `§` comment, which may cite a section that moved between draft
versions), `README.md` for public API shape, the surrounding code for naming and
style, `git log`/`git blame` for why something is the way it is. A question the
repo answers costs the reader the same interruption as a good one.

Batch questions into one round rather than trickling them out, and give a
recommendation with each — "I'd do X because Y; Z is the alternative" is far
easier to answer than an open question.

When you genuinely cannot ask — an autonomous run, no one at the keyboard —
proceed on the most defensible reading, but **mark it**: one line in the
response and in the PR description saying what you assumed and what would change
if it's wrong. An unmarked assumption is the failure mode. A marked one is a
review item.

## Commands

```sh
go build ./...                              # build everything
go test ./...                               # full suite (hermetic — no fixtures/network)
go test ./pkg/moqt/session/ -run TestName   # single test (or a -run regex)
go test -race ./pkg/moqt/session/...        # race detector — use for anything touching goroutines/streams
golangci-lint run                           # lint/format check (.golangci.yml)

make bench-quick                            # ~5s  allocs/op guard — run after every hot-path change
make bench                                  # ~4m30s full suite incl. ns/op — for timing claims only
make cover-gaps                             # every function the tests never reach, least-covered first
make cover-total                            # the published figure (-coverpkg, both modules) — nothing gates on it
make cover-site                             # ...plus the report CI publishes to Pages, to preview locally
```

`make bench-quick BENCH_PKGS=./pkg/moqt/wire/` narrows either benchmark tier to
the packages you touched. **Never read `ns/op` from the quick tier** — 500
iterations doesn't amortize setup, so it can be off by 2x. See
`benchmarks/README.md` for why the tiers differ and what each proves.

The lint config's enabled formatters are `goimports` and `golines`, so format
with those (not plain `gofmt`).

The `modernize` linter is enabled, which covers `errorsastype` (rewrite
`errors.As(err, &t)` → `errors.AsType[*T](err)`). golangci-lint v2.13.0 bundles
a modernize snapshot that ships that analyzer, so `golangci-lint run` covers it.
CI additionally runs the analyzer standalone at `@latest`, which catches
analyzers newer than the bundled snapshot; run it the same way when you want
that check locally (no go.mod changes — pinning gopls as a tool would perturb
the transport deps):

```sh
go run golang.org/x/tools/gopls/internal/analysis/modernize/cmd/modernize@latest \
  -errorsastype ./...        # add -fix to apply
```

Run the stack locally (each in its own terminal):

```sh
go run ./cmd/relay              # self-signed cert on :4433
go run ./cmd/video -in clip.mp4 publish    # or ./cmd/clock for the raw-MOQT demo
go run ./cmd/video -out /tmp/recv.mp4 subscribe
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
- [MOQT CMSF](https://datatracker.ietf.org/doc/draft-ietf-moq-cmsf/)

Only retrieve the sections relevant to the task at hand.

## Conventions

- **Spec-driven, with citations.** Comments cite draft sections (`§10.1`,
  `§11.4.2`) referencing the pinned draft versions
  (`draft-ietf-moq-transport-19`, `-loc-04`, `-msf-01`, `-cmsf-01`) — the source of truth
  when implementing or changing protocol behavior. See the Reference Documents
  section above for the datatracker links.
- **Message types** implement `Append(*wire.Writer)` + `Parse(*wire.Reader)`;
  request-stream messages additionally satisfy `message.WithRequestID`
  (`GetRequestID`/`SetRequestID`).
- **Error/reset codes** are centralized in `pkg/moqt/errors.go` (session, request,
  publish-done, and per-stream `StreamReset*` codes) — use the named constant.
- Targets a recent Go (1.27); prefer modern idioms (the `use-modern-go` skill
  encodes the specifics).

## Definition of done

A slice of work is not done when it compiles and the suite is green. It is done
when **a regression of it would fail the suite**. Work through this in order
before committing; none of it is optional because the change "is small".

### 1. A test that fails without the change

Write the test, run it against the *unpatched* code, and confirm it fails — and
fails for the reason you expect, not because a helper panicked. A test written
after the fix that has never been seen red is not evidence of anything; it may
be asserting something the code did all along. Say in the commit message that
you verified it fails first.

This is what commit `fea80fc` is a monument to. The relay built its
`LARGEST_OBJECT` watermark only from objects it had watched arrive and discarded
the value on three of the four paths an upstream can report it on. Every test
passed. Coverage over those paths was fine — the messages *were* parsed and
handled, just not saved. The failure needed a three-instance mesh and a track
published exactly once to appear, and in production it silently made a
participant invisible for an entire call, with nothing logged. Three regression
tests came with the fix, each verified red beforehand.

The rules that fall out of it:

- **Test the shape, not the line.** The bug wasn't an unreached statement, it
  was a missing behavior on a reached one. Ask "what sequence of events would
  break this?" and write *that*, rather than calling the new function and
  asserting it returns.
- **Test at the layer the bug lives at.** A cross-relay bug needs two relays
  wired together (`pkg/relay/cross_relay_test.go`), not a unit test of the
  setter. `sessiontest` and `relaytest` exist to make that cheap.
- **One path is not all paths.** When a value can arrive on several paths —
  SUBSCRIBE_OK, PUBLISH, REQUEST_UPDATE_OK — cover each. `fea80fc` needed three
  tests for one fix, and the two-publisher case was the only one that
  distinguished re-deriving a value from copying it through.
- **Prefer the boring end of the range.** Published-once, subscriber-arrives-
  late, zero objects, one publisher vs. two. That is where this bug lived.

### 2. `-race` for anything concurrent

`go test -race ./pkg/moqt/session/... ./pkg/relay/...` if the change touches
goroutines, streams, or shared relay state. The default suite will not find it.

### 3. Benchmarks, if the change is on a hot path

Hot paths are the wire codec, per-object session and relay paths, and the cache
— anything whose cost multiplies by the live-stream object rate.

```sh
make bench-quick        # ~5s; compare allocs/op against benchmarks/baseline-go1.27.txt
```

`allocs/op` is the regression metric: one allocation added to a per-object path
multiplies by the object rate, so it shows up here before it can hide inside a
larger number. An unexplained increase is a blocker, not a note for later. Use
`make bench` + `benchstat` when you need `ns/op` or want to confirm a quick-tier
reading — and never quote timing from the quick tier.

**New hot path ⇒ new benchmark.** The suite is deliberately small (one
representative case per code path); keep it that way, but don't let a new
per-object path in unmeasured.

### 4. Coverage is measured, not enforced

Untested code is dead code: either it has a caller whose behavior nobody is
checking, or it has no caller at all. Coverage is a floor, not a target — it
proves a line ran, not that anything asserted what it did (see `fea80fc` above,
where the lines ran fine). Don't chase the number with tests that execute code
without asserting on it.

**Nothing gates on it.** The per-package floors and `coverage-floors.txt` were
removed deliberately. CI measures coverage on every push and publishes it, but
no job fails on a drop — a slice of work can land untested with the build fully
green. That check is yours to make, not CI's:

```sh
make cover-total    # the published figure: whole-suite, across both modules
make cover-site     # ...plus the report CI publishes, to preview it locally
make cover          # per-package, each credited only for its OWN tests
make cover-gaps COVER_PKGS=./pkg/relay/cache/   # least-covered functions first
```

Read `cover-gaps` and decide, per entry, *missing test* or *delete it*.
Repo-wide it's ~390 lines, so it's only useful scoped.

Where the numbers surface: the README badge reads a shields endpoint published
to <https://floatdrop.github.io/moq-go/>, which also serves a line-by-line HTML
report per module — that page is the replacement for what Coveralls used to
answer, i.e. *which lines in this file are uncovered*. Each CI run additionally
prints the per-package table on its own summary page, so a drop is visible in
the run that caused it without opening anything.

**The published figure is the `-coverpkg` view**, crediting a package for every
line the suite exercises rather than only for what its own tests reach. The two
differ a lot and unevenly: `wire`'s own tests reach ~81% of it while the suite
as a whole drives ~97%, because most of `wire` runs under `message`, `session`,
and `relay` tests. `make cover` shows the narrow view and `make cover-total` the
published one — don't read a low number from the former as a thin package
without checking the latter.

It spans both modules. `pkg/relay/discovery/etcd` has its own `go.mod`, so
`go test ./pkg/...` cannot reach it; `scripts/coverage-report.sh` runs it
separately (`COVER_SUBMODULES`) and concatenates the profile, which stays
unambiguous because profile blocks are keyed by full import path. Without that
the one distributed `DiscoveryStore` backend, and the `relay-etcd` binary the
multi-instance deployments run, would be invisible.

Only `cmd/*` and `internal/dial` are outside, and only because `COVER_PKGS`
defaults to `./pkg/...`. They're thin wiring over tested packages, covered
end-to-end by the interop suite instead — not an invitation to add untested code
there. The test helpers are *in*: `sessiontest`, `relaytest`, and `conntest` are
measured like everything else, so expect the number to move when you refactor
one.

**The transport adapters are where a healthy number still misleads you.** The
shared `Conn` conformance suite (`77ba4ec`) drives all three adapters over
in-process plumbing, which is why `quicconn` and `wtconn` now read in the
high-seventies-to-eighties rather than the fifties. What it cannot reach is
everything that touches a real transport: `quicconn.Dial` and `wtconn.upgrade`
are the actual network dial and HTTP/3 upgrade and sit at 0% and ~55%, and the
error halves of `OpenStream`/`AcceptStream` need a transport that fails. Those
run only in the interop jobs.

So when you add transport behavior to `Conn`/`Stream` and land it in all three
adapters (see Architecture), the two real ones ship covered by conformance but
unproven against quic-go and webtransport-go. Run `make interop-loopback` for
that change — a stale flag in `entrypoint-relay.sh` once broke the WebTransport
path with the whole unit suite green.

### 5. `/moqt-review`

Run it on the pending change before creating the commit. It checks IETF draft
compliance (correct `§X.Y` citations and matching wire behavior), reinvented
standard-library logic, and adherence to the conventions above — distinct from
`/code-review` (general bugs/simplification) and `use-modern-go` (Go-version
idioms), which cover their own concerns and shouldn't be duplicated by it.
