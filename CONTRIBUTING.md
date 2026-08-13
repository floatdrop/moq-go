# Contributing

Thanks for your interest in contributing to `moq-go` — a Go implementation of
the Media over QUIC IETF drafts. This guide covers how the project is organized,
how to get a change building and tested, and what we look for in a pull request.

## Getting started

You'll need **Go 1.26 or newer**. Clone the repo and confirm the suite is green
before you start:

```sh
go build ./...    # build everything
go test ./...      # full suite — hermetic, no fixtures or network
```

The packages form a strict bottom-up dependency stack
(`wire → message → session → relay`), with `track`, `loc`, and `msf` as siblings
at the edges. `CLAUDE.md` has a fuller map of the architecture and where changes
belong; `README.md` is the example-driven guide to the public API.

## Development workflow

Run these before opening a pull request:

```sh
go build ./...                              # build everything
go test ./...                               # full suite
go test -race ./pkg/moqt/session/...        # race detector — for anything touching goroutines/streams
golangci-lint run                           # lint + format check (.golangci.yml)
```

A few specifics:

- **Formatting.** The enabled formatters are `goimports` and `golines` — format
  with those, not plain `gofmt`.
- **The `modernize` linter** is enabled. golangci-lint bundles a snapshot that
  predates the `errorsastype` analyzer, so until it catches up, run that check
  standalone (see `CLAUDE.md` for the exact command).
- **Benchmarks** live alongside the wire codec and session/relay hot paths. Run
  `make bench-quick` (~5s) after a change to any of them and compare `allocs/op`
  against `benchmarks/baseline-go1.26.txt`; that metric multiplies by the
  live-stream object rate, so an unexplained increase is a blocker. `make bench`
  (~4m30s) is the full suite and the only one whose `ns/op` means anything. See
  `benchmarks/README.md`.
- **Coverage** is gated. `make cover-check` fails if any package falls below its
  floor in `coverage-floors.txt`, and CI runs the same command. If you've raised
  coverage, run `make cover-floors` to re-record the floors and commit that with
  your tests — the file is generated, never hand-edited. Floors ratchet one way:
  `cover-floors` refuses to lower one, so a failing check can't be silenced by
  regenerating (pass `--allow-lower` to the script if a drop is genuinely
  correct, and explain it in the commit). When the gate fires,
  `make cover-gaps COVER_PKGS=./pkg/relay/` lists the functions no test reaches.
  A brand-new package fails the check until its floor is recorded; that's
  intended.
- **Whole-suite coverage** is a separate, non-gating view: `make cover-total`
  credits each package for code that *any* test reaches, which the floors
  deliberately don't. The floors are the better regression signal; this is the
  more honest headline number, and it's what the README's Coveralls badge
  shows. CI publishes it on every build via `make cover-profile`, so there is
  nothing to refresh by hand.

### Interoperability

This is wire-protocol code, so unit tests aren't the whole story: they round-trip
through our own codec, which means a wire-encoding regression can pass every unit
test yet break interop with other implementations. If your change touches the
wire format or session/relay behavior, run the interop suite:

```sh
make interop          # third-party test client against our relay, both transports
make interop-matrix   # several clients, pass/skip/fail matrix
make interop-client   # our client against a relay
```

See `cmd/relay/README.md` for targets and options; current results are tracked
in `STATUS.md`.

## Spec-driven changes

Protocol behavior is driven by specific, pinned IETF draft versions — currently
`draft-ietf-moq-transport-19`, `draft-ietf-moq-loc-04`,
`draft-ietf-moq-msf-01`, and `draft-ietf-moq-cmsf-01` (see the links in
`README.md`). These are the source of truth when implementing or changing wire
behavior.

- Cite the relevant draft section in comments (e.g. `§10.1`, `§11.4.2`) when you
  implement or change protocol behavior.
- Pin to the draft version the code targets. The datatracker links track moving
  drafts and may be ahead of what's implemented, so confirm the section against
  the pinned version above.
- Centralize error and reset codes in `pkg/moqt/errors.go` and use the named
  constant rather than a literal.
- Transport behavior added to the `Conn`/`Stream` interface
  (`pkg/moqt/session/conn.go`) must land in all three adapters: `quicconn`,
  `wtconn`, and `sessiontest`.

## Pull requests

- Keep changes focused; one logical change per pull request.
- Match the surrounding code's style, naming, and comment density.
- Add or update tests for the behavior you change — the suite is hermetic
  (no network, no fixtures), so new tests should be too. **Run a new test
  against the unpatched code first and confirm it fails**, then say so in the
  description. A test that has never been seen red may be asserting something
  the code already did; commit `fea80fc` is the cautionary tale, a production
  outage that every existing test passed straight through.
- Ask rather than assume. If the request or the draft is ambiguous in a way that
  changes what gets built, raise it before writing the code — a wrong assumption
  in wire-format code round-trips cleanly through our own codec and passes the
  whole suite.
- Make sure `go build ./...`, `go test ./...`, and `golangci-lint run` all pass.
  Run `-race` and the interop suite when your change warrants it (see above).
- Write a clear PR description: what changed, why, and which draft sections (if
  any) it follows.

## License

This project is dual-licensed under
[Apache-2.0](LICENSE-APACHE) and [MIT](LICENSE-MIT).

Unless you explicitly state otherwise, any contribution you intentionally submit
for inclusion in the work, as defined in the Apache-2.0 license, shall be
dual-licensed as above, without any additional terms or conditions.
