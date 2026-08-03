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
- **Benchmarks** live alongside the wire codec and session/relay hot paths:
  `go test -run='^$' -bench=. -benchmem ./...`. See `benchmarks/README.md` for
  the `benchstat` regression-comparison workflow.

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
`draft-ietf-moq-transport-19`, `draft-ietf-moq-loc-04`, and
`draft-ietf-moq-msf-01` (see the links in `README.md`). These are the source of
truth when implementing or changing wire behavior.

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
  (no network, no fixtures), so new tests should be too.
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
