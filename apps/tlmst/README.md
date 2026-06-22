# tlmst — Media-over-QUIC video conferencing demo

`tlmst` is a small [Wails3](https://v3.wails.io/) desktop application that
demonstrates the relay's **multi-party video-conferencing** capabilities. It is a
test / demo client for the parent [`moq-go`](../../README.md) library, not a
production app: each participant joins a room on a relay, publishes their camera
and microphone, and subscribes to everyone else in the room — all carried over
Media over QUIC Transport (MOQT).

It exists to exercise the library end-to-end the way a real client would —
SETUP, publish, subscribe, subgroup data streams, and live teardown — against a
running [`cmd/relay`](../../cmd/relay).

## What it does

- **Join a room.** Establishes a QUIC + MOQT session against a relay address and
  announces the local participant.
- **Publish local media.** Captures camera/microphone in the webview frontend,
  encodes it, and publishes it as MOQT tracks through the relay.
- **Subscribe to peers.** Discovers other participants in the room and subscribes
  to their tracks; remote frames are decoded and rendered in the UI. One
  goroutine per subgroup means frames from adjacent GOPs can arrive concurrently.
- **Show live stats.** A debug panel surfaces connection stats and a stream of
  backend log records (proxied to the frontend via Wails events).

### How it maps to the library

The Go backend is the interesting part for library users:

- `sessionservice.go` — the Wails service bound to the frontend. Opens the
  `session.Session` (`pkg/moqt/session`) over `quicconn`, and owns join/leave.
- `publisher.go` — publishes local media tracks to the relay.
- `subscriber.go` — subscribes to remote participants and forwards decoded
  `MediaChunk`s to the UI.
- `stats.go` — collects per-connection statistics for the debug panel.
- `main.go` — Wails app wiring; registers the typed events (`moq:log`,
  `moq:participant-joined`, `moq:participant-left`, `moq:media-chunk`).

The frontend (`frontend/`, SvelteKit) has two screens: a join screen (`/`) and
the call screen (`/call`). It talks to the backend exclusively through the
generated bindings in `frontend/bindings/`.

## Why it is a separate module

`apps/tlmst` is a **deliberately separate Go module** (see `CLAUDE.md` at the
repo root) so that its CGO/WebKit/Wails dependencies stay out of the parent
module's hermetic, pure-Go build and test suite. It resolves the parent library
via a `replace github.com/floatdrop/moq-go => ../..` directive, so plain `go`
commands work without a `go.work` file. Consequently `go test ./...` from the
repo root does **not** build or test this app — it has its own CI job (see
[Continuous integration](#continuous-integration) below).

## Prerequisites

- Go (matching the version in `go.mod`).
- Node.js + npm (for the SvelteKit frontend).
- The [Wails3 CLI](https://v3.wails.io/getting-started/installation/):
  `go install github.com/wailsapp/wails/v3/cmd/wails3@latest`
- Platform GUI toolkits required by Wails3:
  - **macOS** — Xcode command-line tools.
  - **Linux** — `libgtk-3-dev` and `libwebkit2gtk-4.1-dev`.
- A running relay to connect to:

  ```sh
  go run ./cmd/relay      # from the repo root; self-signed cert on :4433
  ```

## Running

From this directory (`apps/tlmst`):

```sh
wails3 dev      # development mode with hot-reload (frontend + backend)
wails3 build    # production build into ./bin
```

Then point the app at your relay address (default `localhost:4433`) on the join
screen. Launch a second instance to see two participants in the same room.

## Building manually

The Go binary embeds `frontend/dist` via `//go:embed`, so the frontend must be
built before the Go build:

```sh
cd frontend && npm ci && npm run build && cd ..
go build .      # builds the desktop binary (the root package)
```

> Note: `go build ./...` will fail in `build/ios/` — that directory is Wails iOS
> scaffolding that only compiles under the `ios` build tag (via Xcode /
> `build/ios/build.sh`). Build the root package (`go build .`) instead.

## Continuous integration

Because this module is excluded from the root suite, the repository's CI has a
dedicated `tlmst` job that guards against breakage (e.g. a stale module path in
the regenerated frontend bindings). It builds the frontend and then compiles the
Go binary, which is enough to catch import/binding regressions. See
`.github/workflows/ci.yml`.

## Project structure

- `main.go`, `sessionservice.go`, `publisher.go`, `subscriber.go`, `stats.go` —
  the Go backend (`package main`).
- `frontend/` — SvelteKit frontend (`src/`), generated Wails bindings
  (`frontend/bindings/`), and the built assets (`frontend/dist/`, gitignored).
- `build/` — per-platform Wails build scaffolding and `Taskfile.yml` includes.
- `Taskfile.yml` — Task targets (`build`, `dev`, `run`, server/Docker modes).
