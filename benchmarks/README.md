# Benchmarks

A lean, regression-focused benchmark suite for the layers most load-bearing in a
media relay: the **wire codec** (runs on every byte), the **message-passing
path** (session control + relay object fanout), and the **object cache**.

The suite is deliberately small. Each benchmark covers one representative case of
a distinct code path — a single ~1 QUIC-packet payload (1200 B), one complex
control message plus one trivial one, one fan-out point — rather than sweeping
size/type/count matrices. The goal is a fast, low-variance signal where
**`allocs/op` is the primary regression metric**: an allocation added to a
per-object path multiplies by the live-stream object rate, so it shows up here
before it can hide inside a larger number.

## Regression suite

| Suite | Package | Benchmarks |
|---|---|---|
| 1. Wire primitives | `pkg/moqt/wire` | `VarintEncode`, `VarintBytesRoundTrip`, `KVPairsEncode`, `FrameRoundTrip` |
| 2. Message codec | `pkg/moqt/message` | `SubgroupObjectAppend`, `SubgroupObjectParse`, `MarshalRoundTrip` (SUBSCRIBE + REQUEST_OK), `ParametersRoundTrip` |
| 3. Session passing | `pkg/moqt/session` | `ControlRoundTrip`, `SubgroupThroughput`, `SubgroupForwardCodec` |
| 4. Relay fanout | `pkg/relay` | `Fanout1to1`, `Fanout1toN` (64 subs), `DatagramFanout` (audio, 1 sub), `FetchFromCache` |
| 5. Cache | `pkg/relay/cache` | `CachePut`, `CacheGet` (hit/miss), `CacheGetRange` |

Suites 1, 2, and 5 are pure in-memory micro-benchmarks — directly comparable
across commits. Suites 3 and 4 run over the in-process `sessiontest` pipe
transport — **not real QUIC** — so they measure the *relative* CPU and
allocation cost of our code, not wire throughput. `BenchmarkSubgroupForwardCodec`
is the one transport-free data-plane benchmark: it exercises `ReadObject` ↔
`WriteObject` directly, so it's the cleanest signal for codec regressions on the
hot path.

`Fanout1to1` and `Fanout1toN` together give the split that matters: `Fanout1to1`
is the fixed per-object cost, and `(Fanout1toN − Fanout1to1) / 63` is the
marginal per-subscriber cost.

### Running

The suite runs in two tiers, because the full one takes **4m30s** — too slow to
sit in the loop after every change, which is exactly when a new allocation is
cheapest to notice.

| Tier | Command | Cost | Gives you |
|---|---|---|---|
| Quick | `make bench-quick` | ~5s | `allocs/op`, `B/op` |
| Full | `make bench` | ~4m30s | `allocs/op`, `B/op`, **`ns/op`** |
| Smoke | `make bench-smoke` | ~15s | compiles and runs, no numbers (`-short`) |

**Run the quick tier after every change to a hot path.** Run the full tier
before claiming a timing result, before refreshing the baseline, and when the
quick tier shows something you're about to act on.

#### Why the quick tier works

`make bench-quick` replaces `-count=10` at the default 1s-per-benchmark with a
fixed `-benchtime=500x -count=1`. Allocation counts are deterministic *per
iteration*, so they do not need a long run to stabilize — only enough
iterations to amortize per-run setup, which for this suite is reached by 500.
Measured against the full run, the quick tier reports **identical `allocs/op`
for every benchmark except the two relay fanout ones**, at 1/60th the wall
clock.

#### What the quick tier is not

- **`ns/op` from `make bench-quick` is meaningless.** 500 iterations does not
  warm caches or amortize setup: `Fanout1to1` reads ~2.4x slower there than it
  actually is. Anything about time comes from `make bench`, no exceptions.
- **`BenchmarkFanout1toN` carries ~±2 allocs of real scheduling noise**, in both
  tiers (the full `-count=10` run itself reports 130 and 131 across samples),
  and can read further off under a loaded machine. It is a 64-subscriber
  multi-goroutine fanout, so its per-op allocations genuinely depend on
  scheduling; `-p 1` does not fix this. Treat only a shift well outside that
  band as signal, and confirm it with the full tier before acting.

#### The flags, if you run it by hand

- `-run='^$'` skips the normal tests (no name matches), so only benchmarks run.
- `-skip='Conference'` excludes the conference capacity run (below), which is
  real-time paced and not a regression signal.
- `-benchmem` reports `B/op` and `allocs/op` — the primary regression signal.
- `-count=10` gives `benchstat` enough samples for a stable mean and variance.
- `2>/dev/null` drops the relay's per-session shutdown/GOAWAY log lines (the
  benchmarks already silence the relay logger; this catches residual transport
  noise) so **stdout** stays `benchstat`-parseable.

Narrow either tier to the packages you touched with `BENCH_PKGS`, which cuts the
quick tier to about a second:

```sh
make bench-quick BENCH_PKGS=./pkg/moqt/wire/
make bench BENCH_PKGS='./pkg/relay/ ./pkg/relay/cache/'
```

## Conference capacity (non-regression)

The regression suite says what one object costs; it cannot say how many people
a relay serves. That is a scenario question, answered by `internal/conference`:
a teleconference where a few **speakers** each publish a video track (one
subgroup stream per GOP, keyframe first) and an audio track (one datagram per
frame, each its own Group), and every other **participant** subscribes to all
of them. Media is paced at its real rate and every Object carries its send
time, so each participant measures its own delivery and one-way latency, and
counts as **served** when, over the measured window:

- it lost at most 1% of its audio (`-slo-audio-loss`),
- it missed no video Object and saw no video stream reset, and
- its p99 latency, audio and video each, stayed under 200 ms (`-slo-latency`).

The default profile is two 720p speakers: 1.5 Mbps / 30 fps video with a 2 s
GOP (keyframes 10× a delta frame) and 48 kbps audio in 20 ms frames. Relay
egress is speakers × bitrate × participants, ~3.1 Gbps at 1000 participants,
which can hit a NIC or VM bandwidth cap before any CPU limit.

It runs two ways, over the same driver:

| | Transport | Answers |
|---|---|---|
| `BenchmarkConference` (`pkg/relay`) | in-process `memconn`, relay with production defaults | the **relay's own** cost at scale: no QUIC, TLS or kernel on the CPU, and the profile splits by pprof label (`role=relay` / `role=participant`) |
| `cmd/confbench` | real QUIC (`moqt://`) or WebTransport (`https://`) to a running `cmd/relay` | **how many participants** a relay serves, with its CPU, memory and goroutines read off `-relay-metrics` and a CPU profile of the window from `-relay-pprof` |

```sh
make bench-conference                                   # in-process, 1000 participants, relay hotspots
make bench-conference CONFERENCE_FLAGS='-conference.participants=2000 -conference.window=10s'

go run ./cmd/relay -health-addr 127.0.0.1:8080 -pprof-addr 127.0.0.1:6060
go run ./cmd/confbench -relay moqt://127.0.0.1:4433 -ramp 250,500,750,1000 \
    -relay-metrics http://127.0.0.1:8080/healthz/metrics -relay-pprof http://127.0.0.1:6060
```

`-ramp` runs each count on fresh connections and stops after the first that
leaves someone unserved (`-ramp-all` runs them all); every run publishes under
a room of its own, so a relay that still caches the previous run's tracks is
not handed a changed Group 0. Run `cmd/confbench -h` for the profile, join and
SLO flags.

What the numbers are and are not:

- **Every participant lives in the harness process**, so the harness needs
  about as much CPU as the relay. On one machine they compete; pin them apart
  (`taskset`, or `docker run --cpuset-cpus`) or use two machines. The
  `pacing` line is the check: speakers far behind their schedule mean the
  harness ran out of CPU or the relay pushed back on its publishers, and
  confbench warns; `process cpu` against the core count tells which.
- **Loopback is not a network**: no loss, sub-millisecond RTT. Latency here is
  the relay and transport queueing, a floor on what a real path adds to.
- **The OS matters more than the relay.** quic-go sends one packet per
  syscall where it has no GSO (macOS), through the listener's single UDP
  socket, whose writes Go serializes; on Linux it batches with GSO, and
  `cmd/relay -udp-send-writers N` spreads the writes over N handles of that
  socket, which is what lifts the single-socket limit. Compare Linux numbers
  to Linux numbers.
- `BenchmarkConference` reports per-run custom metrics (`served`,
  `audio-p99-ms`, `cpu-cores`, `allocs/delivered`, …), not `ns/op`. Its CPU
  includes the participants' session code; split it with
  `go tool pprof -tagfocus=role=relay`.

## Comparing with benchstat

```sh
go install golang.org/x/perf/cmd/benchstat@latest

git stash && make bench > old.txt          # base commit
git stash pop && make bench > new.txt      # after your change

benchstat old.txt new.txt
```

`benchstat` prints the per-benchmark delta and flags statistical significance.
Treat a significant `allocs/op` regression in suites 1, 2, or the cache as a
blocker — those allocations multiply by the per-object call rate.

## Baseline

[`baseline-go1.27.txt`](baseline-go1.27.txt) is a committed reference run of the
regression suite on one machine (the `goos`/`goarch`/`cpu` header records which).
Use it as a sanity check and as the `old.txt` for a quick local diff. Refresh it
deliberately — never silently — when an intentional performance change lands, and
say in the commit message why the numbers moved:

```sh
make bench-baseline
```

`BENCH_PKGS` defaults to `./pkg/...`, so neither tier reaches `cmd/`. The
relay's metrics exporter has a per-object path with its own benchmarks, and they
only run when asked for by name:

```sh
make bench-quick BENCH_PKGS=./cmd/relay/
```

Worth remembering when changing that exporter: an allocation added there
multiplies by the object rate per subscriber, and the default guard will not see
it.

Absolute numbers are not portable across machines, so don't gate CI on them. The
value is the *shape* (allocation counts) and the per-commit `benchstat` delta on
the same machine.

## CI

The `Benchmarks (smoke)` job runs `make bench-smoke` — compile-and-run,
one iteration each — which keeps the benchmarks from bit-rotting into something
that no longer builds. CI runners are too noisy for timing *or* allocation
assertions, so there are no pass/fail thresholds; comparison stays a local
`make bench-quick` / `benchstat` step.
