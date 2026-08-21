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
| 4. Relay fanout | `pkg/relay` | `Fanout1to1`, `Fanout1toN` (64 subs), `FetchFromCache` |
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
| Smoke | `make bench-smoke` | ~7s | compiles and runs, no numbers |

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
- `-skip='FanoutBuffered|FanoutQUIC'` excludes the throughput probes (below),
  which are noisy and not regression signals.
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

## Throughput probes (non-regression)

Two extra relay benchmarks in `pkg/relay/relay_throughput_bench_test.go` answer
"how fast can it actually go", each a different question. They are **excluded
from the regression suite and the baseline** — run them on demand.

| Benchmark | Transport | Answers |
|---|---|---|
| `BenchmarkFanoutBuffered` | buffered in-memory pipe (`sessiontest.NewConnPairBuffered`) | the **relay's own** forwarding ceiling, with the synchronous-pipe scheduler ping-pong removed but no quic-go/kernel noise |
| `BenchmarkFanoutQUIC` | real loopback QUIC on `127.0.0.1` | the end-to-end **CPU-bound loopback ceiling**, dominated by quic-go (packetisation, ACKs, TLS, UDP syscalls) |

```sh
go test -run='^$' -bench='FanoutBuffered|FanoutQUIC' -benchmem -count=6 ./pkg/relay/ 2>/dev/null
```

`BenchmarkFanoutQUIC` is **not** network throughput: there is no loss, RTT, or
congestion, and the number reflects quic-go's per-packet work far more than the
relay's. Treat `allocs/op`, not `ns/op`, as the stable signal (kernel UDP
scheduling makes loopback timing noisy). The QUIC matrix is intentionally small
(`subs=1,8`) — each subscriber is a separate loopback connection, and `subs=1`
is the headline per-stream ceiling.

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
