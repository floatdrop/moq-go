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

```sh
go test -run='^$' -bench=. -skip='FanoutBuffered|FanoutQUIC' -benchmem -count=10 ./pkg/... 2>/dev/null
```

- `-run='^$'` skips the normal tests (no name matches), so only benchmarks run.
- `-skip='FanoutBuffered|FanoutQUIC'` excludes the throughput probes (below),
  which are noisy and not regression signals.
- `-benchmem` reports `B/op` and `allocs/op` — the primary regression signal.
- `-count=10` gives `benchstat` enough samples for a stable mean and variance.
- `2>/dev/null` drops the relay's per-session shutdown/GOAWAY log lines (the
  benchmarks already silence the relay logger; this catches residual transport
  noise) so **stdout** stays `benchstat`-parseable.

Compile-and-run smoke check (no timing):

```sh
go test -run='^$' -bench=. -benchtime=1x ./...
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

# base commit
go test -run='^$' -bench=. -skip='FanoutBuffered|FanoutQUIC' -benchmem -count=10 ./pkg/... > old.txt 2>/dev/null
# after your change
go test -run='^$' -bench=. -skip='FanoutBuffered|FanoutQUIC' -benchmem -count=10 ./pkg/... > new.txt 2>/dev/null

benchstat old.txt new.txt
```

`benchstat` prints the per-benchmark delta and flags statistical significance.
Treat a significant `allocs/op` regression in suites 1, 2, or the cache as a
blocker — those allocations multiply by the per-object call rate.

## Baseline

[`baseline-go1.26.txt`](baseline-go1.26.txt) is a committed reference run of the
regression suite on one machine (the `goos`/`goarch`/`cpu` header records which).
Use it as a sanity check and as the `old.txt` for a quick local diff. Refresh it
deliberately — never silently — when an intentional performance change lands:

```sh
go test -run='^$' -bench=. -skip='FanoutBuffered|FanoutQUIC' -benchmem -count=10 \
  ./pkg/moqt/wire/ ./pkg/moqt/message/ ./pkg/moqt/session/ ./pkg/relay/ ./pkg/relay/cache/ \
  > benchmarks/baseline-go1.26.txt 2>/dev/null
```

Absolute numbers are not portable across machines, so don't gate CI on them. The
value is the *shape* (allocation counts) and the per-commit `benchstat` delta on
the same machine.

## CI

A compile-and-run smoke job (`-bench=. -benchtime=1x -count=1`) keeps the
benchmarks from bit-rotting. CI runners are too noisy for timing assertions, so
there are no pass/fail thresholds — timing comparison stays a local/manual
`benchstat` step.
