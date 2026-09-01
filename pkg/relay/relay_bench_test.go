package relay_test

// Suite 4 (relay object fanout) of the benchmark suite — see
// benchmarks/README.md. Fanout is the relay's reason to exist: one inbound
// object becomes N outbound writes.
//
// IMPORTANT CAVEAT: like the session suite, these benchmarks run over the
// synchronous in-process pipe transport, NOT real QUIC, so they measure the
// *relative* CPU and allocation cost of the relay's forwarding code (ReadObject,
// filter eval, cache Put, delta re-encode, per-subscriber WriteObject), not wire
// throughput. Use benchstat to compare across commits. For actual throughput
// numbers, see the BenchmarkFanout{Buffered,QUIC} probes in
// relay_throughput_bench_test.go.
//
// Run:
//
//	go test -run='^$' -bench=Fanout -benchmem -count=10 ./pkg/relay/

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// benchQuietLogger returns a logger that discards everything, so the relay's
// per-session slog output (accept/stop/GOAWAY lines) doesn't interleave with —
// and skew — benchmark results. Without this the relay logs several lines per
// iteration to stderr.
func benchQuietLogger() *slog.Logger {
	return slog.New(slog.DiscardHandler)
}

// Package-level sink to defeat dead-code elimination.
var benchSinkFetch *message.FetchOK

const (
	benchPubAlias = uint64(7)
)

var (
	benchNS   = wire.TrackNamespace{[]byte("video")}
	benchName = []byte("cam1")
)

func benchObjPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// BenchmarkFanout1to1 measures the full single-subscriber forward path: the
// publisher writes b.N objects on one subgroup, the relay runs runFanout
// (ReadObject + filter + cache Put + delta re-encode + WriteObject), and one
// subscriber drains all b.N. This is the 1→1 baseline for the scaling curve.
func BenchmarkFanout1to1(b *testing.B) {
	benchFanout(b, 1, 1200)
}

// BenchmarkFanout1toN measures fanout to a representative 64 subscribers. ns/op
// is per *published* object; the relay does 64 downstream WriteObjects per
// published object, so divide by 64 for the per-subscriber cost. Paired with
// BenchmarkFanout1to1 this gives the fixed + per-subscriber split that is the
// relay's core regression metric. (The full 1→N scaling curve lives in the
// throughput probes; see relay_throughput_bench_test.go.)
func BenchmarkFanout1toN(b *testing.B) {
	benchFanout(b, 64, 1200)
}

// benchFanout is the shared driver: it stands up a relay, a publisher, and
// subCount subscriber sessions each with a reader goroutine, then times the
// publisher writing b.N objects on a single subgroup and waits for every
// subscriber to receive all b.N.
func benchFanout(b *testing.B, subCount, payloadSize int) {
	b.Helper()
	ctx := b.Context()

	pubSess, teardown := connectRelay(b, relay.Config{
		// A generous send queue + disabled slow-reader escalation so the
		// benchmark measures forwarding cost, not the drop/lag reset path.
		SendQueueSize:       1 << 16,
		MaxDropsBeforeReset: 1 << 30,
		MaxFanoutLag:        time.Hour,
		Logger:              benchQuietLogger(),
	})
	defer teardown()

	// Publisher claims the track. Drain its request stream in the background
	// so the relay never blocks writing control messages back to it.
	pubReq, err := pubSess.Publish(ctx, &message.Publish{
		Namespace:  benchNS,
		Name:       benchName,
		TrackAlias: benchPubAlias,
	})
	if err != nil {
		b.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()
	go drainStream(pubReq)

	// Stand up subCount subscribers. Each gets its own session and a reader
	// goroutine that accumulates objects until it has seen b.N. readyCh
	// reports first-object arrival isn't needed; instead each reader signals
	// completion on doneCh after b.N objects.
	// Each reader consumes 1 warm-up object + b.N timed objects, signalling
	// firstCh after the warm-up object and doneCh after the last. The warm-up
	// phase exists to pull all one-time setup OUT of the timed region: the
	// per-subscriber inbox channel (sized SendQueueSize, ~1 MB here) and the
	// writer goroutine are allocated lazily by runFanout on first forward, and
	// both outbound subgroup streams are opened then too. Without the warm-up
	// these fixed costs land inside ResetTimer at low b.N and dominate B/op and
	// MB/s (e.g. ~1 MB / b.N), masking the true steady-state forwarding cost.
	doneCh := make(chan struct{}, subCount)
	firstCh := make(chan struct{}, subCount)
	for s := range subCount {
		subSess := dialAnotherClient(b, pubSess)
		_, err := subSess.Subscribe(ctx, &message.Subscribe{
			Namespace: benchNS,
			Name:      benchName,
		})
		if err != nil {
			b.Fatalf("Subscribe #%d: %v", s, err)
		}
		go benchSubscriberReader(ctx, subSess, 1+b.N, doneCh, firstCh)
	}

	payload := benchObjPayload(payloadSize)
	obj := &message.SubgroupObject{ObjectIDDelta: 0, Payload: payload}

	// Open one subgroup AFTER all subscribers are established so runFanout's
	// first-object snapshot already includes every downstream sub. All
	// Object IDs are consecutive (delta 0), so the relay keeps a single
	// outbound subgroup per subscriber (no §11.4.3 reopen).
	pubSg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     benchPubAlias,
		GroupID:        0,
	})
	if err != nil {
		b.Fatalf("OpenSubgroup: %v", err)
	}

	// Warm-up object: forces runFanout to allocate every subscriber's inbox
	// channel + writer goroutine and open both outbound streams before timing.
	if err := pubSg.WriteObject(obj); err != nil {
		b.Fatalf("warm-up WriteObject: %v", err)
	}
	for range subCount {
		<-firstCh
	}

	b.ReportAllocs()
	b.SetBytes(int64(payloadSize))
	b.ResetTimer()
	for range b.N {
		if err := pubSg.WriteObject(obj); err != nil {
			b.Fatalf("WriteObject: %v", err)
		}
	}
	// FIN the publisher subgroup so each relay→subscriber stream FINs and the
	// readers observe EOF after the final object.
	_ = pubSg.Close()
	b.StopTimer()

	for range subCount {
		<-doneCh
	}
}

// benchSubscriberReader accepts the inbound subgroup stream(s) for one
// subscriber and reads objects until it has seen want of them, then signals on
// doneCh. If firstCh is non-nil it is signalled exactly once, after the first
// object arrives — used by the fanout benchmark to confirm runFanout's
// per-subscriber setup is complete before the timer starts. It tolerates
// §11.4.3 stream reopens by accepting another data stream when the current one
// ends before the count is reached.
func benchSubscriberReader(
	ctx context.Context,
	sess *session.Session,
	want int,
	doneCh chan<- struct{},
	firstCh chan<- struct{},
) {
	defer func() { doneCh <- struct{}{} }()
	read := 0
	for read < want {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			if errors.Is(err, session.ErrPaddingStream) {
				continue
			}
			return // session closing / ctx cancelled
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			continue
		}
		for read < want {
			// Discard the decoded object: ReadObject performs real stream
			// I/O (and mutates decoder state), so the call is never elided
			// even though we drop the result — and NOT writing a shared sink
			// keeps concurrent readers (subs>1) race-free under -race.
			if _, err := sg.ReadObject(); err != nil {
				break // EOF or reset; loop to accept the next stream
			}
			read++
			if read == 1 && firstCh != nil {
				firstCh <- struct{}{}
			}
		}
	}
}

// BenchmarkFetchFromCache measures a standalone FETCH served entirely from the
// relay's object cache: the publisher pushes a range of objects, then a
// subscriber repeatedly FETCHes that range. This exercises GetRange + the
// FETCH_HEADER stream write path. The cache is pre-warmed before the timer
// starts.
func BenchmarkFetchFromCache(b *testing.B) {
	ctx := b.Context()
	pubSess, teardown := connectRelay(b, relay.Config{Logger: benchQuietLogger()})
	defer teardown()

	pubReq, err := pubSess.Publish(ctx, &message.Publish{
		Namespace:  benchNS,
		Name:       benchName,
		TrackAlias: benchPubAlias,
	})
	if err != nil {
		b.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()
	go drainStream(pubReq)

	// Pre-warm the cache: publish a 16-object group, delivered to one
	// subscriber so the relay registers the track and caches the objects.
	const rangeObjs = 16
	warmSub := dialAnotherClient(b, pubSess)
	if _, err := warmSub.Subscribe(ctx, &message.Subscribe{
		Namespace: benchNS, Name: benchName,
	}); err != nil {
		b.Fatalf("warm Subscribe: %v", err)
	}
	warmDone := make(chan struct{}, 1)
	go benchSubscriberReader(ctx, warmSub, rangeObjs, warmDone, nil)

	pubSg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     benchPubAlias,
		GroupID:        0,
	})
	if err != nil {
		b.Fatalf("OpenSubgroup: %v", err)
	}
	for range rangeObjs {
		if err := pubSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       benchObjPayload(256),
		}); err != nil {
			b.Fatalf("warm WriteObject: %v", err)
		}
	}
	_ = pubSg.Close()
	<-warmDone

	fetchSess := dialAnotherClient(b, pubSess)

	b.ReportAllocs()
	for b.Loop() {
		stream, err := fetchSess.Fetch(ctx, &message.Fetch{
			Namespace: benchNS,
			Name:      benchName,
			Parameters: message.Parameters{
				fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: rangeObjs - 2}),
			},
		})
		if err != nil {
			b.Fatalf("Fetch: %v", err)
		}
		benchSinkFetch = stream.OK
		// Drain the FETCH response stream so the relay-side writer completes
		// and the transport reclaims the uni-stream before the next iteration.
		drainFetchStream(ctx, fetchSess)
		_ = stream.Close()
	}
}

// drainFetchStream accepts and discards one inbound FETCH response stream.
func drainFetchStream(ctx context.Context, sess *session.Session) {
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		return
	}
	for {
		if _, err := fs.ReadObject(); err != nil {
			return
		}
	}
}

// drainStream reads and discards control messages off a request stream until it
// closes. Keeps the relay from blocking when it writes back on the stream.
func drainStream(stream io.Reader) {
	for {
		if _, err := message.Parse(stream); err != nil {
			return
		}
	}
}

// subsName builds a stable sub-benchmark name like "subs=8".
func subsName(n int) string {
	return "subs=" + itoaBench(n)
}

func itoaBench(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}
