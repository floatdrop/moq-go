package relay_test

import (
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// recordingMetrics is a concurrency-safe [relay.Metrics] for assertions. It
// embeds NopMetrics so it keeps satisfying the interface as the interface
// grows, then overrides each method with an atomic counter.
type recordingMetrics struct {
	relay.NopMetrics

	sessionsOpened atomic.Int64
	sessionsClosed atomic.Int64
	upstreamOpened atomic.Int64
	subsOpened     atomic.Int64
	subsClosed     atomic.Int64
	received       atomic.Int64
	forwarded      atomic.Int64
	dropped        atomic.Int64
	slowReset      atomic.Int64
	fetchServed    atomic.Int64
	fetchObjects   atomic.Int64
	dialFailed     atomic.Int64
	nsResolved     atomic.Int64

	// mu guards the label observations below. The counters above stay atomic
	// because they are written from the per-object fanout path; these are
	// read once at the end of a test.
	mu         sync.Mutex
	trackNames map[string]int
	subgroups  map[uint64]int
	resets     map[relay.ResetCause]int
}

func (m *recordingMetrics) note(t relay.TrackRef, subgroup uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.trackNames == nil {
		m.trackNames = make(map[string]int)
		m.subgroups = make(map[uint64]int)
	}
	m.trackNames[t.Name+"/"+t.Leg.String()]++
	m.subgroups[subgroup]++
}

func (m *recordingMetrics) SessionOpened(leg relay.Leg) {
	m.sessionsOpened.Add(1)
	if leg == relay.LegUpstream {
		m.upstreamOpened.Add(1)
	}
}
func (m *recordingMetrics) SessionClosed(relay.Leg)           { m.sessionsClosed.Add(1) }
func (m *recordingMetrics) SubscriptionOpened(relay.TrackRef) { m.subsOpened.Add(1) }
func (m *recordingMetrics) SubscriptionClosed(relay.TrackRef) { m.subsClosed.Add(1) }

func (m *recordingMetrics) ObjectReceived(relay.TrackRef, uint64) {
	m.received.Add(1)
}

func (m *recordingMetrics) ObjectForwarded(t relay.TrackRef, subgroup uint64) {
	m.forwarded.Add(1)
	m.note(t, subgroup)
}

func (m *recordingMetrics) ObjectDropped(t relay.TrackRef, subgroup uint64) {
	m.dropped.Add(1)
	m.note(t, subgroup)
}

func (m *recordingMetrics) SubgroupStreamReset(_ relay.TrackRef, _ uint64, cause relay.ResetCause) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resets == nil {
		m.resets = make(map[relay.ResetCause]int)
	}
	m.resets[cause]++
}

func (m *recordingMetrics) SubscriptionResetSlowReader(_ relay.TrackRef, cause relay.ResetCause) {
	m.slowReset.Add(1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resets == nil {
		m.resets = make(map[relay.ResetCause]int)
	}
	m.resets[cause]++
}

func (m *recordingMetrics) FetchServed(_ relay.TrackRef, n int) {
	m.fetchServed.Add(1)
	m.fetchObjects.Add(int64(n))
}

func (m *recordingMetrics) UpstreamDialFailed(string) { m.dialFailed.Add(1) }
func (m *recordingMetrics) NamespaceResolved(int)     { m.nsResolved.Add(1) }

// resetCount reports how many times cause was recorded.
func (m *recordingMetrics) resetCount(cause relay.ResetCause) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resets[cause]
}

// TestMetricsHooks drives a publish → subscribe → forward → fetch flow through
// the relay with a recording [relay.Metrics] installed and asserts each hook
// fires with the expected counts, including that the session/subscription
// gauges balance once the relay tears down.
func TestMetricsHooks(t *testing.T) {
	rec := &recordingMetrics{}
	pubSess, teardown := connectRelay(t, relay.Config{Metrics: rec})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(teardown) }
	defer stop()

	const alias = uint64(7)
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: ns, Name: name, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Reader: accept the live subgroup stream and count its objects.
	type result struct {
		n   int
		err error
	}
	subgroupCh := make(chan result, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			subgroupCh <- result{err: err}
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			subgroupCh <- result{err: errors.New("not a SubgroupStream")}
			return
		}
		var n int
		for {
			if _, err := sg.ReadObject(); err != nil {
				subgroupCh <- result{n: n}
				return
			}
			n++
		}
	}()

	const sgCount = 5
	pubSg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     alias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for i := range sgCount {
		if err := pubSg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('A' + i)}}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := pubSg.Close(); err != nil {
		t.Fatalf("pubSg.Close: %v", err)
	}

	var received int
	select {
	case res := <-subgroupCh:
		if res.err != nil {
			t.Fatalf("subgroup reader: %v", res.err)
		}
		received = res.n
	case <-time.After(2 * time.Second):
		t.Fatal("subgroup did not arrive within deadline")
	}
	if received != sgCount {
		t.Fatalf("received %d objects, want %d", received, sgCount)
	}

	// Hot-path: every delivered object was counted as forwarded, none dropped.
	if got := rec.forwarded.Load(); got != int64(sgCount) {
		t.Errorf("ObjectForwarded = %d, want %d", got, sgCount)
	}
	if got := rec.dropped.Load(); got != 0 {
		t.Errorf("ObjectDropped = %d, want 0", got)
	}
	// With one subscriber, received and forwarded are one-to-one.
	if got := rec.received.Load(); got != int64(sgCount) {
		t.Errorf("ObjectReceived = %d, want %d", got, sgCount)
	}
	// The labels must carry the track name and the leg the session sits on.
	// Both peers dialled in, so everything here is LegLocal — an upstream leg
	// only appears on a relay that dialled out (cross_relay_test.go).
	rec.mu.Lock()
	names := maps.Clone(rec.trackNames)
	subgroups := maps.Clone(rec.subgroups)
	rec.mu.Unlock()
	if got := names["cam1/local"]; got != sgCount {
		t.Errorf("forwarded objects labelled cam1/local = %d, want %d (saw %v)", got, sgCount, names)
	}
	if got := subgroups[0]; got != sgCount {
		t.Errorf("forwarded objects on subgroup 0 = %d, want %d (saw %v)", got, sgCount, subgroups)
	}
	// A clean, gapless subgroup must not report any stream reset: the reset
	// counters are only meaningful if the healthy path leaves them at zero.
	for _, c := range []relay.ResetCause{
		relay.ResetCauseGap,
		relay.ResetCauseDeliveryTimeout,
		relay.ResetCauseWriteError,
		relay.ResetCauseInboundReset,
	} {
		if got := rec.resetCount(c); got != 0 {
			t.Errorf("SubgroupStreamReset(%s) = %d on a clean subgroup, want 0", c, got)
		}
	}
	// Lifecycle opens: publisher + subscriber sessions, one subscription.
	if got := rec.sessionsOpened.Load(); got < 2 {
		t.Errorf("SessionOpened = %d, want >= 2", got)
	}
	if got := rec.subsOpened.Load(); got < 1 {
		t.Errorf("SubscriptionOpened = %d, want >= 1", got)
	}

	// FETCH the cached range and confirm FetchServed fires with the count.
	fetchReq, err := subSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     ns,
			Name:          name,
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 0, Object: uint64(sgCount - 1)},
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fetchReq.Close()
	drainFetch(t, subSess)

	if got := rec.fetchServed.Load(); got < 1 {
		t.Errorf("FetchServed calls = %d, want >= 1", got)
	}
	// The exact object count depends on FETCH End-Location semantics (covered
	// by the message/fetch tests); here we only assert the hook reported a
	// non-empty served range.
	if got := rec.fetchObjects.Load(); got < 1 {
		t.Errorf("FetchServed objects = %d, want >= 1", got)
	}

	// Tearing down must balance the gauges: every open is matched by a close.
	stop()
	waitFor(t, 2*time.Second, func() bool {
		return rec.sessionsClosed.Load() >= rec.sessionsOpened.Load() &&
			rec.subsClosed.Load() >= rec.subsOpened.Load()
	}, "session/subscription close gauges did not balance")
}

// drainFetch accepts the next data stream (expected to be the FETCH response)
// and reads it to completion.
func drainFetch(t *testing.T, sess *session.Session) {
	t.Helper()
	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream (fetch): %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingFetchStream", ds)
	}
	for {
		if _, err := fs.ReadObject(); err != nil {
			return
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !cond() {
		t.Fatal(msg)
	}
}
