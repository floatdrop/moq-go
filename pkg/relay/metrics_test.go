package relay_test

import (
	"bytes"
	"errors"
	"maps"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/loc"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

type receivedMetrics struct {
	relay.NopMetrics

	objects chan relay.ReceivedObject
	refs    chan relay.TrackRef
}

func (m *receivedMetrics) ObjectReceived(ref relay.TrackRef, o relay.ReceivedObject) {
	o.Properties = bytes.Clone(o.Properties)
	o.Payload = bytes.Clone(o.Payload)
	m.refs <- ref
	m.objects <- o
}

// Datagrams must be observable even without subscribers, with duplicate and
// unknown-alias copies excluded from the hook.
func TestMetricsDatagramReceived(t *testing.T) {
	t.Parallel()

	m := &receivedMetrics{objects: make(chan relay.ReceivedObject, 8), refs: make(chan relay.TrackRef, 8)}
	pub, teardown := connectRelay(t, relay.Config{Metrics: m})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(teardown) }
	defer stop()
	p, err := pub.Publish(t.Context(), &message.Publish{
		Namespace: ns("room", "speaker"), Name: []byte("audio"), TrackAlias: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	props := (&loc.Properties{Timestamp: 123456, HasTimestamp: true}).Encode()
	d := &message.ObjectDatagram{
		Type:       message.DatagramPropertiesBit | message.DatagramEndOfGroupBit,
		TrackAlias: 99, GroupID: 7, ObjectID: 3, Properties: props, ObjectPayload: []byte("opus"),
	}
	if err := pub.SendDatagram(d); err != nil {
		t.Fatal(err)
	}
	d.TrackAlias = p.TrackAlias()
	for range 2 {
		if err := pub.SendDatagram(d); err != nil {
			t.Fatal(err)
		}
	}
	if err := pub.SendDatagram(&message.ObjectDatagram{
		Type: message.DatagramStatusBit, TrackAlias: p.TrackAlias(), GroupID: 8,
		ObjectStatus: message.ObjectStatusEndOfGroup,
	}); err != nil {
		t.Fatal(err)
	}
	for i := range 2 {
		select {
		case o := <-m.objects:
			ref := <-m.refs
			if ref.Name != "audio" || ref.Leg != relay.LegLocal {
				t.Errorf("ref = %+v", ref)
			}
			if o.Name.Key() != (track.FullTrackName{Namespace: ns("room", "speaker"), Name: []byte("audio")}).Key() {
				t.Errorf("track = %+v", o.Name)
			}
			if o.HeaderAt.IsZero() || o.HeaderAt.After(time.Now()) {
				t.Errorf("receipt time = %v", o.HeaderAt)
			}
			if !o.Datagram || o.Subgroup != 0 {
				t.Errorf("datagram = %v, subgroup = %d", o.Datagram, o.Subgroup)
			}
			if i == 0 {
				if o.Group != 7 || o.Object != 3 || o.Status != message.ObjectStatusNormal || !o.EndOfGroup {
					t.Errorf("data metadata = %+v", o)
				}
				if !bytes.Equal(o.Properties, props) || string(o.Payload) != "opus" {
					t.Errorf("data properties = %x, payload = %q", o.Properties, o.Payload)
				}
			} else if o.Group != 8 || o.Object != 0 || o.Status != message.ObjectStatusEndOfGroup ||
				o.EndOfGroup || len(o.Payload) != 0 || len(o.Properties) != 0 {
				t.Errorf("status object = %+v", o)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ObjectReceived was not called for datagram")
		}
	}
	stop()
	if len(m.objects) != 0 {
		t.Fatal("duplicate or unknown-alias datagram reported")
	}
}

// The same hook exposes stream payloads and resolved absolute IDs, including
// objects with a nonzero delta and terminal markers with no payload.
func TestMetricsSubgroupReceived(t *testing.T) {
	t.Parallel()

	m := &receivedMetrics{objects: make(chan relay.ReceivedObject, 8), refs: make(chan relay.TrackRef, 8)}
	pub, teardown := connectRelay(t, relay.Config{Metrics: m})
	defer teardown()
	p, err := pub.Publish(t.Context(), &message.Publish{
		Namespace: ns("room", "speaker"), Name: []byte("audio"), TrackAlias: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	sg, err := p.OpenSubgroup(message.SubgroupHeader{
		GroupID: 7, SubgroupID: 2, SubgroupIDMode: message.SubgroupIDExplicit, Properties: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	props := (&loc.Properties{Timestamp: 123456, HasTimestamp: true}).Encode()
	for _, id := range []uint64{5, 9, 10} {
		o := &message.SubgroupObject{Payload: []byte("opus"), Properties: props}
		if id == 10 {
			o = &message.SubgroupObject{ObjectStatus: message.ObjectStatusEndOfGroup}
		}
		if err := sg.WriteObjectAt(id, o); err != nil {
			t.Fatal(err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatal(err)
	}
	for _, id := range []uint64{5, 9, 10} {
		select {
		case o := <-m.objects:
			if o.Datagram || o.EndOfGroup || o.Group != 7 || o.Subgroup != 2 || o.Object != id {
				t.Errorf("stream metadata = %+v, want object %d", o, id)
			}
			if id == 10 {
				if o.Status != message.ObjectStatusEndOfGroup || len(o.Payload) != 0 {
					t.Errorf("terminal object = %+v", o)
				}
			} else if o.Status != message.ObjectStatusNormal || string(o.Payload) != "opus" ||
				!bytes.Equal(o.Properties, props) {
				t.Errorf("data object = %+v", o)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("ObjectReceived was not called for stream object")
		}
	}
}

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
	mu           sync.Mutex
	lastReceived relay.ReceivedObject
	trackNames   map[string]int
	subgroups    map[uint64]int
	resets       map[relay.ResetCause]int
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

func (m *recordingMetrics) ObjectReceived(_ relay.TrackRef, o relay.ReceivedObject) {
	m.mu.Lock()
	m.lastReceived = relay.ReceivedObject{
		Name:       o.Name,
		Subgroup:   o.Subgroup,
		HeaderAt:   o.HeaderAt,
		Properties: bytes.Clone(o.Properties),
	}
	m.mu.Unlock()
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

// TestMetricsHooks: a publish, subscribe, forward and fetch flow fires each
// [relay.Metrics] hook with the expected counts, and the gauges balance after
// teardown.
func TestMetricsHooks(t *testing.T) {
	rec := &recordingMetrics{}
	pubSess, teardown := connectRelay(t, relay.Config{Metrics: rec})
	var stopOnce sync.Once
	stop := func() { stopOnce.Do(teardown) }
	defer stop()

	const alias = uint64(7)
	video := ns("video")
	name := []byte("cam1")

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: name, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
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
	// Each received object is told as the relay saw it: the track's full
	// name and a receipt time of the relay's clock during the test.
	rec.mu.Lock()
	last := rec.lastReceived
	rec.mu.Unlock()
	if string(last.Name.Name) != "cam1" || !last.Name.Namespace.HasPrefix(video) {
		t.Errorf("ObjectReceived told of track %s/%s, want video/cam1", last.Name.Namespace, last.Name.Name)
	}
	if last.HeaderAt.IsZero() || last.HeaderAt.After(time.Now()) {
		t.Errorf("ObjectReceived told of a receipt at %v", last.HeaderAt)
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
		relay.ResetCauseProperties,
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
		Namespace: video,
		Name:      name,
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: uint64(sgCount - 2)}),
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

// TestLegString and TestResetCauseString pin the metric label values, an
// external contract; an unrecognised value renders "unknown" to keep label
// cardinality bounded.
func TestLegString(t *testing.T) {
	t.Parallel()
	for leg, want := range map[relay.Leg]string{
		relay.LegLocal:     "local",
		relay.LegUpstream:  "upstream",
		relay.LegInProcess: "in_process",
		relay.Leg(99):      "unknown",
	} {
		if got := leg.String(); got != want {
			t.Errorf("Leg(%d).String() = %q, want %q", int(leg), got, want)
		}
	}
}

// TestResetCauseString: see TestLegString.
func TestResetCauseString(t *testing.T) {
	t.Parallel()
	for cause, want := range map[relay.ResetCause]string{
		relay.ResetCauseGap:             "gap",
		relay.ResetCauseDeliveryTimeout: "delivery_timeout",
		relay.ResetCauseTooFarBehind:    "too_far_behind",
		relay.ResetCauseExcessiveLoad:   "excessive_load",
		relay.ResetCauseInboundReset:    "inbound_reset",
		relay.ResetCauseWriteError:      "write_error",
		relay.ResetCauseProperties:      "properties",
		relay.ResetCause(99):            "unknown",
	} {
		if got := cause.String(); got != want {
			t.Errorf("ResetCause(%d).String() = %q, want %q", int(cause), got, want)
		}
	}
}
