package session_test

import (
	"context"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestDemuxRoutesStreams checks that Demux dispatches subgroup streams by Track
// Alias and FETCH streams by Request ID, honours a handler registered after Run
// has already started, and routes a FETCH stream nobody claimed to OnUnknown.
//
// A subgroup stream for an unregistered alias is NOT unknown — see
// TestDemuxParksStreamsUntilAliasKnown.
func TestDemuxRoutesStreams(t *testing.T) {
	t.Parallel()
	pub, sub := openPair(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type ev struct {
		kind string
		id   uint64
	}
	events := make(chan ev, 8)
	firstSeen := make(chan struct{}, 1)

	d := session.NewDemux()
	d.HandleTrack(42, func(s *session.IncomingSubgroupStream) {
		_, _ = io.ReadAll(s)
		events <- ev{"subgroup", s.Header.TrackAlias}
		firstSeen <- struct{}{}
	})
	d.HandleFetch(7, func(s *session.IncomingFetchStream) {
		_, _ = io.ReadAll(s)
		events <- ev{"fetch", s.Header.RequestID}
	})
	d.OnUnknown(func(ds session.DataStream) {
		_, _ = io.ReadAll(ds)
		events <- ev{"unknown", 0}
	})

	runErr := make(chan error, 1)
	go func() { runErr <- d.Run(ctx, sub) }()

	// 1. A subgroup on the registered alias 42.
	openSubgroup(t, pub, 42)
	<-firstSeen // ensure Run is live before late registration

	// 2. Register alias 43 *after* Run started, then push a stream for it.
	d.HandleTrack(43, func(s *session.IncomingSubgroupStream) {
		_, _ = io.ReadAll(s)
		events <- ev{"subgroup", s.Header.TrackAlias}
	})
	openSubgroup(t, pub, 43)

	// 3. A FETCH stream answering Request ID 7.
	fs, err := pub.OpenFetchStream(message.FetchHeader{RequestID: 7})
	if err != nil {
		t.Fatalf("OpenFetchStream: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("fetch Close: %v", err)
	}

	// 4. A FETCH stream nobody registered for → OnUnknown. (A subgroup
	//    stream for an unknown alias would be parked, not unknown.)
	orphan, err := pub.OpenFetchStream(message.FetchHeader{RequestID: 99})
	if err != nil {
		t.Fatalf("OpenFetchStream: %v", err)
	}
	if err := orphan.Close(); err != nil {
		t.Fatalf("orphan fetch Close: %v", err)
	}

	got := collectEvents(t, events, 4)
	cancel()

	want := []ev{
		{"subgroup", 42},
		{"subgroup", 43},
		{"fetch", 7},
		{"unknown", 0},
	}
	for _, w := range want {
		if !slices.Contains(got, w) {
			t.Errorf("missing event %+v; got %+v", w, got)
		}
	}
}

// TestIncomingSubgroupTrackKeyResolvesLive verifies the §11.1 fix: TrackKey
// resolves against the inbound alias registry at call time, so a stream
// accepted before its alias is registered resolves once it is.
func TestIncomingSubgroupTrackKeyResolvesLive(t *testing.T) {
	t.Parallel()
	pub, sub := openPair(t)

	const alias = 55
	key := track.NewKey(wire.Namespace("video"), []byte("cam1"))

	accepted := make(chan *session.IncomingSubgroupStream, 1)
	go func() {
		ds, err := sub.AcceptDataStream(context.Background())
		if err != nil {
			return
		}
		if s, ok := ds.(*session.IncomingSubgroupStream); ok {
			accepted <- s
		}
	}()

	openSubgroup(t, pub, alias)

	var s *session.IncomingSubgroupStream
	select {
	case s = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("data stream not accepted")
	}

	// Before registration the alias is unknown.
	if _, ok := s.TrackKey(); ok {
		t.Fatal("TrackKey resolved before the alias was registered")
	}

	// Registering the alias now must make the *already-accepted* stream resolve.
	if err := sub.RegisterInboundTrackAlias(alias, key); err != nil {
		t.Fatalf("RegisterInboundTrackAlias: %v", err)
	}
	gotKey, ok := s.TrackKey()
	if !ok {
		t.Fatal("TrackKey did not resolve after the alias was registered")
	}
	if gotKey != key {
		t.Errorf("TrackKey = %+v, want %+v", gotKey, key)
	}
}

// openSubgroup opens a header-only subgroup stream for alias and FINs it.
func openSubgroup(t *testing.T, s *session.Session, alias uint64) {
	t.Helper()
	out, err := s.OpenSubgroup(message.SubgroupHeader{TrackAlias: alias})
	if err != nil {
		t.Fatalf("OpenSubgroup(alias=%d): %v", alias, err)
	}
	if err := out.Close(); err != nil {
		t.Fatalf("subgroup Close(alias=%d): %v", alias, err)
	}
}

// collectEvents reads exactly n events or fails after a timeout.
func collectEvents[T any](t *testing.T, ch <-chan T, n int) []T {
	t.Helper()
	out := make([]T, 0, n)
	for range n {
		select {
		case e := <-ch:
			out = append(out, e)
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d/%d; got %+v", len(out)+1, n, out)
		}
	}
	return out
}

// TestDemuxParksStreamsUntilAliasKnown pins the reordering allowance in
// §11.4.2: "if an endpoint receives a subgroup with an unknown Track Alias, it
// MAY abandon the stream, or choose to buffer it for a brief period to handle
// reordering with the control message that establishes the Track Alias".
//
// Demux buffers. It has to: a subscriber learns a track's alias from its
// SUBSCRIBE_OK, and a publisher may push the track's first subgroup streams
// before that reply has been read, so the opening Groups of a live broadcast
// routinely land with no handler registered. Abandoning them loses media that
// was delivered perfectly well.
func TestDemuxParksStreamsUntilAliasKnown(t *testing.T) {
	t.Parallel()
	pub, sub := openPair(t)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	const alias = 77
	unknown := make(chan struct{}, 4)

	d := session.NewDemux()
	d.OnUnknown(func(ds session.DataStream) {
		_, _ = io.ReadAll(ds)
		unknown <- struct{}{}
	})
	go func() { _ = d.Run(ctx, sub) }()

	// Two Groups arrive before anything is registered to read them.
	openSubgroup(t, pub, alias)
	openSubgroup(t, pub, alias)

	// Neither may be treated as unwanted.
	select {
	case <-unknown:
		t.Fatal("a subgroup stream for an unregistered alias went to OnUnknown; " +
			"it should have been parked until the alias was claimed")
	case <-time.After(200 * time.Millisecond):
	}

	// Registering the alias releases them, in arrival order, and says how
	// many were waiting.
	seen := make(chan uint64, 4)
	released := d.HandleTrack(alias, func(s *session.IncomingSubgroupStream) {
		_, _ = io.ReadAll(s)
		seen <- s.Header.TrackAlias
	})
	if released != 2 {
		t.Errorf("HandleTrack released %d parked streams, want 2", released)
	}
	for i := range 2 {
		select {
		case got := <-seen:
			if got != alias {
				t.Errorf("released stream %d had alias %d, want %d", i, got, alias)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("parked stream %d was never handed to the handler", i)
		}
	}
}

// parkLimitForTest mirrors the unexported parkLimit in demux.go. Kept here
// rather than exported: the bound is an implementation choice, not API.
const parkLimitForTest = 8

// TestDemuxParkingIsBounded covers what stops parked streams accumulating.
//
// A parked stream is header-parsed and then left unread, so its body sits in
// the transport's receive buffers holding connection-level flow control. §11.4.2
// allows buffering "for a brief period" and abandoning otherwise; these bounds
// are how Demux abandons.
func TestDemuxParkingIsBounded(t *testing.T) {
	t.Parallel()

	t.Run("past parkLimit the oldest is reset", func(t *testing.T) {
		t.Parallel()
		pub, sub := openPair(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const alias = 21
		d := session.NewDemux()
		go func() { _ = d.Run(ctx, sub) }()

		// One more than the bound.
		for range parkLimitForTest + 1 {
			openSubgroup(t, pub, alias)
		}
		// Let every one of them be dispatched and parked before claiming
		// the alias; a stream still in flight would be handed straight to
		// the handler instead and would not count as released.
		time.Sleep(300 * time.Millisecond)

		released := d.HandleTrack(alias, func(*session.IncomingSubgroupStream) {})
		if released > parkLimitForTest {
			t.Errorf("HandleTrack released %d parked streams, want at most %d; "+
				"the oldest should have been reset once the bound was passed",
				released, parkLimitForTest)
		}
	})

	t.Run("Run releases whatever is still parked when it returns", func(t *testing.T) {
		t.Parallel()
		pub, sub := openPair(t)
		ctx, cancel := context.WithCancel(t.Context())

		const alias = 23
		d := session.NewDemux()
		runDone := make(chan struct{})
		go func() { _ = d.Run(ctx, sub); close(runDone) }()

		openSubgroup(t, pub, alias)
		time.Sleep(200 * time.Millisecond) // let it be dispatched and parked

		// Nothing ever claimed the alias. Stopping dispatch must reset and
		// forget it, rather than leave it pinning flow control for the life
		// of the session; claiming the alias afterwards must find nothing.
		cancel()
		<-runDone

		if released := d.HandleTrack(alias, func(*session.IncomingSubgroupStream) {}); released != 0 {
			t.Errorf("after Run returned, HandleTrack still released %d parked streams, want 0",
				released)
		}
	})

	t.Run("a retired alias is discarded, not parked", func(t *testing.T) {
		t.Parallel()
		pub, sub := openPair(t)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		const alias = 22
		d := session.NewDemux()
		go func() { _ = d.Run(ctx, sub) }()

		// Register then unregister: the subscription has been cancelled, so
		// §11.1 wants late Objects discarded quickly rather than treated as
		// belonging to an unknown alias.
		d.HandleTrack(alias, func(*session.IncomingSubgroupStream) {})
		d.HandleTrack(alias, nil)

		openSubgroup(t, pub, alias)
		time.Sleep(200 * time.Millisecond)

		if released := d.HandleTrack(alias, func(*session.IncomingSubgroupStream) {}); released != 0 {
			t.Errorf("re-registering a retired alias released %d parked streams, want 0; "+
				"streams arriving after cancellation were buffered instead of discarded", released)
		}
	})
}
