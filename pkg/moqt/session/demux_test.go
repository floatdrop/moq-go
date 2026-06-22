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
// Alias and FETCH streams by Request ID, routes an unmatched alias to OnUnknown,
// and honours a handler registered after Run has already started.
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

	// 4. An unregistered alias 99 → OnUnknown.
	openSubgroup(t, pub, 99)

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
