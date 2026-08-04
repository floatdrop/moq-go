package discovery_test

import (
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

func ns(parts ...string) wire.TrackNamespace {
	out := make(wire.TrackNamespace, len(parts))
	for i, p := range parts {
		out[i] = []byte(p)
	}
	return out
}

func newKey(parts []string, name string) track.Key {
	return track.NewKey(ns(parts...), []byte(name))
}

// TestMemoryStore_PublishFindTrack pins the basic happy path: a track
// published once is returned by FindTrack across all RelayAddrs.
func TestMemoryStore_PublishFindTrack(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	key := newKey([]string{"video"}, "cam1")
	info := discovery.TrackInfo{
		Key:       key,
		FullName:  track.FullTrackName{Namespace: ns("video"), Name: []byte("cam1")},
		RelayAddr: "relay-A",
	}
	if err := s.PublishTrack(t.Context(), info); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	got, err := s.FindTrack(t.Context(), key)
	if err != nil {
		t.Fatalf("FindTrack: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("FindTrack returned %d entries, want 1", len(got))
	}
	if got[0].RelayAddr != "relay-A" {
		t.Errorf("RelayAddr = %q, want relay-A", got[0].RelayAddr)
	}
	if got[0].PublishedAt.IsZero() {
		t.Error("PublishedAt is zero; MemoryStore should stamp it")
	}
}

// TestMemoryStore_TrackMultipleRelays verifies that two relays
// hosting the same track produce two separate FindTrack entries.
func TestMemoryStore_TrackMultipleRelays(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	key := newKey([]string{"video"}, "cam1")
	for _, addr := range []string{"relay-A", "relay-B"} {
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: addr}); err != nil {
			t.Fatalf("PublishTrack %s: %v", addr, err)
		}
	}

	got, err := s.FindTrack(t.Context(), key)
	if err != nil {
		t.Fatalf("FindTrack: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FindTrack returned %d entries, want 2", len(got))
	}

	addrs := []string{got[0].RelayAddr, got[1].RelayAddr}
	sort.Strings(addrs)
	if addrs[0] != "relay-A" || addrs[1] != "relay-B" {
		t.Errorf("RelayAddrs = %v, want [relay-A relay-B]", addrs)
	}
}

// TestMemoryStore_PublishTrackIdempotent pins the contract that
// publishing the same (Key, RelayAddr) twice replaces — not duplicates.
func TestMemoryStore_PublishTrackIdempotent(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	key := newKey([]string{"video"}, "cam1")
	for i := range 3 {
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{
			Key:       key,
			RelayAddr: "relay-A",
		}); err != nil {
			t.Fatalf("PublishTrack #%d: %v", i, err)
		}
	}
	got, _ := s.FindTrack(t.Context(), key)
	if len(got) != 1 {
		t.Fatalf("FindTrack returned %d entries, want 1 (duplicate publishes must collapse)", len(got))
	}
}

// TestMemoryStore_UnpublishTrack covers removal + the unknown-entry
// silent-no-op contract.
func TestMemoryStore_UnpublishTrack(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	key := newKey([]string{"video"}, "cam1")
	_ = s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"})

	if err := s.UnpublishTrack(t.Context(), key, "relay-A"); err != nil {
		t.Fatalf("UnpublishTrack: %v", err)
	}
	got, _ := s.FindTrack(t.Context(), key)
	if len(got) != 0 {
		t.Fatalf("FindTrack after Unpublish returned %d entries, want 0", len(got))
	}

	// Idempotent: unknown unpublish is a silent no-op.
	if err := s.UnpublishTrack(t.Context(), key, "relay-A"); err != nil {
		t.Fatalf("second UnpublishTrack: %v", err)
	}
}

// TestMemoryStore_FindNamespacePrefixMatch pins §6.1 / §9.5 prefix
// matching semantics: a query for ["a","b","c"] matches stored
// prefixes ["a"], ["a","b"], ["a","b","c"]; ["a","b","c","d"] does NOT.
func TestMemoryStore_FindNamespacePrefixMatch(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	prefixes := [][]string{
		{"a"},
		{"a", "b"},
		{"a", "b", "c"},
		{"a", "b", "c", "d"}, // strictly longer — must NOT match
		{"x"},                // unrelated
	}
	for _, p := range prefixes {
		if err := s.PublishNamespace(t.Context(), discovery.NamespaceInfo{
			Prefix:    ns(p...),
			RelayAddr: "relay-A",
		}); err != nil {
			t.Fatalf("PublishNamespace %v: %v", p, err)
		}
	}

	got, err := s.FindNamespace(t.Context(), ns("a", "b", "c"))
	if err != nil {
		t.Fatalf("FindNamespace: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("FindNamespace returned %d entries, want 3 (a, a/b, a/b/c)", len(got))
	}

	// Spot-check that ["a","b","c","d"] is excluded.
	for _, g := range got {
		if len(g.Prefix) == 4 {
			t.Fatalf(
				"FindNamespace returned the strictly-longer prefix %v; spec says only same-or-shorter prefixes match",
				g.Prefix,
			)
		}
	}
}

// TestMemoryStore_FindNamespacesUnder pins the descendant direction: a query
// returns advertisements whose Prefix extends (is at or below) the query, the
// inverse of FindNamespace.
func TestMemoryStore_FindNamespacesUnder(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	prefixes := [][]string{
		{"a"},
		{"a", "b"},
		{"a", "b", "c"},
		{"x"}, // unrelated — must NOT match a query under ["a"]
	}
	for _, p := range prefixes {
		if err := s.PublishNamespace(
			t.Context(),
			discovery.NamespaceInfo{Prefix: ns(p...), RelayAddr: "relay-A"},
		); err != nil {
			t.Fatalf("PublishNamespace %v: %v", p, err)
		}
	}

	// Query ["a"] matches ["a"], ["a","b"], ["a","b","c"] but not ["x"].
	got, err := s.FindNamespacesUnder(t.Context(), ns("a"))
	if err != nil {
		t.Fatalf("FindNamespacesUnder: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("FindNamespacesUnder(a) returned %d entries, want 3", len(got))
	}
	for _, g := range got {
		if len(g.Prefix) == 0 || string(g.Prefix[0]) != "a" {
			t.Errorf("unexpected non-descendant %v in results", g.Prefix)
		}
	}

	// A deeper query narrows to itself and below.
	got, err = s.FindNamespacesUnder(t.Context(), ns("a", "b"))
	if err != nil {
		t.Fatalf("FindNamespacesUnder(a,b): %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("FindNamespacesUnder(a,b) returned %d entries, want 2 (a/b, a/b/c)", len(got))
	}

	// The empty prefix matches everything.
	got, err = s.FindNamespacesUnder(t.Context(), ns())
	if err != nil {
		t.Fatalf("FindNamespacesUnder(): %v", err)
	}
	if len(got) != len(prefixes) {
		t.Fatalf("FindNamespacesUnder() returned %d entries, want %d (all)", len(got), len(prefixes))
	}
}

// TestMemoryStore_WatchTracksReceivesEvents pins the Watch contract:
// every Publish + Unpublish triggers a TrackEvent on the channel.
func TestMemoryStore_WatchTracksReceivesEvents(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := s.WatchTracks(ctx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}

	key := newKey([]string{"video"}, "cam1")
	_ = s.PublishTrack(ctx, discovery.TrackInfo{Key: key, RelayAddr: "relay-A"})
	_ = s.UnpublishTrack(ctx, key, "relay-A")

	first, ok := receiveTrack(ch, 2*time.Second)
	if !ok {
		t.Fatal("did not receive publish event")
	}
	if first.Op != discovery.OpPublish {
		t.Errorf("first event Op = %v, want publish", first.Op)
	}
	if first.Info.RelayAddr != "relay-A" {
		t.Errorf("first event RelayAddr = %q, want relay-A", first.Info.RelayAddr)
	}

	second, ok := receiveTrack(ch, 2*time.Second)
	if !ok {
		t.Fatal("did not receive unpublish event")
	}
	if second.Op != discovery.OpUnpublish {
		t.Errorf("second event Op = %v, want unpublish", second.Op)
	}
}

// TestMemoryStore_WatchTracksClosedOnCtxCancel verifies that cancelling
// the watch ctx closes the channel and unhooks the watcher (so subsequent
// publishes don't try to deliver to it).
func TestMemoryStore_WatchTracksClosedOnCtxCancel(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	ctx, cancel := context.WithCancel(t.Context())
	ch, err := s.WatchTracks(ctx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}

	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, open := <-ch:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("watch channel did not close after ctx cancel")
		}
	}
}

// TestMemoryStore_ClosedRejectsOperations verifies that after Close,
// every method returns ErrClosed and active watchers see their channels
// closed.
func TestMemoryStore_ClosedRejectsOperations(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()

	ch, _ := s.WatchTracks(t.Context())

	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Watch channel is closed.
	select {
	case _, open := <-ch:
		if open {
			t.Error("watch channel returned a value after Close instead of closing")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("watch channel not closed after Close")
	}

	// All ops now return ErrClosed.
	key := newKey([]string{"x"}, "y")
	if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key}); !errors.Is(err, discovery.ErrClosed) {
		t.Errorf("PublishTrack after Close = %v, want ErrClosed", err)
	}
	if _, err := s.FindTrack(t.Context(), key); !errors.Is(err, discovery.ErrClosed) {
		t.Errorf("FindTrack after Close = %v, want ErrClosed", err)
	}
	if _, err := s.WatchTracks(t.Context()); !errors.Is(err, discovery.ErrClosed) {
		t.Errorf("WatchTracks after Close = %v, want ErrClosed", err)
	}

	// Second Close is a no-op.
	if err := s.Close(); err != nil {
		t.Errorf("second Close = %v, want nil", err)
	}
}

// TestMemoryStore_WatchNamespacesReceivesEvents — same contract as
// WatchTracks but on the namespace channel.
func TestMemoryStore_WatchNamespacesReceivesEvents(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore()
	defer s.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch, err := s.WatchNamespaces(ctx)
	if err != nil {
		t.Fatalf("WatchNamespaces: %v", err)
	}

	_ = s.PublishNamespace(ctx, discovery.NamespaceInfo{Prefix: ns("chat"), RelayAddr: "relay-A"})
	_ = s.UnpublishNamespace(ctx, ns("chat"), "relay-A")

	first, ok := receiveNamespace(ch, 2*time.Second)
	if !ok {
		t.Fatal("did not receive publish event")
	}
	if first.Op != discovery.OpPublish {
		t.Errorf("first Op = %v, want publish", first.Op)
	}
	second, ok := receiveNamespace(ch, 2*time.Second)
	if !ok {
		t.Fatal("did not receive unpublish event")
	}
	if second.Op != discovery.OpUnpublish {
		t.Errorf("second Op = %v, want unpublish", second.Op)
	}
}

// TestMemoryStore_SlowWatcherDoesNotBlockPublisher verifies the
// non-blocking-fanout contract: a watcher that doesn't drain its
// channel cannot stall the publish path.
func TestMemoryStore_SlowWatcherDoesNotBlockPublisher(t *testing.T) {
	t.Parallel()
	s := discovery.NewMemoryStore(discovery.WithWatchBufferSize(2))
	defer s.Close()

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	_, err := s.WatchTracks(ctx) // never drained
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}

	// Issue more publishes than the buffer can hold. None of these
	// must block; the watcher drops the excess silently.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			_ = s.PublishTrack(ctx, discovery.TrackInfo{
				Key:       newKey([]string{"x"}, "y"),
				RelayAddr: "addr",
			})
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("PublishTrack blocked on slow watcher")
	}
}

// TestMemoryStore_WatchSeedsSnapshot pins the snapshot-then-follow contract: a
// watch registered against a non-empty store first receives the current
// advertisements as OpPublish events, then subsequent live changes.
func TestMemoryStore_WatchSeedsSnapshot(t *testing.T) {
	s := discovery.NewMemoryStore()
	defer s.Close()
	ctx := t.Context()

	// Seed state BEFORE the watch exists.
	key := newKey([]string{"video"}, "cam1")
	for _, addr := range []string{"relay-A", "relay-B"} {
		if err := s.PublishTrack(ctx, discovery.TrackInfo{Key: key, RelayAddr: addr}); err != nil {
			t.Fatalf("PublishTrack %s: %v", addr, err)
		}
	}
	if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{Prefix: ns("video"), RelayAddr: "relay-A"}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	trackCh, err := s.WatchTracks(ctx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}
	nsCh, err := s.WatchNamespaces(ctx)
	if err != nil {
		t.Fatalf("WatchNamespaces: %v", err)
	}

	// Snapshot: both track advertisements, as OpPublish, in some order.
	seen := map[string]bool{}
	for range 2 {
		ev, ok := receiveTrack(trackCh, 2*time.Second)
		if !ok {
			t.Fatal("timed out waiting for track snapshot event")
		}
		if ev.Op != discovery.OpPublish {
			t.Errorf("snapshot event Op = %v, want publish", ev.Op)
		}
		seen[ev.Info.RelayAddr] = true
	}
	if !seen["relay-A"] || !seen["relay-B"] {
		t.Errorf("track snapshot addrs = %v, want relay-A and relay-B", seen)
	}

	nsEv, ok := receiveNamespace(nsCh, 2*time.Second)
	if !ok {
		t.Fatal("timed out waiting for namespace snapshot event")
	}
	if nsEv.Op != discovery.OpPublish || nsEv.Info.RelayAddr != "relay-A" {
		t.Errorf("namespace snapshot = %+v, want publish/relay-A", nsEv)
	}

	// Follow: a live publish after the snapshot is delivered too.
	key2 := newKey([]string{"video"}, "cam2")
	if err := s.PublishTrack(ctx, discovery.TrackInfo{Key: key2, RelayAddr: "relay-A"}); err != nil {
		t.Fatalf("live PublishTrack: %v", err)
	}
	live, ok := receiveTrack(trackCh, 2*time.Second)
	if !ok {
		t.Fatal("timed out waiting for live track event")
	}
	if live.Op != discovery.OpPublish || live.Info.Key != key2 {
		t.Errorf("live event = %+v, want publish of cam2", live)
	}
}

func receiveTrack(ch <-chan discovery.TrackEvent, d time.Duration) (discovery.TrackEvent, bool) {
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(d):
		return discovery.TrackEvent{}, false
	}
}

func receiveNamespace(ch <-chan discovery.NamespaceEvent, d time.Duration) (discovery.NamespaceEvent, bool) {
	select {
	case ev, ok := <-ch:
		return ev, ok
	case <-time.After(d):
		return discovery.NamespaceEvent{}, false
	}
}

// TestMemoryStoreWithdraw pins the [discovery.DiscoveryStore.Withdraw] contract
// on the reference implementation: it removes only the named relay's
// advertisements, tells watchers about each removal, and is terminal for that
// address so a late publisher cannot re-advertise a relay that is draining.
func TestMemoryStoreWithdraw(t *testing.T) {
	s := discovery.NewMemoryStore()
	defer s.Close()

	const (
		leaving = "relay-a:4433"
		staying = "relay-b:4433"
	)
	key := track.Key{}
	prefix := ns("withdraw")

	events, err := s.WatchTracks(t.Context())
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}

	for _, addr := range []string{leaving, staying} {
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: addr}); err != nil {
			t.Fatalf("PublishTrack(%s): %v", addr, err)
		}
		if err := s.PublishNamespace(t.Context(),
			discovery.NamespaceInfo{Prefix: prefix, RelayAddr: addr}); err != nil {
			t.Fatalf("PublishNamespace(%s): %v", addr, err)
		}
	}
	// Drain the two publish events so the next receive is the withdrawal's.
	for range 2 {
		<-events
	}

	if err := s.Withdraw(t.Context(), leaving); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	// Watchers must see the removal, exactly as they would an Unpublish.
	select {
	case ev := <-events:
		if ev.Op != discovery.OpUnpublish {
			t.Errorf("event Op = %v, want OpUnpublish", ev.Op)
		}
		if ev.Info.RelayAddr != leaving {
			t.Errorf("event RelayAddr = %q, want %q", ev.Info.RelayAddr, leaving)
		}
	case <-time.After(time.Second):
		t.Fatal("Withdraw emitted no track event")
	}

	// Only the withdrawn relay's entries are gone.
	tracks, err := s.FindTrack(t.Context(), key)
	if err != nil {
		t.Fatalf("FindTrack: %v", err)
	}
	if len(tracks) != 1 || tracks[0].RelayAddr != staying {
		t.Errorf("FindTrack = %+v, want only %q", tracks, staying)
	}
	spaces, err := s.FindNamespace(t.Context(), prefix)
	if err != nil {
		t.Fatalf("FindNamespace: %v", err)
	}
	if len(spaces) != 1 || spaces[0].RelayAddr != staying {
		t.Errorf("FindNamespace = %+v, want only %q", spaces, staying)
	}

	// Terminal for the withdrawn address: a late publisher must not restore it.
	if err := s.PublishTrack(t.Context(),
		discovery.TrackInfo{Key: key, RelayAddr: leaving}); !errors.Is(err, discovery.ErrWithdrawn) {
		t.Errorf("PublishTrack after Withdraw = %v, want ErrWithdrawn", err)
	}
	if err := s.PublishNamespace(t.Context(),
		discovery.NamespaceInfo{Prefix: prefix, RelayAddr: leaving}); !errors.Is(err, discovery.ErrWithdrawn) {
		t.Errorf("PublishNamespace after Withdraw = %v, want ErrWithdrawn", err)
	}
	if tracks, _ := s.FindTrack(t.Context(), key); len(tracks) != 1 {
		t.Errorf("a withdrawn relay re-advertised itself: %+v", tracks)
	}

	// The other relay is unaffected, and a second Withdraw is a no-op.
	if err := s.PublishTrack(t.Context(),
		discovery.TrackInfo{Key: key, RelayAddr: staying}); err != nil {
		t.Errorf("PublishTrack(%s) after another relay withdrew: %v", staying, err)
	}
	if err := s.Withdraw(t.Context(), leaving); err != nil {
		t.Errorf("second Withdraw = %v, want nil (idempotent)", err)
	}
}
