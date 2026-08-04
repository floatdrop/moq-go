package nats_test

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	natsstore "github.com/floatdrop/moq-go/pkg/relay/discovery/nats"
)

// One embedded nats-server + one connection back every subtest; each Store gets
// a unique bucket so subtests never see each other's advertisements. This keeps
// the server bootstrap to once per package run.
func newStoreSet(t *testing.T) func(bucket string, opts ...natsstore.Option) *natsstore.Store {
	t.Helper()
	url := startEmbeddedNATS(t)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}

	return func(bucket string, opts ...natsstore.Option) *natsstore.Store {
		all := append([]natsstore.Option{natsstore.WithBucket(bucket)}, opts...)
		s, err := natsstore.New(t.Context(), js, all...)
		if err != nil {
			t.Fatalf("new store %q: %v", bucket, err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
}

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

func TestNATSStore(t *testing.T) {
	store := newStoreSet(t)

	t.Run("PublishFindTrack", func(t *testing.T) {
		s := store("pubfind")
		key := newKey([]string{"video"}, "cam1")
		info := discovery.TrackInfo{
			Key:        key,
			FullName:   track.FullTrackName{Namespace: ns("video"), Name: []byte("cam1")},
			Properties: []byte{0x01, 0x02, 0x03},
			RelayAddr:  "relay-A",
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
			t.Error("PublishedAt is zero; store should stamp it")
		}
		if !bytes.Equal(got[0].Properties, []byte{0x01, 0x02, 0x03}) {
			t.Errorf("Properties = %v, want [1 2 3]", got[0].Properties)
		}
		if got[0].Key != key {
			t.Error("recomputed Key does not match original")
		}
		if !bytes.Equal(got[0].FullName.Name, []byte("cam1")) {
			t.Errorf("FullName.Name = %q, want cam1", got[0].FullName.Name)
		}
	})

	t.Run("TrackMultipleRelays", func(t *testing.T) {
		s := store("multirelay")
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
	})

	t.Run("PublishTrackIdempotent", func(t *testing.T) {
		s := store("idem")
		key := newKey([]string{"video"}, "cam1")
		for i := range 3 {
			if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
				t.Fatalf("PublishTrack #%d: %v", i, err)
			}
		}
		got, _ := s.FindTrack(t.Context(), key)
		if len(got) != 1 {
			t.Fatalf("FindTrack returned %d entries, want 1 (duplicates must collapse)", len(got))
		}
	})

	t.Run("UnpublishTrack", func(t *testing.T) {
		s := store("unpub")
		key := newKey([]string{"video"}, "cam1")
		_ = s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key, RelayAddr: "relay-A"})
		if err := s.UnpublishTrack(t.Context(), key, "relay-A"); err != nil {
			t.Fatalf("UnpublishTrack: %v", err)
		}
		got, _ := s.FindTrack(t.Context(), key)
		if len(got) != 0 {
			t.Fatalf("FindTrack after Unpublish returned %d entries, want 0", len(got))
		}
		// Unknown unpublish is a silent no-op.
		if err := s.UnpublishTrack(t.Context(), key, "relay-A"); err != nil {
			t.Fatalf("second UnpublishTrack: %v", err)
		}
	})

	t.Run("EmptyRelayAddr", func(t *testing.T) {
		// The empty address (single-relay default) must round-trip: it hits the
		// addrToken "_" sentinel path in the key encoding.
		s := store("emptyaddr")
		key := newKey([]string{"video"}, "cam1")
		if err := s.PublishTrack(t.Context(), discovery.TrackInfo{Key: key}); err != nil {
			t.Fatalf("PublishTrack empty addr: %v", err)
		}
		got, err := s.FindTrack(t.Context(), key)
		if err != nil {
			t.Fatalf("FindTrack: %v", err)
		}
		if len(got) != 1 || got[0].RelayAddr != "" {
			t.Fatalf("FindTrack = %+v, want one entry with empty RelayAddr", got)
		}
		if err := s.UnpublishTrack(t.Context(), key, ""); err != nil {
			t.Fatalf("UnpublishTrack empty addr: %v", err)
		}
		got, _ = s.FindTrack(t.Context(), key)
		if len(got) != 0 {
			t.Fatalf("FindTrack after Unpublish = %+v, want empty", got)
		}
	})

	t.Run("FindNamespacePrefixMatch", func(t *testing.T) {
		s := store("nsprefix")
		prefixes := [][]string{
			{"a"},
			{"a", "b"},
			{"a", "b", "c"},
			{"a", "b", "c", "d"}, // strictly longer — must NOT match
			{"x"},                // unrelated
		}
		for _, p := range prefixes {
			if err := s.PublishNamespace(
				t.Context(),
				discovery.NamespaceInfo{Prefix: ns(p...), RelayAddr: "relay-A"},
			); err != nil {
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
		for _, g := range got {
			if len(g.Prefix) == 4 {
				t.Fatalf("returned strictly-longer prefix %v; only same-or-shorter prefixes match", g.Prefix)
			}
		}
	})

	t.Run("FindNamespacesUnderDescendantMatch", func(t *testing.T) {
		s := store("nsunder")
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
		got, err := s.FindNamespacesUnder(t.Context(), ns("a"))
		if err != nil {
			t.Fatalf("FindNamespacesUnder: %v", err)
		}
		if len(got) != 3 {
			t.Fatalf("FindNamespacesUnder(a) returned %d entries, want 3 (a, a/b, a/b/c)", len(got))
		}
		for _, g := range got {
			if len(g.Prefix) == 0 || string(g.Prefix[0]) != "a" {
				t.Errorf("unexpected non-descendant %v in results", g.Prefix)
			}
		}
	})

	t.Run("FindNamespaceEmptyQueryMatchesRoot", func(t *testing.T) {
		s := store("nsroot")
		// A zero-length advertised prefix (SUBSCRIBE_NAMESPACE with no filter)
		// must match any query, and be found by an empty query too.
		if err := s.PublishNamespace(
			t.Context(),
			discovery.NamespaceInfo{Prefix: ns(), RelayAddr: "relay-A"},
		); err != nil {
			t.Fatalf("PublishNamespace root: %v", err)
		}
		got, err := s.FindNamespace(t.Context(), ns("anything", "deep"))
		if err != nil {
			t.Fatalf("FindNamespace: %v", err)
		}
		if len(got) != 1 || len(got[0].Prefix) != 0 {
			t.Fatalf("root prefix not matched by nested query: got %+v", got)
		}
	})

	t.Run("WatchTracksReceivesEvents", func(t *testing.T) {
		s := store("watchtr")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ch, err := s.WatchTracks(ctx)
		if err != nil {
			t.Fatalf("WatchTracks: %v", err)
		}

		key := newKey([]string{"video"}, "cam1")
		if err := s.PublishTrack(ctx, discovery.TrackInfo{
			Key:       key,
			FullName:  track.FullTrackName{Namespace: ns("video"), Name: []byte("cam1")},
			RelayAddr: "relay-A",
		}); err != nil {
			t.Fatalf("PublishTrack: %v", err)
		}
		first := receiveTrack(t, ch)
		if first.Op != discovery.OpPublish {
			t.Errorf("first event Op = %v, want publish", first.Op)
		}
		if first.Info.RelayAddr != "relay-A" {
			t.Errorf("first event RelayAddr = %q, want relay-A", first.Info.RelayAddr)
		}

		if err := s.UnpublishTrack(ctx, key, "relay-A"); err != nil {
			t.Fatalf("UnpublishTrack: %v", err)
		}
		// The delete marker carries no value; reconstruction relies on the
		// watcher's last-value cache. Assert the unpublish still resolves the key.
		second := receiveTrack(t, ch)
		if second.Op != discovery.OpUnpublish {
			t.Errorf("second event Op = %v, want unpublish", second.Op)
		}
		if second.Info.Key != key {
			t.Error("unpublish event Key mismatch (last-value reconstruction failed)")
		}
	})

	t.Run("WatchNamespacesReceivesEvents", func(t *testing.T) {
		s := store("watchns")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		ch, err := s.WatchNamespaces(ctx)
		if err != nil {
			t.Fatalf("WatchNamespaces: %v", err)
		}
		if err := s.PublishNamespace(
			ctx,
			discovery.NamespaceInfo{Prefix: ns("chat"), RelayAddr: "relay-A"},
		); err != nil {
			t.Fatalf("PublishNamespace: %v", err)
		}
		first := receiveNamespace(t, ch)
		if first.Op != discovery.OpPublish {
			t.Errorf("first Op = %v, want publish", first.Op)
		}
		if err := s.UnpublishNamespace(ctx, ns("chat"), "relay-A"); err != nil {
			t.Fatalf("UnpublishNamespace: %v", err)
		}
		second := receiveNamespace(t, ch)
		if second.Op != discovery.OpUnpublish {
			t.Errorf("second Op = %v, want unpublish", second.Op)
		}
	})

	t.Run("WatchSeedsSnapshotThenFollows", func(t *testing.T) {
		s := store("watchsnap")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		// Seed two advertisements of the same track BEFORE the watch exists.
		key := newKey([]string{"video"}, "cam1")
		for _, addr := range []string{"relay-A", "relay-B"} {
			if err := s.PublishTrack(ctx, discovery.TrackInfo{
				Key:       key,
				FullName:  track.FullTrackName{Namespace: ns("video"), Name: []byte("cam1")},
				RelayAddr: addr,
			}); err != nil {
				t.Fatalf("seed PublishTrack %s: %v", addr, err)
			}
		}

		ch, err := s.WatchTracks(ctx)
		if err != nil {
			t.Fatalf("WatchTracks: %v", err)
		}

		// Snapshot: both seeded advertisements arrive as OpPublish, any order.
		seen := map[string]bool{}
		for range 2 {
			ev := receiveTrack(t, ch)
			if ev.Op != discovery.OpPublish {
				t.Errorf("snapshot event Op = %v, want publish", ev.Op)
			}
			seen[ev.Info.RelayAddr] = true
		}
		if !seen["relay-A"] || !seen["relay-B"] {
			t.Errorf("snapshot addrs = %v, want relay-A and relay-B", seen)
		}

		// Follow: a publish after the snapshot streams through gaplessly.
		key2 := newKey([]string{"video"}, "cam2")
		if err := s.PublishTrack(ctx, discovery.TrackInfo{
			Key:       key2,
			FullName:  track.FullTrackName{Namespace: ns("video"), Name: []byte("cam2")},
			RelayAddr: "relay-A",
		}); err != nil {
			t.Fatalf("live PublishTrack: %v", err)
		}
		live := receiveTrack(t, ch)
		if live.Op != discovery.OpPublish || live.Info.Key != key2 {
			t.Errorf("live event = %+v, want publish of cam2", live)
		}
	})

	t.Run("WatchClosedOnCtxCancel", func(t *testing.T) {
		s := store("watchcancel")
		ctx, cancel := context.WithCancel(t.Context())
		ch, err := s.WatchTracks(ctx)
		if err != nil {
			t.Fatalf("WatchTracks: %v", err)
		}
		cancel()
		deadline := time.After(5 * time.Second)
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
	})

	t.Run("ClosedRejectsOperations", func(t *testing.T) {
		// Own the connection here so Close semantics are self-contained.
		url := startEmbeddedNATS(t)
		s, err := natsstore.Open(t.Context(), url, natsstore.WithBucket("closed"))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		ch, _ := s.WatchTracks(t.Context())
		if err := s.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		select {
		case _, open := <-ch:
			if open {
				t.Error("watch channel yielded a value after Close instead of closing")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("watch channel not closed after Close")
		}
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
		if err := s.Close(); err != nil {
			t.Errorf("second Close = %v, want nil", err)
		}
	})
}

func receiveTrack(t *testing.T, ch <-chan discovery.TrackEvent) discovery.TrackEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed before event")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for track event")
		return discovery.TrackEvent{}
	}
}

func receiveNamespace(t *testing.T, ch <-chan discovery.NamespaceEvent) discovery.NamespaceEvent {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed before event")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for namespace event")
		return discovery.NamespaceEvent{}
	}
}
