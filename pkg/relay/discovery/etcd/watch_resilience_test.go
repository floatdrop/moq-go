package etcd_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	etcdstore "github.com/floatdrop/moq-go/pkg/relay/discovery/etcd"
)

// newStoreAndClient returns a Store scoped to prefix plus the raw etcd client
// behind it, so a test can corrupt what the Store wrote and watch what it does
// about it.
func newStoreAndClient(t *testing.T, prefix string, opts ...etcdstore.Option) (*etcdstore.Store, *clientv3.Client) {
	t.Helper()
	endpoints := startEmbeddedEtcd(t)
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial etcd: %v", err)
	}
	t.Cleanup(func() { cli.Close() })

	all := append([]etcdstore.Option{
		etcdstore.WithPrefix(prefix),
		etcdstore.WithLogger(slog.New(slog.DiscardHandler)),
	}, opts...)
	s := etcdstore.New(cli, all...)
	t.Cleanup(func() { s.Close() })
	return s, cli
}

// soleKeyUnder returns the single etcd key under prefix, failing if there is
// not exactly one. Deriving the key this way rather than recomputing the
// Store's layout keeps the test honest: it corrupts whatever the Store actually
// wrote, so it cannot drift out of sync with the key scheme.
func soleKeyUnder(t *testing.T, cli *clientv3.Client, prefix string) string {
	t.Helper()
	resp, err := cli.Get(t.Context(), prefix, clientv3.WithPrefix())
	if err != nil {
		t.Fatalf("get %s: %v", prefix, err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("found %d keys under %s, want exactly 1", len(resp.Kvs), prefix)
	}
	return string(resp.Kvs[0].GetKey())
}

// TestWatchNamespacesSeedsSnapshot covers the namespace half of the snapshot
// path — WatchTracks had a test for this and WatchNamespaces did not, which is
// why namespaceSnapshot sat at 0%.
//
// The snapshot is what makes a relay joining a running cluster useful
// immediately: without it, it would learn only about namespaces advertised
// after it happened to start watching, and every participant already in a
// conference would be invisible to it.
func TestWatchNamespacesSeedsSnapshot(t *testing.T) {
	s, _ := newStoreAndClient(t, "/nssnap/")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	// Advertised BEFORE the watch exists.
	if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    ns("chat"),
		RelayAddr: "relay-A",
	}); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}

	ch, err := s.WatchNamespaces(ctx)
	if err != nil {
		t.Fatalf("WatchNamespaces: %v", err)
	}

	got := receiveNamespace(t, ch)
	if got.Op != discovery.OpPublish {
		t.Errorf("snapshot Op = %v, want publish", got.Op)
	}
	if got.Info.RelayAddr != "relay-A" {
		t.Errorf("snapshot RelayAddr = %q, want relay-A", got.Info.RelayAddr)
	}

	// And the watch still follows live changes after replaying the snapshot.
	if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    ns("chat"),
		RelayAddr: "relay-B",
	}); err != nil {
		t.Fatalf("live PublishNamespace: %v", err)
	}
	if live := receiveNamespace(t, ch); live.Info.RelayAddr != "relay-B" {
		t.Errorf("live event RelayAddr = %q, want relay-B", live.Info.RelayAddr)
	}
}

// TestWatchSkipsUndecodableEntries is the resilience property that matters for
// a shared cluster: one corrupt value must not take the watch down with it.
//
// These stores share a keyspace with every other relay process, including ones
// running a different build. A value this version cannot decode — written by an
// older relay, a newer one, or a hand-edited key — has to be logged and stepped
// over. If it aborted the watch instead, a single bad entry would stop one relay
// discovering anything at all, and the failure would look like a network
// problem rather than a data one.
func TestWatchSkipsUndecodableEntries(t *testing.T) {
	t.Run("in the snapshot", func(t *testing.T) {
		const prefix = "/nsgarbage/"
		s, cli := newStoreAndClient(t, prefix)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix:    ns("chat"),
			RelayAddr: "relay-A",
		}); err != nil {
			t.Fatalf("seed PublishNamespace: %v", err)
		}
		// Corrupt it in place, so the watch's snapshot has to skip it.
		key := soleKeyUnder(t, cli, prefix)
		if _, err := cli.Put(ctx, key, "{not json"); err != nil {
			t.Fatalf("corrupt %s: %v", key, err)
		}

		ch, err := s.WatchNamespaces(ctx)
		if err != nil {
			t.Fatalf("WatchNamespaces: %v", err)
		}

		// The corrupt entry yields nothing, but the watch is alive: a good
		// advertisement published afterwards still arrives.
		if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix:    ns("chat"),
			RelayAddr: "relay-B",
		}); err != nil {
			t.Fatalf("PublishNamespace after corruption: %v", err)
		}
		got := receiveNamespace(t, ch)
		if got.Info.RelayAddr != "relay-B" {
			t.Errorf("got RelayAddr %q, want relay-B — the corrupt snapshot entry "+
				"was delivered instead of skipped", got.Info.RelayAddr)
		}
	})

	t.Run("in a live event", func(t *testing.T) {
		const prefix = "/nsgarbagelive/"
		s, cli := newStoreAndClient(t, prefix)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		ch, err := s.WatchNamespaces(ctx)
		if err != nil {
			t.Fatalf("WatchNamespaces: %v", err)
		}

		// A PUT the Store cannot decode, arriving while the watch is running.
		if _, err := cli.Put(ctx, prefix+"n/deadbeef/relay-X", "{not json"); err != nil {
			t.Fatalf("put garbage: %v", err)
		}
		if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix:    ns("chat"),
			RelayAddr: "relay-B",
		}); err != nil {
			t.Fatalf("PublishNamespace: %v", err)
		}

		got := receiveNamespace(t, ch)
		if got.Info.RelayAddr != "relay-B" {
			t.Errorf("got RelayAddr %q, want relay-B — the undecodable event was "+
				"delivered instead of skipped", got.Info.RelayAddr)
		}
	})
}

// TestWatchDeliversUnpublishOnDelete covers the DELETE half of the event
// decoders. It is worth its own test because a delete cannot be decoded the
// same way as a put: the storage key is a one-way hash, so the Store has to
// read PrevKv to recover which namespace went away. Losing that would leave
// every relay believing a departed participant is still reachable.
func TestWatchDeliversUnpublishOnDelete(t *testing.T) {
	s, _ := newStoreAndClient(t, "/nsdelete/")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	ch, err := s.WatchNamespaces(ctx)
	if err != nil {
		t.Fatalf("WatchNamespaces: %v", err)
	}
	if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    ns("chat"),
		RelayAddr: "relay-A",
	}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	if first := receiveNamespace(t, ch); first.Op != discovery.OpPublish {
		t.Fatalf("first Op = %v, want publish", first.Op)
	}

	if err := s.UnpublishNamespace(ctx, ns("chat"), "relay-A"); err != nil {
		t.Fatalf("UnpublishNamespace: %v", err)
	}
	second := receiveNamespace(t, ch)
	if second.Op != discovery.OpUnpublish {
		t.Errorf("second Op = %v, want unpublish", second.Op)
	}
	// Recovered from PrevKv, not from the key.
	if second.Info.RelayAddr != "relay-A" {
		t.Errorf("unpublish RelayAddr = %q, want relay-A — PrevKv was not decoded",
			second.Info.RelayAddr)
	}
}

// TestWithWatchBufferSize covers the option and its documented guard: a
// non-positive size falls back to the default rather than creating an
// unbuffered channel that would stall the watch pump.
func TestWithWatchBufferSize(t *testing.T) {
	for _, size := range []int{1, 0, -1} {
		s, _ := newStoreAndClient(t, "/bufsize/", etcdstore.WithWatchBufferSize(size))
		ctx, cancel := context.WithCancel(t.Context())

		ch, err := s.WatchNamespaces(ctx)
		if err != nil {
			t.Fatalf("WatchNamespaces(size=%d): %v", size, err)
		}
		if err := s.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix:    ns("chat"),
			RelayAddr: "relay-A",
		}); err != nil {
			t.Fatalf("PublishNamespace(size=%d): %v", size, err)
		}
		if got := receiveNamespace(t, ch); got.Op != discovery.OpPublish {
			t.Errorf("size=%d: Op = %v, want publish", size, got.Op)
		}
		cancel()
	}
}

// TestWatchTracksSkipsUndecodableEntries mirrors the namespace resilience test
// on the track side, which is the busier of the two: every published track is a
// key here, and a relay that stopped discovering tracks because one entry was
// corrupt would fail to route anything at all.
//
// It also covers the track DELETE decoder, which like the namespace one has to
// read PrevKv — the track key is a one-way hash of the FullTrackName and cannot
// be reversed to say which track went away.
func TestWatchTracksSkipsUndecodableEntries(t *testing.T) {
	const prefix = "/trkgarbage/"
	s, cli := newStoreAndClient(t, prefix)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	key := newKey([]string{"video"}, "cam1")
	info := discovery.TrackInfo{
		Key:       key,
		FullName:  track.FullTrackName{Namespace: ns("video"), Name: []byte("cam1")},
		RelayAddr: "relay-A",
	}
	if err := s.PublishTrack(ctx, info); err != nil {
		t.Fatalf("seed PublishTrack: %v", err)
	}
	// Corrupt the seeded entry so the snapshot has to step over it.
	corrupt := soleKeyUnder(t, cli, prefix)
	if _, err := cli.Put(ctx, corrupt, "{not json"); err != nil {
		t.Fatalf("corrupt %s: %v", corrupt, err)
	}

	ch, err := s.WatchTracks(ctx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}

	// A live PUT the Store cannot decode either, then a good one.
	if _, err := cli.Put(ctx, prefix+"t/deadbeef/relay-X", "{not json"); err != nil {
		t.Fatalf("put garbage: %v", err)
	}
	good := info
	good.RelayAddr = "relay-B"
	if err := s.PublishTrack(ctx, good); err != nil {
		t.Fatalf("PublishTrack after corruption: %v", err)
	}

	got := receiveTrack(t, ch)
	if got.Op != discovery.OpPublish || got.Info.RelayAddr != "relay-B" {
		t.Errorf("got %v from %q, want publish from relay-B — an undecodable entry "+
			"was delivered instead of skipped", got.Op, got.Info.RelayAddr)
	}

	// And the DELETE decoder, which recovers the track from PrevKv.
	if err := s.UnpublishTrack(ctx, key, "relay-B"); err != nil {
		t.Fatalf("UnpublishTrack: %v", err)
	}
	del := receiveTrack(t, ch)
	if del.Op != discovery.OpUnpublish {
		t.Errorf("Op = %v, want unpublish", del.Op)
	}
	if del.Info.RelayAddr != "relay-B" {
		t.Errorf("unpublish RelayAddr = %q, want relay-B — PrevKv was not decoded",
			del.Info.RelayAddr)
	}
}
