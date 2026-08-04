package nats_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	natsstore "github.com/floatdrop/moq-go/pkg/relay/discovery/nats"
)

// TestLivenessExpiryUnpublishes is the headline property of a distributed
// discovery backend: when a relay crashes without a graceful Close, its
// advertisements must expire and peers must learn to stop routing to it — the
// same outcome an expired etcd lease produces.
//
// It runs two stores over one bucket. The publisher advertises a track with a
// short TTL, then its connection is dropped abruptly (a crash: no Unpublish, no
// Close), so its heartbeat can no longer refresh the key. The watcher on the
// second store must observe the key expire as an OpUnpublish, which only happens
// because the bucket sets LimitMarkerTTL — a plain TTL expiry would remove the
// key silently.
func TestLivenessExpiryUnpublishes(t *testing.T) {
	url := startEmbeddedNATS(t)
	const bucket = "liveness"
	const ttl = 2 * time.Second

	// Watcher store on its own connection — it survives the publisher's "crash".
	ncWatch, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect watcher: %v", err)
	}
	t.Cleanup(ncWatch.Close)
	jsWatch, err := jetstream.New(ncWatch)
	if err != nil {
		t.Fatalf("jetstream watcher: %v", err)
	}
	watcher, err := natsstore.New(t.Context(), jsWatch,
		natsstore.WithBucket(bucket), natsstore.WithLivenessTTL(ttl))
	if err != nil {
		t.Fatalf("new watcher store: %v", err)
	}
	t.Cleanup(func() { _ = watcher.Close() })

	watchCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	ch, err := watcher.WatchTracks(watchCtx)
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}

	// Publisher store on a separate connection we can kill.
	ncPub, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect publisher: %v", err)
	}
	jsPub, err := jetstream.New(ncPub)
	if err != nil {
		t.Fatalf("jetstream publisher: %v", err)
	}
	publisher, err := natsstore.New(t.Context(), jsPub,
		natsstore.WithBucket(bucket), natsstore.WithLivenessTTL(ttl))
	if err != nil {
		t.Fatalf("new publisher store: %v", err)
	}
	// Do NOT defer publisher.Close(): a graceful Close would delete the key, which
	// is exactly the path we are NOT testing. The heartbeat goroutine is orphaned
	// when we drop ncPub below; it dies with the test process.

	key := track.NewKey(ns("video"), []byte("cam1"))
	if err := publisher.PublishTrack(t.Context(), discovery.TrackInfo{
		Key:       key,
		FullName:  track.FullTrackName{Namespace: ns("video"), Name: []byte("cam1")},
		RelayAddr: "relay-crashed",
	}); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	pub := receiveTrack(t, ch)
	if pub.Op != discovery.OpPublish || pub.Info.RelayAddr != "relay-crashed" {
		t.Fatalf("first event = %+v, want publish of relay-crashed", pub)
	}

	// Crash: drop the publisher's connection so its heartbeat can no longer
	// refresh the key. After the TTL elapses with no refresh, JetStream expires
	// the key and (thanks to LimitMarkerTTL) emits a marker the watcher turns into
	// OpUnpublish.
	ncPub.Close()

	// Allow generous slack: MaxAge enforcement is timer-driven and can lag the TTL.
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("watch channel closed before expiry event")
		}
		if ev.Op != discovery.OpUnpublish {
			t.Fatalf("event after crash = %v, want unpublish", ev.Op)
		}
		if ev.Info.Key != key {
			t.Error("expiry OpUnpublish Key mismatch (last-value reconstruction failed)")
		}
	case <-time.After(20 * time.Second):
		t.Fatal("no OpUnpublish within 20s of crash — TTL expiry did not propagate")
	}
}

// TestWithdrawUnpublishesToPeers pins the [discovery.DiscoveryStore.Withdraw]
// contract on the NATS backend, which is what makes a relay stop being
// discoverable before it sends GOAWAY. A second store over the same bucket plays
// the peer relay: it must observe the withdrawal as an OpUnpublish, at once,
// rather than waiting out the liveness TTL.
func TestWithdrawUnpublishesToPeers(t *testing.T) {
	url := startEmbeddedNATS(t)
	const bucket = "withdraw"

	peer := openStore(t, url, bucket, nil)
	leaving := openStore(t, url, bucket, nil)

	key := newKey([]string{"video"}, "cam1")
	if err := leaving.PublishTrack(t.Context(),
		discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); err != nil {
		t.Fatalf("PublishTrack: %v", err)
	}

	events, err := peer.WatchTracks(t.Context())
	if err != nil {
		t.Fatalf("WatchTracks: %v", err)
	}
	// The watch opens with a snapshot, which must carry the advertisement.
	select {
	case ev := <-events:
		if ev.Op != discovery.OpPublish {
			t.Fatalf("snapshot Op = %v, want OpPublish", ev.Op)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("peer never saw the advertisement")
	}

	if err := leaving.Withdraw(t.Context(), "relay-A"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}

	select {
	case ev := <-events:
		if ev.Op != discovery.OpUnpublish {
			t.Errorf("event Op = %v, want OpUnpublish", ev.Op)
		}
		if ev.Info.RelayAddr != "relay-A" {
			t.Errorf("event RelayAddr = %q, want relay-A", ev.Info.RelayAddr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("peer never observed the withdrawal")
	}

	// The peer no longer resolves the withdrawn relay.
	if infos, err := peer.FindTrack(t.Context(), key); err != nil {
		t.Fatalf("FindTrack: %v", err)
	} else if len(infos) != 0 {
		t.Errorf("peer still resolves %d advertisements after Withdraw", len(infos))
	}

	// Terminal: a publisher arriving while the relay drains must not restore it.
	if err := leaving.PublishTrack(t.Context(),
		discovery.TrackInfo{Key: key, RelayAddr: "relay-A"}); !errors.Is(err, discovery.ErrWithdrawn) {
		t.Errorf("PublishTrack after Withdraw = %v, want ErrWithdrawn", err)
	}

	// Reads still work: withdrawn, not closed.
	if _, err := leaving.FindTrack(t.Context(), key); err != nil {
		t.Errorf("FindTrack on a withdrawn store: %v", err)
	}
	if err := leaving.Withdraw(t.Context(), "relay-A"); err != nil {
		t.Errorf("second Withdraw = %v, want nil", err)
	}
}

// TestWithdrawRacingPublishLeavesNothingBehind pins the one race Withdraw cannot
// cover with the store lock alone: publish's Put is a network round trip, so a
// Withdraw can land after the sweep took the own set but before the new key
// would have joined it. A key in that gap belongs to nobody — no heartbeat
// refreshes it, no Close or later Withdraw deletes it — so peers would keep
// resolving a relay that already withdrew, until the liveness TTL expired it.
// That is precisely what withdrawing before the GOAWAY exists to prevent, so the
// publish that loses the race must clean up after itself.
//
// Detection profile, measured against the unfixed publish: 3/5 runs. Only a
// publish already past the withdrawn check can orphan a key, and paced writers
// are usually between Puts rather than inside one, so the window is genuinely
// narrow. Unpaced writers detect it 5/5 but publish tens of thousands of keys in
// the same window, and verifying those exactly takes ~20s — pinning the bug
// deterministically and cheaply would need an injection point in the Put path
// that the store does not expose. This never fails on correct code.
func TestWithdrawRacingPublishLeavesNothingBehind(t *testing.T) {
	url := startEmbeddedNATS(t)
	const bucket = "withdraw_race"

	peer := openStore(t, url, bucket, nil)
	leaving := openStore(t, url, bucket, nil)

	// Each writer publishes a stream of *distinct* keys and keeps going until
	// Withdraw has returned, which is what guarantees Puts are in flight while it
	// sweeps. The keys must be distinct for the race to bite: a key the writer
	// already published is in the own set, so the sweep deletes it, whereas a key
	// whose first Put lands after the sweep is the one nobody owns. Each writer
	// paces itself so the recorded key set stays small enough to verify exactly.
	const (
		writers    = 8
		pace       = 2 * time.Millisecond
		withdrawAt = 40 * time.Millisecond
	)

	var (
		keysMu    sync.Mutex
		attempted []track.Key
	)
	swept := make(chan struct{})
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-swept:
					return
				default:
				}
				key := newKey([]string{"video"}, fmt.Sprintf("cam-%d-%d", w, i))
				// Recorded before the Put, so a key that lands is never missed
				// by the check below.
				keysMu.Lock()
				attempted = append(attempted, key)
				keysMu.Unlock()
				err := leaving.PublishTrack(context.Background(),
					discovery.TrackInfo{Key: key, RelayAddr: "relay-A"})
				if err != nil && !errors.Is(err, discovery.ErrWithdrawn) {
					t.Errorf("PublishTrack: %v", err)
					return
				}
				time.Sleep(pace)
			}
		}(w)
	}

	// Withdraw underneath the writers, mid-stream.
	time.Sleep(withdrawAt)
	if err := leaving.Withdraw(context.Background(), "relay-A"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	close(swept)
	wg.Wait()

	keysMu.Lock()
	keys := attempted
	keysMu.Unlock()
	t.Logf("verifying %d attempted advertisements", len(keys))

	// Nothing this relay advertised may still be visible to a peer. The keys are
	// enumerated rather than scanned: FindTrack reads to the bucket's
	// end-of-initial-values sentinel, so this needs no timing heuristic that
	// could under-count and quietly weaken the test.
	//
	// The deadline must stay well under the liveness TTL. At or past it, an
	// orphan would expire on its own and the check would pass for the wrong
	// reason — the bug would look fixed.
	deadline := time.Now().Add(3 * time.Second)
	for {
		leftover := 0
		for _, key := range keys {
			infos, err := peer.FindTrack(context.Background(), key)
			if err != nil {
				t.Fatalf("FindTrack: %v", err)
			}
			for _, info := range infos {
				if info.RelayAddr == "relay-A" {
					leftover++
				}
			}
		}
		if leftover == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d advertisements survived Withdraw; peers would keep"+
				" resolving a withdrawn relay until the TTL", leftover)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
