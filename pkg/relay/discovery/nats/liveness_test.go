package nats_test

import (
	"context"
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
