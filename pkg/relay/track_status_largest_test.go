package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestTrackStatus_ReturnsLargestObject is the canonical assertion: after
// the publisher emits objects, a TRACK_STATUS for the same track carries
// the §10.2.11 LARGEST_OBJECT parameter in TRACK_STATUS_OK, sourced from
// the entry's watermark (which the fanout maintains on every forwarded
// object).
func TestTrackStatus_ReturnsLargestObject(t *testing.T) {
	t.Parallel()

	pubSess, _, publisherAlias := publishAndCache(t)
	publishObjects(t, pubSess, publisherAlias, 4 /*group*/, 3 /*count*/)

	// Watermark = {4, 2}. Give the relay a beat to drain the fanout
	// into the cache + watermark before issuing the query.
	time.Sleep(50 * time.Millisecond)

	querySess := dialAnotherClient(t, pubSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	p, found := tsStream.OK.Parameters.Find(message.ParamLargestObject)
	if !found {
		t.Fatal("TRACK_STATUS_OK missing LARGEST_OBJECT parameter")
	}
	if p.Group != 4 || p.Object != 2 {
		t.Fatalf("LARGEST_OBJECT = {%d, %d}, want {4, 2}", p.Group, p.Object)
	}
}

// TestTrackStatus_OmitsLargestObjectBeforeAnyObjects pins the boundary
// for tracks the relay knows about but where no object has been
// forwarded yet (e.g., publisher claimed the track via PUBLISH but
// hasn't opened a subgroup). The entry's watermark is the zero
// Location and the reply MUST NOT carry a LARGEST_OBJECT parameter —
// emitting one would claim the publisher delivered Location {0, 0},
// which §10.2.11 explicitly reserves for "no objects observed":
//
//	"If omitted from a message, the sending endpoint has not published
//	or received any Objects in the Track."
func TestTrackStatus_OmitsLargestObjectBeforeAnyObjects(t *testing.T) {
	t.Parallel()

	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackAlias:      1,
		TrackProperties: []byte("rtp-h265"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	querySess := dialAnotherClient(t, clientSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	if _, found := tsStream.OK.Parameters.Find(message.ParamLargestObject); found {
		t.Fatal("TRACK_STATUS_OK unexpectedly carried LARGEST_OBJECT before any objects were forwarded")
	}
	if string(tsStream.OK.TrackProperties) != "rtp-h265" {
		t.Fatalf("TrackProperties = %q, want %q", tsStream.OK.TrackProperties, "rtp-h265")
	}
}

// TestFetch_CacheEvictionUnderLoad is the integration test paired with
// the cache eviction story: under a publish flood that exceeds MaxCacheSize
// the relay's per-track cache must evict older entries, and a FETCH
// over the early range comes back with strictly fewer objects than
// the range implies. Per §10.12.3 the subscriber interprets gaps as
// "objects do not exist".
//
// The FIFO ring evicts strictly oldest-first, but the test asserts only
// the boundary (Len ≤ cap on the recent-tail FETCH, fewer-than-cap on
// the early-range FETCH) rather than specific IDs.
func TestFetch_CacheEvictionUnderLoad(t *testing.T) {
	t.Parallel()

	const cacheCap = 16

	pubSess, teardown := connectRelay(t, relay.Config{
		MaxCacheSize: cacheCap,
	})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	// A subscriber must exist for the fanout to commit objects to the
	// per-track cache (the drainInbound path discards otherwise).
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()
	go drainAllStreams(t.Context(), subSess)

	// Publish 4× cacheCap objects on a single subgroup so size-based
	// eviction is forced.
	const totalObjects = cacheCap * 4
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, totalObjects)

	// Give the relay a beat to drain the publish flood into the cache
	// before the read side asserts.
	time.Sleep(200 * time.Millisecond)

	// FETCH the recent tail — those objects should still be present.
	fetchSess := dialAnotherClient(t, pubSess)
	_, recent := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: uint64(totalObjects - cacheCap)},
		message.Location{Group: 0, Object: uint64(totalObjects)},
		message.GroupOrderAscending,
	)
	if len(recent) == 0 {
		t.Fatal("recent-tail FETCH returned 0 objects; expected eviction to retain recently-published entries")
	}
	if len(recent) > cacheCap {
		t.Fatalf("recent-tail FETCH returned %d objects, want <= cacheCap (%d)", len(recent), cacheCap)
	}
	for _, o := range recent {
		if o.object < uint64(totalObjects-cacheCap) {
			t.Fatalf("recent-tail FETCH returned object %d, below the tail boundary (%d)",
				o.object, totalObjects-cacheCap)
		}
	}

	// FETCH the oldest range — these objects should mostly be evicted.
	// The FIFO ring keeps exactly the newest cacheCap entries, but the
	// test only asserts the boundary: fewer objects than the range
	// itself.
	fetchSess2 := dialAnotherClient(t, pubSess)
	_, oldest := fetchAndDrain(t,
		fetchSess2,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 0, Object: uint64(cacheCap)},
		message.GroupOrderAscending,
	)
	if len(oldest) >= cacheCap {
		t.Fatalf("oldest-range FETCH returned %d objects; expected eviction to have dropped some (want < %d)",
			len(oldest), cacheCap)
	}
}
