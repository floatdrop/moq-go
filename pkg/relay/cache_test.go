package relay_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// The relay's per-track cache as seen by FETCH: size-based eviction, and
// MAX_CACHE_DURATION (§12.3), after which the relay must not start forwarding
// an Object, from the cache or from a live subscriber's queue.

// TestFetch_CacheEvictionUnderLoad: past MaxCacheSize the cache evicts the
// oldest Objects, so a FETCH of the early range returns fewer Objects than it
// spans while the recent tail is still served.
func TestFetch_CacheEvictionUnderLoad(t *testing.T) {
	t.Parallel()

	const cacheCap = 16

	pubSess, teardown := connectRelay(t, relay.Config{
		MaxCacheSize: cacheCap,
	})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	// The fanout caches only while a subscriber exists.
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()
	go drainAll(t.Context(), subSess)

	const totalObjects = cacheCap * 4
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, totalObjects)
	time.Sleep(200 * time.Millisecond) // let the flood reach the cache

	fetchSess := dialAnotherClient(t, pubSess)
	_, recent := fetchAndDrain(t,
		fetchSess,
		ns("video"),
		[]byte("cam1"),
		message.Location{Group: 0, Object: uint64(totalObjects - cacheCap)},
		message.Location{Group: 0, Object: uint64(totalObjects - 1)},
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

	fetchSess2 := dialAnotherClient(t, pubSess)
	_, oldest := fetchAndDrain(t,
		fetchSess2,
		ns("video"),
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 0, Object: uint64(cacheCap - 1)},
		message.GroupOrderAscending,
	)
	if len(oldest) >= cacheCap {
		t.Fatalf("oldest-range FETCH returned %d objects; expected eviction to have dropped some (want < %d)",
			len(oldest), cacheCap)
	}
}

const maxCacheMs = 100

// maxCacheDurationProp sets MAX_CACHE_DURATION to maxCacheMs.
func maxCacheDurationProp() []wire.KVPair {
	return trackProp(message.PropertyMaxCacheDuration, maxCacheMs)
}

// fetchCam1 FETCHes video/cam1 up to {lastGroup, 0}, retrying for 2s until
// served.
func fetchCam1(t *testing.T, sess *session.Session, lastGroup uint64) []fetchElem {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if elems := tryFetchElems(t, sess, ns("video"), []byte("cam1"), lastGroup, nil); elems != nil {
			return elems
		}
		if time.Now().After(deadline) {
			t.Fatal("FETCH never served")
		}
	}
}

// findElem returns the element at {group, 0}.
func findElem(elems []fetchElem, group uint64) (fetchElem, bool) {
	i := slices.IndexFunc(elems, func(e fetchElem) bool { return e.Group == group && e.Object == 0 })
	if i < 0 {
		return fetchElem{}, false
	}
	return elems[i], true
}

// TestRelay_MaxCacheDurationNotServedFromCache: a FETCH is not served an Object
// whose MAX_CACHE_DURATION elapsed (§12.3). This relay reads a present 0 as
// "never serve from the cache", unlike an absent one, while still forwarding
// live Objects.
func TestRelay_MaxCacheDurationNotServedFromCache(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		duration uint64
		wait     time.Duration
	}{
		{"expired", maxCacheMs, 3 * maxCacheMs * time.Millisecond},
		{"zero", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, alias := newCam1Publisher(t, trackProp(message.PropertyMaxCacheDuration, tc.duration))
			subSess := newCam1Subscriber(t, pubSess)
			publishObjects(t, pubSess, alias, 3, 1)
			if !awaitSubgroupObject(t, subSess, 2*time.Second) {
				t.Fatal("live subscriber did not receive the Object")
			}
			time.Sleep(tc.wait)

			fetchSess := dialAnotherClient(t, pubSess)
			for _, e := range tryFetchElems(t, fetchSess, ns("video"), []byte("cam1"), 3, nil) {
				if !e.Unknown && e.Group == 3 {
					t.Fatalf("FETCH served Object {3,%d} with MAX_CACHE_DURATION=%d after %v",
						e.Object, tc.duration, tc.wait)
				}
			}
		})
	}
}

// TestRelay_MaxCacheDurationDropsStaleQueuedObject: an Object that waited in a
// slow subscriber's queue past the duration is not forwarded to it.
func TestRelay_MaxCacheDurationDropsStaleQueuedObject(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, maxCacheDurationProp())
	subSess := newCam1Subscriber(t, pubSess)
	go sendObjects(pubSess, alias, 3, 1)
	// Not reading: the relay cannot forward until the subscriber accepts,
	// by which time the Object is stale.
	time.Sleep(3 * maxCacheMs * time.Millisecond)

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	ds, err := subSess.AcceptDataStream(ctx)
	if err != nil {
		return // nothing forwarded: correct
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("got %T", ds)
	}
	if obj, err := sg.ReadObject(); err == nil {
		t.Fatalf("relay forwarded a stale Object (%d-byte payload) past MAX_CACHE_DURATION", len(obj.Payload))
	}
}

// TestRelay_MaxCacheDurationSkippedHeadClearsFirstObject: a stream whose
// expired first Object was skipped must not claim FIRST_OBJECT (§11.4.2), and
// must end in a reset, not a FIN (§11.4.3).
func TestRelay_MaxCacheDurationSkippedHeadClearsFirstObject(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, maxCacheDurationProp())
	subSess := newCam1Subscriber(t, pubSess)
	sg, err := pubSess.OpenSubgroup(subgroupHeader(alias, 3))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("head")}); err != nil {
		t.Fatalf("write head: %v", err)
	}
	// The subscriber does not read yet, so the head goes stale in the
	// relay's queue; then a fresh Object follows.
	time.Sleep(3 * maxCacheMs * time.Millisecond)
	go func() {
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("tail")})
		_ = sg.Close()
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		ds, err := subSess.AcceptDataStream(ctx)
		cancel()
		if err != nil {
			t.Fatal("the fresh Object was never forwarded")
		}
		in := ds.(*session.IncomingSubgroupStream)
		obj, err := in.ReadObject()
		if err != nil {
			continue // a stream that carried only the skipped head
		}
		if string(obj.Payload) == "head" {
			t.Fatal("relay forwarded the stale head Object")
		}
		if !in.Header.ReplayingSubgroup {
			t.Fatal("stream starting after a skipped head claims FIRST_OBJECT")
		}
		for {
			if _, err := in.ReadObject(); err != nil {
				if errors.Is(err, io.EOF) {
					t.Fatal("the stream after an expired head ended with a FIN; want a reset")
				}
				return
			}
		}
	}
}

// TestRelay_MaxCacheDurationPerUpstream: an Object is bound by the
// MAX_CACHE_DURATION of the upstream it arrived on (§12.3), not another
// publisher's.
func TestRelay_MaxCacheDurationPerUpstream(t *testing.T) {
	t.Parallel()
	pubA, _ := newCam1Publisher(t, maxCacheDurationProp())
	pubB := dialAnotherClient(t, pubA)
	publishVideoTrackProps(t, pubB, "cam1", 9, nil)
	newCam1Subscriber(t, pubA)
	publishObjects(t, pubB, 9, 5, 1)
	time.Sleep(3 * maxCacheMs * time.Millisecond)

	elems := fetchCam1(t, dialAnotherClient(t, pubA), 5)
	if e, ok := findElem(elems, 5); !ok || e.Unknown {
		t.Fatalf("FETCH elements %+v: the Object from the publisher without MAX_CACHE_DURATION "+
			"expired by the other publisher's value", elems)
	}
}

// TestRelay_MaxCacheDurationExpiredObjectIsUnknown: an expired Object's state
// is unknown (§12.3), so FETCH reports it with an End of Unknown Range marker,
// not a gap (§11.4.4).
func TestRelay_MaxCacheDurationExpiredObjectIsUnknown(t *testing.T) {
	t.Parallel()
	pubA, aliasA := newCam1Publisher(t, maxCacheDurationProp())
	pubB := dialAnotherClient(t, pubA)
	publishVideoTrackProps(t, pubB, "cam1", 9, nil)
	newCam1Subscriber(t, pubA)
	// END_OF_GROUP, so the Groups' ends are known and only the expired
	// Object is unknown.
	for _, p := range []struct {
		sess         *session.Session
		alias, group uint64
	}{{pubB, 9, 1}, {pubA, aliasA, 2}, {pubB, 9, 3}} {
		hdr := subgroupHeader(p.alias, p.group)
		hdr.EndOfGroup = true
		if err := writeSubgroup(p.sess, hdr, 1); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(3 * maxCacheMs * time.Millisecond)

	elems := fetchCam1(t, dialAnotherClient(t, pubA), 3)
	e1, ok1 := findElem(elems, 1)
	e2, ok2 := findElem(elems, 2)
	e3, ok3 := findElem(elems, 3)
	if !ok1 || e1.Unknown || !ok3 || e3.Unknown || !ok2 || !e2.Unknown {
		t.Fatalf("FETCH elements %+v, want Objects at groups 1 and 3 and an unknown marker at group 2", elems)
	}
}

// TestRelay_MaxCacheDurationExpiresDuringFetch: an Object fresh when the FETCH
// began but expired by its turn to be written (slow reader) is not sent.
func TestRelay_MaxCacheDurationExpiresDuringFetch(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, maxCacheDurationProp())
	newCam1Subscriber(t, pubSess)
	for g := uint64(1); g <= 3; g++ {
		publishObjects(t, pubSess, alias, g, 1)
	}
	fc := dialAnotherClient(t, pubSess)
	fr, err := fc.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{fetchRangeFilter(message.Location{Group: 1}, message.Location{Group: 3})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	ds, err := fc.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs := ds.(*session.IncomingFetchStream)
	time.Sleep(3 * maxCacheMs * time.Millisecond) // the relay's writes wait on this read

	var served []uint64
	for {
		obj, err := fs.ReadDecoded()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("fetch stream: %v", err)
			}
			break
		}
		if !obj.IsEndOfRange() {
			served = append(served, obj.GroupID)
		}
	}
	if len(served) > 1 {
		t.Fatalf(
			"served Objects in groups %v after they expired; at most the first write may have started in time",
			served,
		)
	}
}
