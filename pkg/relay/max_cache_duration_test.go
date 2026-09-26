package relay_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §12.3 MAX_CACHE_DURATION: "If present, the relay MUST NOT start forwarding
// any individual Object received through this subscription or fetch after the
// specified number of milliseconds has elapsed since the beginning of the
// Object was received." Both ways a relay forwards an Object are covered: from
// its cache (FETCH) and from a live subscriber's queue.

const maxCacheMs = 100

func maxCacheDurationProp() []wire.KVPair {
	return []wire.KVPair{{Type: message.PropertyMaxCacheDuration, IntVal: maxCacheMs}}
}

// TestRelay_MaxCacheDurationExpiresCachedObject: a FETCH after the duration
// must not be served the Object from the cache.
func TestRelay_MaxCacheDurationExpiresCachedObject(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, maxCacheDurationProp())
	subSess := subscribeCam1(t, pubSess)
	publishSubgroupObject(t, pubSess, alias, 3, -1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("object not forwarded live")
	}
	time.Sleep(3 * maxCacheMs * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	for _, e := range tryFetchElems(t, fetchSess, wire.TrackNamespace{[]byte("video")}, []byte("cam1"), 3, nil) {
		if !e.Unknown && e.Group == 3 {
			t.Fatalf("FETCH served Object {3,%d} after its MAX_CACHE_DURATION elapsed", e.Object)
		}
	}
}

// TestRelay_MaxCacheDurationDropsStaleQueuedObject: an Object that waited in a
// slow subscriber's queue past the duration is not forwarded to it.
func TestRelay_MaxCacheDurationDropsStaleQueuedObject(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, maxCacheDurationProp())
	subSess := subscribeCam1(t, pubSess)
	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 3,
		})
		if err != nil {
			return
		}
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
		_ = sg.Close()
	}()
	// Not reading: the relay's writer for this subscriber cannot start
	// forwarding until the subscriber accepts, by which time the Object is
	// stale.
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

// TestRelay_MaxCacheDurationSkippedHeadClearsFirstObject: when the subgroup's
// first Object expires before it is forwarded but a later one is sent, the
// stream the subscriber receives must not claim FIRST_OBJECT (§11.4.2: "the
// first object in this subgroup stream is the first object published in the
// subgroup"). The skipped Object's state is unknown (§12.3), not
// non-existent.
func TestRelay_MaxCacheDurationSkippedHeadClearsFirstObject(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, maxCacheDurationProp())
	subSess := subscribeCam1(t, pubSess)
	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 3,
	})
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
		// §11.4.3: the expired head is missing, so no FIN.
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

// TestRelay_MaxCacheDurationZeroNeverServesFromCache: a present
// MAX_CACHE_DURATION of 0 differs from an absent one (§12.3: only "If
// MAX_CACHE_DURATION is not sent" may Objects be cached until evicted). This
// relay reads 0 as "never serve from the cache" while still forwarding live
// Objects to current subscribers.
func TestRelay_MaxCacheDurationZeroNeverServesFromCache(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t,
		[]wire.KVPair{{Type: message.PropertyMaxCacheDuration, IntVal: 0}})
	subSess := subscribeCam1(t, pubSess)
	publishSubgroupObject(t, pubSess, alias, 3, -1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("live subscriber did not receive the Object")
	}
	fetchSess := dialAnotherClient(t, pubSess)
	for _, e := range tryFetchElems(t, fetchSess, wire.TrackNamespace{[]byte("video")}, []byte("cam1"), 3, nil) {
		if !e.Unknown && e.Group == 3 {
			t.Fatalf("FETCH served Object {3,%d} from the cache despite MAX_CACHE_DURATION=0", e.Object)
		}
	}
}

// publishSecondCam1 PUBLISHes video/cam1 again, from a second publisher
// session, with the given Track Properties, and returns that session.
func publishSecondCam1(t *testing.T, pubSess *session.Session, alias uint64, props []wire.KVPair) *session.Session {
	t.Helper()
	second := dialAnotherClient(t, pubSess)
	p, err := second.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"), TrackAlias: alias,
		TrackProperties: message.AppendTrackProperties(props),
	})
	if err != nil {
		t.Fatalf("second Publish: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return second
}

// fetchCam1 FETCHes video/cam1 up to {lastGroup, 0}, retrying until served.
func fetchCam1(t *testing.T, sess *session.Session, lastGroup uint64) []fetchElem {
	t.Helper()
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		if elems := tryFetchElems(
			t,
			sess,
			wire.TrackNamespace{[]byte("video")},
			[]byte("cam1"),
			lastGroup,
			nil,
		); elems != nil {
			return elems
		}
		if time.Now().After(deadline) {
			t.Fatal("FETCH never served")
		}
	}
}

func findElem(elems []fetchElem, group uint64) (fetchElem, bool) {
	for _, e := range elems {
		if e.Group == group && e.Object == 0 {
			return e, true
		}
	}
	return fetchElem{}, false
}

// TestRelay_MaxCacheDurationPerUpstream: §12.3 limits "any individual Object
// received through this subscription or fetch", so an Object is bound by the
// MAX_CACHE_DURATION of the upstream it arrived on, not by whichever
// publisher's Track Properties the relay saw first.
func TestRelay_MaxCacheDurationPerUpstream(t *testing.T) {
	t.Parallel()
	pubA, _ := publishWithTrackProps(t, maxCacheDurationProp())
	pubB := publishSecondCam1(t, pubA, 9, nil)
	subscribeCam1(t, pubA)
	publishSubgroupObject(t, pubB, 9, 5, -1)
	time.Sleep(3 * maxCacheMs * time.Millisecond)

	elems := fetchCam1(t, dialAnotherClient(t, pubA), 5)
	if e, ok := findElem(elems, 5); !ok || e.Unknown {
		t.Fatalf("FETCH elements %+v: the Object from the publisher without MAX_CACHE_DURATION "+
			"expired by the other publisher's value", elems)
	}
}

// TestRelay_MaxCacheDurationExpiredObjectIsUnknown: "Once Objects have expired
// from cache, their state becomes unknown" (§12.3). An Object that expired
// between two that did not is reported with an End of Unknown Range marker,
// not a plain gap, which a FETCH response asserts as non-existence (§11.4.4).
func TestRelay_MaxCacheDurationExpiredObjectIsUnknown(t *testing.T) {
	t.Parallel()
	pubA, aliasA := publishWithTrackProps(t, maxCacheDurationProp())
	pubB := publishSecondCam1(t, pubA, 9, nil)
	subscribeCam1(t, pubA)
	publishSubgroupObject(t, pubB, 9, 1, -1)
	publishSubgroupObject(t, pubA, aliasA, 2, -1)
	publishSubgroupObject(t, pubB, 9, 3, -1)
	time.Sleep(3 * maxCacheMs * time.Millisecond)

	elems := fetchCam1(t, dialAnotherClient(t, pubA), 3)
	e1, ok1 := findElem(elems, 1)
	e2, ok2 := findElem(elems, 2)
	e3, ok3 := findElem(elems, 3)
	if !ok1 || e1.Unknown || !ok3 || e3.Unknown || !ok2 || !e2.Unknown {
		t.Fatalf("FETCH elements %+v, want Objects at groups 1 and 3 and an unknown marker at group 2", elems)
	}
}

// TestRelay_MaxCacheDurationExpiresDuringFetch: the relay "MUST NOT start
// forwarding any individual Object [...] after" MAX_CACHE_DURATION. An Object
// that was fresh when the FETCH began but has expired by the time its turn to
// be written comes (the subscriber is slow to read) is reported unknown
// instead of sent.
func TestRelay_MaxCacheDurationExpiresDuringFetch(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, maxCacheDurationProp())
	subscribeCam1(t, pubSess)
	for g := uint64(1); g <= 3; g++ {
		publishSubgroupObject(t, pubSess, alias, g, -1)
	}
	fc := dialAnotherClient(t, pubSess)
	fr, err := fc.Fetch(t.Context(), &message.Fetch{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
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
