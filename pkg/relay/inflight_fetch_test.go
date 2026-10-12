package relay_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// arrivingObject publishes video/cam1 Objects {0,0}..{0,4} whole and {0,5}
// part way, half its payload ("pq" of "pqrs") sent, on Subgroup 0, then {0,6}
// whole on Subgroup 1, so that {0,6} is Largest Object while {0,5} is still
// arriving. It returns the publisher, a fetching client, and Subgroup 0's
// stream, positioned inside {0,5}'s payload.
func arrivingObject(t *testing.T) (*session.Session, *session.Session, *session.OutgoingSubgroupStream) {
	t.Helper()
	return arrivingObjectProps(t, nil)
}

// arrivingObjectProps is [arrivingObject] for a track published with props.
func arrivingObjectProps(
	t *testing.T,
	props []wire.KVPair,
) (*session.Session, *session.Session, *session.OutgoingSubgroupStream) {
	t.Helper()
	pubSess, alias := newCam1Publisher(t, props)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	go drainAll(t.Context(), subSess)
	slow, err := openSubgroupWaiting(t, pubSess, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 0, SubgroupID: 0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup 0: %v", err)
	}
	for range 5 {
		if err := slow.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
	}
	// Object 5: Object ID Delta 0, Payload Length 4, half the payload.
	if _, err := slow.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil {
		t.Fatalf("Write object 5 header: %v", err)
	}
	fast, err := openSubgroupWaiting(t, pubSess, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 0, SubgroupID: 1,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup 1: %v", err)
	}
	if err := fast.WriteObject(&message.SubgroupObject{ObjectIDDelta: 6, Payload: []byte("y")}); err != nil {
		t.Fatalf("WriteObject 6: %v", err)
	}
	_ = fast.Close()
	waitRelayLargest(t, subSess, ns("video"), []byte("cam1"), 0, 6)
	return pubSess, dialAnotherClient(t, pubSess), slow
}

// fetchGroup0 FETCHes video/cam1 {0,0}..{0,6} with FILL_TIMEOUT fill and
// returns the response's elements and how long it took.
func fetchGroup0(
	t *testing.T,
	sess *session.Session,
	fill time.Duration,
) ([]*session.DecodedFetchObject, time.Duration) {
	t.Helper()
	began := time.Now()
	req, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 6}),
			message.FillTimeoutParam(fill),
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	t.Cleanup(func() { _ = req.Close() })
	elems := readFetchResponse(t, sess, message.GroupOrderAscending, 10*time.Second)
	return elems, time.Since(began)
}

// object5 returns the element at {0,5}, or nil.
func object5(elems []*session.DecodedFetchObject) *session.DecodedFetchObject {
	for _, e := range elems {
		if e.GroupID == 0 && e.ObjectID == 5 {
			return e
		}
	}
	return nil
}

// TestFetch_WaitsForObjectStillArriving: Object {0,5} is mid-payload on one
// Subgroup when {0,6} arrives whole on another and becomes Largest Object. A
// FETCH of Group 0 includes {0,5}: the relay is receiving it, so its status is
// not unknown, and the response carries it in full once its last byte lands
// (§3.2: the publisher "delivers the remainder as it becomes available").
func TestFetch_WaitsForObjectStillArriving(t *testing.T) {
	t.Parallel()
	_, fetcher, slow := arrivingObject(t)
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = slow.Write([]byte{'r', 's'})
		_ = slow.Close()
	}()
	elems, _ := fetchGroup0(t, fetcher, 3*time.Second)
	for _, e := range elems {
		if e.IsEndOfRange() {
			t.Fatalf("response has an End of Range marker at {%d,%d}; want every Object", e.GroupID, e.ObjectID)
		}
	}
	if o := object5(elems); o == nil || !bytes.Equal(o.Payload, []byte("pqrs")) {
		t.Fatalf("Object {0,5} = %+v, want payload %q", o, "pqrs")
	}
}

// TestFetch_ObjectStillArrivingTimesOut: an Object that does not finish
// arriving within the FILL_TIMEOUT budget (§9.20.5) is an End of Timed-Out
// Range, not of Unknown Range: the relay knows it exists.
func TestFetch_ObjectStillArrivingTimesOut(t *testing.T) {
	t.Parallel()
	_, fetcher, _ := arrivingObject(t)
	elems, _ := fetchGroup0(t, fetcher, 300*time.Millisecond)
	if o := object5(elems); o == nil || !o.EndOfTimedOutRange {
		t.Fatalf("element at {0,5} = %+v, want an End of Timed-Out Range", o)
	}
}

// TestFetch_ObjectStillArrivingNoWait: FILL_TIMEOUT 0 allows no wait
// (§9.20.5), so an Object still arriving is timed out at once rather than
// waited for, even though §3.2 would include it.
func TestFetch_ObjectStillArrivingNoWait(t *testing.T) {
	t.Parallel()
	_, fetcher, _ := arrivingObject(t)
	elems, took := fetchGroup0(t, fetcher, 0)
	if o := object5(elems); o == nil || !o.EndOfTimedOutRange {
		t.Fatalf("element at {0,5} = %+v, want an End of Timed-Out Range", o)
	}
	if took > time.Second {
		t.Fatalf("FETCH with FILL_TIMEOUT 0 took %s; it must not wait", took)
	}
}

// TestFetch_ObjectStillArrivingAbandoned: an Object whose stream is reset
// mid-payload never arrives, and the relay no longer knows it exists: the
// waiting FETCH reports it as an End of Unknown Range as soon as it is
// abandoned, not when the budget runs out.
func TestFetch_ObjectStillArrivingAbandoned(t *testing.T) {
	t.Parallel()
	_, fetcher, slow := arrivingObject(t)
	go func() {
		time.Sleep(200 * time.Millisecond)
		slow.Cancel(moqt.StreamResetCancelled)
	}()
	elems, took := fetchGroup0(t, fetcher, 5*time.Second)
	if o := object5(elems); o == nil || !o.EndOfUnknownRange {
		t.Fatalf("element at {0,5} = %+v, want an End of Unknown Range", o)
	}
	if took > 3*time.Second {
		t.Fatalf("FETCH took %s; an abandoned Object must end the wait", took)
	}
}

// TestFetch_ObjectStillArrivingMaxCacheDurationZero: a FETCH waiting on an
// Object is handed it as it completes, which is forwarding it live, not
// serving it from the cache: a MAX_CACHE_DURATION of 0, which bars the latter
// (§10.3), does not withhold it. The Objects the relay could only serve from
// its cache are unknown.
func TestFetch_ObjectStillArrivingMaxCacheDurationZero(t *testing.T) {
	t.Parallel()
	_, fetcher, slow := arrivingObjectProps(t, trackProp(message.PropertyMaxCacheDuration, 0))
	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = slow.Write([]byte{'r', 's'})
		_ = slow.Close()
	}()
	elems, _ := fetchGroup0(t, fetcher, 3*time.Second)
	if o := object5(elems); o == nil || o.IsEndOfRange() || !bytes.Equal(o.Payload, []byte("pqrs")) {
		t.Fatalf("element at {0,5} = %+v, want Object {0,5} with payload %q", o, "pqrs")
	}
	for _, e := range elems {
		if !e.IsEndOfRange() && e.ObjectID < 5 {
			t.Fatalf("Object {0,%d} served from the cache under MAX_CACHE_DURATION 0", e.ObjectID)
		}
	}
}
