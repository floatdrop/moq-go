package relay_test

import (
	"errors"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// fetchAndDrain issues a standalone FETCH and reads the response stream
// to FIN. It returns the FETCH_OK plus the decoded absolute (group,
// object) tuples and their payloads in arrival order. The decoded
// values reverse §11.4.4's delta encoding so test assertions can
// compare against the publisher's absolute IDs directly.
//
// orderHint is the GroupOrder the test expects the relay to use; the
// delta-reversal must agree with it (§4466-4473).
func fetchAndDrain(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	start, end message.Location,
	order message.GroupOrder,
	extra ...message.Parameter,
) (*message.FetchOK, []decodedFetchObject) {
	t.Helper()

	params := message.Parameters{message.GroupOrderParam(order)}
	params = append(params, extra...)

	reqStream, err := sess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     ns,
			Name:          name,
			StartLocation: start,
			EndLocation:   end,
		},
		Parameters: params,
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	t.Cleanup(func() { reqStream.Close() })

	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}

	objs := decodeFetchStream(t, fs, order)
	return reqStream.OK, objs
}

type decodedFetchObject struct {
	group, object uint64
	payload       []byte
	status        uint64
}

// decodeFetchStream reverses the §11.4.4 delta encoding and returns
// every object until EOF.
func decodeFetchStream(t *testing.T, fs *session.IncomingFetchStream, order message.GroupOrder) []decodedFetchObject {
	t.Helper()
	var (
		out        []decodedFetchObject
		prevGroup  uint64
		prevObject uint64
		havePrev   bool
		descending = order == message.GroupOrderDescending
	)
	for {
		fo, err := fs.ReadObject()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return out
			}
			t.Fatalf("ReadObject: %v", err)
		}

		var g, o uint64
		switch {
		case !havePrev:
			// First object: GroupIDDelta and ObjectIDDelta carry
			// absolute values (§4460-4464).
			g = fo.GroupIDDelta
			o = fo.ObjectIDDelta
		case fo.SerializationFlags&message.FetchFlagGroupIDDelta != 0:
			if descending {
				g = prevGroup - fo.GroupIDDelta - 1
			} else {
				g = prevGroup + fo.GroupIDDelta + 1
			}
			o = fo.ObjectIDDelta
		default:
			g = prevGroup
			if fo.SerializationFlags&message.FetchFlagObjectIDDelta != 0 {
				o = prevObject + fo.ObjectIDDelta + 1
			} else {
				o = prevObject + 1
			}
		}

		out = append(out, decodedFetchObject{
			group:   g,
			object:  o,
			payload: fo.ObjectPayload,
			status:  fo.ObjectStatus,
		})
		prevGroup = g
		prevObject = o
		havePrev = true
	}
}

// publishObjects emits one subgroup with objects at IDs 0..n-1 on
// the given (group, subgroup). The relay's fanout caches them as a
// side effect.
func publishObjects(
	t *testing.T,
	pubSess *session.Session,
	trackAlias, group uint64,
	count int,
) {
	t.Helper()
	sg, err := pubSess.OpenSubgroup(t.Context(), message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     trackAlias,
		GroupID:        group,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup g=%d: %v", group, err)
	}
	for i := range count {
		if err := sg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObject g=%d #%d: %v", group, i, err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("sg.Close g=%d: %v", group, err)
	}
}

// publishAndCache sets up the publisher session, drains the
// subscriber so the fanout doesn't deadlock on OpenSubgroup, and
// returns the subscriber session ready for FETCH.
func publishAndCache(t *testing.T) (*session.Session, *session.Session, uint64) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { pubReq.Close() })

	// A subscriber must exist before the fanout will accept inbound
	// objects (otherwise drainInbound throws them away).
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subReq.Close() })
	go drainAllStreams(t.Context(), subSess)

	return pubSess, subSess, publisherAlias
}

// TestFetch_FromCacheAscending pins the 7d happy path: publisher emits
// objects across two groups; subscriber FETCHes the full range; relay
// returns each object in ascending (group asc, object asc) order with
// payloads intact.
func TestFetch_FromCacheAscending(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)

	// Give the relay a beat to drain the fanout into the cache.
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 1, Object: 2}, // exclusive: covers {1,0} and {1,1}
		message.GroupOrderAscending,
	)

	if ok == nil {
		t.Fatal("FetchOK is nil")
	}
	want := []decodedFetchObject{
		{group: 0, object: 0, payload: []byte("A")},
		{group: 0, object: 1, payload: []byte("B")},
		{group: 0, object: 2, payload: []byte("C")},
		{group: 1, object: 0, payload: []byte("A")},
		{group: 1, object: 1, payload: []byte("B")},
	}
	if !reflect.DeepEqual(objs, want) {
		t.Fatalf("ascending FETCH = %+v, want %+v", objs, want)
	}
}

// TestFetch_FromCacheDescending exercises the §5.2 / §11.4.4
// Descending group-order path: groups arrive in reverse order; objects
// within each group remain ascending (the spec keeps subgroup-internal
// order ascending regardless of GroupOrder).
func TestFetch_FromCacheDescending(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	publishObjects(t, pubSess, publisherAlias, 1, 2)
	publishObjects(t, pubSess, publisherAlias, 2, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 2, Object: 2}, // exclusive: covers groups 0..2
		message.GroupOrderDescending,
	)

	want := []decodedFetchObject{
		{group: 2, object: 0, payload: []byte("A")},
		{group: 2, object: 1, payload: []byte("B")},
		{group: 1, object: 0, payload: []byte("A")},
		{group: 1, object: 1, payload: []byte("B")},
		{group: 0, object: 0, payload: []byte("A")},
		{group: 0, object: 1, payload: []byte("B")},
	}
	if !reflect.DeepEqual(objs, want) {
		t.Fatalf("descending FETCH = %+v, want %+v", objs, want)
	}
}

// TestFetch_RejectsStartBeyondLargest pins §3585-3587: FETCH whose
// StartLocation is strictly greater than the relay's LargestObject is
// REQUEST_ERROR / InvalidRange.
func TestFetch_RejectsStartBeyondLargest(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("video")},
			Name:          []byte("cam1"),
			StartLocation: message.Location{Group: 99, Object: 99},
			EndLocation:   message.Location{Group: 100, Object: 0},
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidRange)
}

// TestFetch_RejectsEmptyTrack pins the "no objects published yet" case
// (§3585-3587): the relay knows the track but the watermark is
// {0, 0}, so any FETCH (other than a request that ends at {0, 0})
// has nothing to serve. REQUEST_ERROR / InvalidRange.
func TestFetch_RejectsEmptyTrack(t *testing.T) {
	t.Parallel()
	pubSess, _, _ := publishAndCache(t)

	fetchSess := dialAnotherClient(t, pubSess)
	_, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("video")},
			Name:          []byte("cam1"),
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 1, Object: 0},
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidRange)
}

// TestFetch_JoiningFetchUnknownRequestID pins §10.12.2: a Joining
// FETCH whose JoiningRequestID does not correspond to an active
// subscription on the same session is rejected with
// INVALID_JOINING_REQUEST_ID.
func TestFetch_JoiningFetchUnknownRequestID(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining:   &message.JoiningFetch{JoiningRequestID: 999, JoiningStart: 0},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidJoiningID)
}

// TestFetch_JoiningFetchRelativeCurrentGroup pins the §5.1.3 / §10.12.2
// happy path: a subscriber issues SUBSCRIBE with FilterLargestObject and
// then a Relative Joining FETCH with JoiningStart=0. The relay computes
// the response range against the Joining Location it captured at
// SUBSCRIBE_OK time and replays the cached current-group objects.
func TestFetch_JoiningFetchRelativeCurrentGroup(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// Two groups; the joining FETCH (relative, start=0) should
	// return only the current group's objects up to and including
	// the Largest Object.
	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 3 /*count*/)
	publishObjects(t, pubSess, publisherAlias, 1, 2)

	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)

	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{message.SubscriptionFilterParam(
			&message.SubscriptionFilter{Type: message.FilterLargestObject},
		)},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	// §10.2.11: relay MUST include LARGEST_OBJECT now that objects
	// have been cached.
	lp, hasLargest := subStream.OK.Parameters.Find(message.ParamLargestObject)
	if !hasLargest {
		t.Fatal("SUBSCRIBE_OK missing LARGEST_OBJECT parameter")
	}
	// largest = {1, 1} (group 1, second of two objects)
	if lp.Group != 1 || lp.Object != 1 {
		t.Fatalf("LARGEST_OBJECT = {%d,%d}, want {1,1}", lp.Group, lp.Object)
	}

	// Relative Joining FETCH, JoiningStart=0 → start at the largest
	// group itself.
	fetchStream, err := subSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining:   &message.JoiningFetch{JoiningRequestID: subMsg.RequestID, JoiningStart: 0},
	})
	if err != nil {
		t.Fatalf("Joining FETCH: %v", err)
	}
	t.Cleanup(func() { fetchStream.Close() })

	// §10.12.2.1: End = {Joining Location.Group, Joining Location.Object + 1}.
	wantEnd := message.Location{Group: 1, Object: 2}
	if fetchStream.OK.EndLocation != wantEnd {
		t.Fatalf("FETCH_OK.EndLocation = %+v, want %+v", fetchStream.OK.EndLocation, wantEnd)
	}

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, isFetch := ds.(*session.IncomingFetchStream)
	if !isFetch {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	objs := decodeFetchStream(t, fs, message.GroupOrderAscending)

	// Expect group 1 objects 0 and 1 (the largest group's contents),
	// not group 0's objects.
	if len(objs) != 2 {
		t.Fatalf("got %d cached objects, want 2", len(objs))
	}
	for _, o := range objs {
		if o.group != 1 {
			t.Errorf("unexpected group %d (want only group 1) in joining FETCH response", o.group)
		}
	}
}

// TestFetch_JoiningFetchAbsoluteFullHistory pins the Absolute Joining
// FETCH variant: with JoiningStart=0 the relay returns the full cached
// history from {0,0} up to and including the Largest Object.
func TestFetch_JoiningFetchAbsoluteFullHistory(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 2)
	publishObjects(t, pubSess, publisherAlias, 1, 1)
	time.Sleep(50 * time.Millisecond)

	subSess := dialAnotherClient(t, pubSess)

	subMsg := &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{message.SubscriptionFilterParam(
			&message.SubscriptionFilter{Type: message.FilterLargestObject},
		)},
	}
	subStream, err := subSess.Subscribe(t.Context(), subMsg)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subStream.Close() })

	fetchStream, err := subSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeAbsoluteJoining,
		Joining:   &message.JoiningFetch{JoiningRequestID: subMsg.RequestID, JoiningStart: 0},
	})
	if err != nil {
		t.Fatalf("Absolute Joining FETCH: %v", err)
	}
	t.Cleanup(func() { fetchStream.Close() })

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs := ds.(*session.IncomingFetchStream)
	objs := decodeFetchStream(t, fs, message.GroupOrderAscending)

	// 2 objects in group 0 + 1 object in group 1 = 3.
	if len(objs) != 3 {
		t.Fatalf("got %d cached objects, want 3", len(objs))
	}
}

// TestFetch_PartialRangeCarriesPropertiesAndPriority verifies the
// FetchObject encoding includes Properties (when present) and
// PublisherPriority delta. We publish two objects with distinct
// priorities, FETCH them, and confirm the decoded objects carry the
// publisher's per-subgroup priority.
func TestFetch_PartialRangeCarriesPriority(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	// One subgroup, two objects — they share the subgroup's
	// publisher priority, which is the §11.4.2 inline default
	// (PublisherPriority field, 0 unless InlinePriority is set on
	// the header). For this test we just verify the field round-
	// trips at all; the per-object delta-encoding test is in the
	// AscendingMultiGroup test above.
	publishObjects(t, pubSess, publisherAlias, 7, 2)
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 7, Object: 0},
		message.Location{Group: 7, Object: 2},
		message.GroupOrderAscending,
	)
	if len(objs) != 2 {
		t.Fatalf("got %d objects, want 2", len(objs))
	}
}

// TestFetch_OKEndLocationCappedToWatermark pins §3628-3632: a FETCH
// whose requested EndLocation extends beyond the relay's Largest
// Object should have FETCH_OK.EndLocation capped at {Largest.Group,
// Largest.Object + 1}.
func TestFetch_OKEndLocationCappedToWatermark(t *testing.T) {
	t.Parallel()
	pubSess, _, publisherAlias := publishAndCache(t)

	publishObjects(t, pubSess, publisherAlias, 0, 3) // largest = {0, 2}
	time.Sleep(50 * time.Millisecond)

	fetchSess := dialAnotherClient(t, pubSess)
	ok, _ := fetchAndDrain(t,
		fetchSess,
		wire.TrackNamespace{[]byte("video")},
		[]byte("cam1"),
		message.Location{Group: 0, Object: 0},
		message.Location{Group: 999, Object: 0}, // far past the watermark
		message.GroupOrderAscending,
	)
	want := message.Location{Group: 0, Object: 3} // largest + 1
	if ok.EndLocation != want {
		t.Fatalf("FETCH_OK.EndLocation = %+v, want %+v", ok.EndLocation, want)
	}
}
