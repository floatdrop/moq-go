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

// filterRelayConfig is a relay that accepts Range Filters. It is the zero
// Config: [relay.DefaultMaxFilterRanges] is what a relay advertises unless it
// says otherwise, because §10.3.1.6's own default of 0 prohibits them and a
// relay that inherited it would reject filters it fully implements.
func filterRelayConfig() relay.Config {
	return relay.Config{}
}

// TestSubscribe_RangeFilterProhibitedWhenConfiguredOff pins the negative
// [Config.MaxFilterRanges]: the relay advertises MAX_FILTER_RANGES=0, so a
// SUBSCRIBE carrying a Range Filter is rejected with INVALID_FILTER (§10.3.1.6).
func TestSubscribe_RangeFilterProhibitedWhenConfiguredOff(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{MaxFilterRanges: -1})
	defer teardown()

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"), TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	_, err = subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.RangeFilterParam(&message.RangeFilter{
				Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 1, End: 2}},
			}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidFilter)
}

// TestFanout_ObjectIDRangeFilter pins §5.1.3 object filtering on live fanout: an
// OBJECTID_FILTER selecting [1,2] drops object 0 and 3, so the subscriber sees
// only IDs 1 and 2 (with deltas re-encoded against the forwarded IDs).
func TestFanout_ObjectIDRangeFilter(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, filterRelayConfig())
	defer teardown()

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"), TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.RangeFilterParam(&message.RangeFilter{
				Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 1, End: 2}},
			}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	type readResult struct {
		ids []uint64
		err error
	}
	resCh := make(chan readResult, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			resCh <- readResult{err: err}
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			resCh <- readResult{err: errors.New("not a SubgroupStream")}
			return
		}
		var (
			ids       []uint64
			prev      uint64
			haveFirst bool
		)
		for {
			obj, err := sg.ReadObject()
			if err != nil {
				resCh <- readResult{ids: ids, err: err}
				return
			}
			var absID uint64
			if !haveFirst {
				absID = obj.ObjectIDDelta
				haveFirst = true
			} else {
				absID = prev + obj.ObjectIDDelta + 1
			}
			prev = absID
			ids = append(ids, absID)
		}
	}()

	sg0, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for i := range 4 {
		if err := sg0.WriteObject(&message.SubgroupObject{Payload: []byte{byte('A' + i)}}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := sg0.Close(); err != nil {
		t.Fatalf("sg0.Close: %v", err)
	}

	select {
	case res := <-resCh:
		if !errors.Is(res.err, io.EOF) {
			t.Fatalf("subscriber read ended with %v, want io.EOF", res.err)
		}
		if want := []uint64{1, 2}; !reflect.DeepEqual(res.ids, want) {
			t.Fatalf("subscriber saw object IDs %v, want %v (OBJECTID_FILTER [1,2])", res.ids, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not drain within deadline")
	}
}

// TestSubscribeTracks_TrackPropertyFilter pins §5.1.3: a TRACK_PROPERTY_FILTER
// on a SUBSCRIBE_TRACKS forwards only PUBLISH messages whose Track Properties
// pass — a matching track is forwarded, a non-matching one is suppressed.
func TestSubscribeTracks_TrackPropertyFilter(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, filterRelayConfig())
	defer teardown()

	const propType = 0x40 // even → single-integer Track Property
	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
		Parameters: message.Parameters{
			message.RangeFilterParam(&message.RangeFilter{
				Type: message.ParamTrackPropertyFilter, PropertyType: propType,
				Ranges: []message.Range{{Start: 10, End: 20}},
			}),
		},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pub := func(name string, propVal uint64) {
		s, err := dialAnotherClient(t, subSess).Publish(t.Context(), &message.Publish{
			Namespace:       wire.TrackNamespace{[]byte("video"), []byte(name)},
			Name:            []byte(name),
			TrackProperties: message.AppendTrackProperties([]wire.KVPair{{Type: propType, IntVal: propVal}}),
		})
		if err != nil {
			t.Fatalf("Publish %s: %v", name, err)
		}
		t.Cleanup(func() { s.Close() })
	}

	// cam2's property (99) is outside [10,20] → suppressed; cam1's (15) passes.
	pub("cam2", 99)
	pub("cam1", 15)

	req, err := subSess.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	got, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("got %T, want *message.Publish", req.First)
	}
	if string(got.Name) != "cam1" {
		t.Fatalf("forwarded PUBLISH Name = %q, want cam1 (cam2 must be filtered out)", got.Name)
	}

	// No second PUBLISH: cam2 was filtered by the TRACK_PROPERTY_FILTER.
	done := make(chan struct{})
	go func() {
		_, _ = subSess.AcceptRequest(t.Context())
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("a second PUBLISH arrived; cam2 should have been filtered")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestFetch_ObjectIDRangeFilter pins §5.1.3 filtering on the FETCH serve path:
// an OBJECTID_FILTER selecting [1,2] returns only the matching cached objects.
func TestFetch_ObjectIDRangeFilter(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, filterRelayConfig())
	defer teardown()

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"), TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	// An unfiltered subscriber lets the fanout accept + cache the objects.
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()
	go drainAllStreams(t.Context(), subSess)

	publishObjects(t, pubSess, publisherAlias, 0 /*group*/, 4 /*count → IDs 0..3*/)
	time.Sleep(50 * time.Millisecond) // let the cache settle

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t, fetchSess,
		wire.TrackNamespace{[]byte("video")}, []byte("cam1"),
		message.Location{Group: 0, Object: 0}, message.Location{Group: 0, Object: 4},
		message.GroupOrderAscending,
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 1, End: 2}},
		}),
	)

	var gotIDs []uint64
	for _, o := range objs {
		gotIDs = append(gotIDs, o.object)
	}
	if want := []uint64{1, 2}; !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("FETCH returned object IDs %v, want %v (OBJECTID_FILTER [1,2])", gotIDs, want)
	}
}

// TestFetch_SubgroupFilterSelectsOneLayer is the temporal-layer backfill a live
// media application needs, on a relay configured with nothing.
//
// A group carrying L1T2 video is two subgroups: subgroup 0 is the base layer,
// which every later base frame references back to the keyframe, and subgroup 1
// is an enhancement layer nothing references. A subscriber joining mid-group
// needs the base layer replayed from the keyframe and none of the enhancement
// layer — those frames are already past and nothing depends on them.
//
// SUBGROUP_FILTER is what says that, and it is the difference between a
// streamable backfill and a buffered one: a FETCH answers in ascending Object
// ID, so filtered to one subgroup the response is already decode order and can
// go frame by frame to a decoder, where the unfiltered answer interleaves two
// layers' ID ranges and has to be held whole and sorted first.
//
// The relay is [relay.Config]{} — the point of the test. §10.3.1.6's own
// MAX_FILTER_RANGES default is 0, which prohibits Range Filters, so a relay
// that inherited it would answer INVALID_FILTER to a filter it implements.
func TestFetch_SubgroupFilterSelectsOneLayer(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"), TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	// An unfiltered subscriber lets the fanout accept + cache the objects.
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()
	go drainAllStreams(t.Context(), subSess)

	// Each layer numbers its objects from its own base, because IDs must be
	// unique within a group and consecutive within a subgroup at once. Base
	// layer at 0..2, enhancement at stride..stride+1.
	const stride = uint64(1) << 16
	publishLayer(t, pubSess, publisherAlias, 0 /*group*/, 0 /*subgroup*/, 0, 3)
	publishLayer(t, pubSess, publisherAlias, 0 /*group*/, 1 /*subgroup*/, stride, 2)
	time.Sleep(50 * time.Millisecond) // let the cache settle

	fetchSess := dialAnotherClient(t, pubSess)
	_, objs := fetchAndDrain(t, fetchSess,
		wire.TrackNamespace{[]byte("video")}, []byte("cam1"),
		message.Location{Group: 0, Object: 0}, message.Location{Group: 0, Object: 0}, // whole group
		message.GroupOrderAscending,
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamSubgroupFilter, Ranges: []message.Range{{Start: 0, End: 0}},
		}),
	)

	var gotIDs []uint64
	for _, o := range objs {
		gotIDs = append(gotIDs, o.object)
	}
	if want := []uint64{0, 1, 2}; !reflect.DeepEqual(gotIDs, want) {
		t.Fatalf("FETCH returned object IDs %v, want %v — SUBGROUP_FILTER [0,0] must "+
			"return the base layer alone, in ascending ID, with no enhancement objects",
			gotIDs, want)
	}
}

// publishLayer writes count objects to one subgroup of one group, numbering
// them consecutively from firstObjectID. Unlike publishObjects it names the
// subgroup and the ID base, which is what a temporal-layer layout needs.
func publishLayer(
	t *testing.T,
	pubSess *session.Session,
	trackAlias, group, subgroup, firstObjectID uint64,
	count int,
) {
	t.Helper()
	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     trackAlias,
		GroupID:        group,
		SubgroupID:     subgroup,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup g=%d sg=%d: %v", group, subgroup, err)
	}
	for i := range count {
		if err := sg.WriteObjectAt(firstObjectID+uint64(i), &message.SubgroupObject{
			Payload: []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObjectAt g=%d sg=%d #%d: %v", group, subgroup, i, err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("sg.Close g=%d sg=%d: %v", group, subgroup, err)
	}
}
