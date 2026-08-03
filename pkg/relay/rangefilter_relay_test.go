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

// filterRelayConfig advertises a non-zero MAX_FILTER_RANGES so the relay
// accepts Range Filters (§10.3.1.6 default 0 prohibits them).
func filterRelayConfig() relay.Config {
	return relay.Config{SessionOptions: []session.Option{session.WithMaxFilterRanges(16)}}
}

// TestSubscribe_RangeFilterProhibitedByDefault pins §10.3.1.6: with the default
// MAX_FILTER_RANGES=0 a SUBSCRIBE carrying a Range Filter is rejected with
// INVALID_FILTER.
func TestSubscribe_RangeFilterProhibitedByDefault(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
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
