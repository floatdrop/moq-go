package relay_test

import (
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Forwarding: what an Object published upstream looks like on the subscriber's
// stream, and which Objects a subscription's filter lets through.

// TestFanout_PublisherToSubscriberSingleObject: one Object reaches a subscriber
// on another session under the relay-allocated Track Alias.
func TestFanout_PublisherToSubscriberSingleObject(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 7)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	reads := readNextSubgroup(t, subSess)
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 5})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("hello-6a")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := awaitSubgroupRead(t, reads)
	if !slices.Equal(r.payloads, []string{"hello-6a"}) {
		t.Fatalf("payloads = %q, want [hello-6a]", r.payloads)
	}
	if r.header.TrackAlias != subReq.OK.TrackAlias {
		t.Fatalf("forwarded TrackAlias = %d, want %d (subscriber's outbound alias)",
			r.header.TrackAlias, subReq.OK.TrackAlias)
	}
	if r.header.GroupID != 5 {
		t.Fatalf("forwarded GroupID = %d, want 5", r.header.GroupID)
	}
}

// requireFilterForwards publishes Objects 0..n-1 of group 0 to a subscriber
// whose SUBSCRIBE carries filter, and requires it to receive exactly want on
// one stream under its own alias, ending in a FIN. The relay re-encodes the
// Object ID deltas, so they must decode to the publisher's IDs (§11.4.2).
func requireFilterForwards(t *testing.T, filter *message.LocationFilter, n int, want []uint64) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	publishVideoTrack(t, pubSess, "cam1", 7)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess, message.LocationFilterParam(filter))

	reads := readNextSubgroup(t, subSess)
	publishObjects(t, pubSess, 7, 0, n)
	r := awaitSubgroupRead(t, reads)
	if !errors.Is(r.end, io.EOF) {
		t.Fatalf("subscriber read ended with %v, want io.EOF", r.end)
	}
	if !slices.Equal(r.ids, want) {
		t.Fatalf("subscriber saw object IDs %v, want %v", r.ids, want)
	}
	if r.header.TrackAlias != subReq.OK.TrackAlias {
		t.Fatalf("forwarded TrackAlias = %d, want %d", r.header.TrackAlias, subReq.OK.TrackAlias)
	}
}

// TestFanout_AbsoluteStartFilter_DropsObjectsBeforeStart: an AbsoluteStart
// {0, 2} filter drops earlier Objects, and the forwarded deltas decode to the
// publisher's Object IDs (§5.1.2).
func TestFanout_AbsoluteStartFilter_DropsObjectsBeforeStart(t *testing.T) {
	t.Parallel()
	requireFilterForwards(t, &message.LocationFilter{Fields: 2, StartGroup: 0, StartObject: 2}, 5, []uint64{2, 3, 4})
}

// TestFanout_AbsoluteRangeFilter_DropsObjectsOutsideRange: an AbsoluteRange
// {0, 1}..group 0 filter forwards Objects 1, 2, 3 with re-encoded deltas.
func TestFanout_AbsoluteRangeFilter_DropsObjectsOutsideRange(t *testing.T) {
	t.Parallel()
	requireFilterForwards(t,
		&message.LocationFilter{Fields: 3, StartGroup: 0, StartObject: 1, EndGroupDelta: 0}, 4, []uint64{1, 2, 3})
}

// TestFanout_UpdatesTrackEntryLargestObject: each forwarded Object advances the
// track's LargestObject watermark (§10.2.17), observed through a later
// LargestObject-filtered SUBSCRIBE.
func TestFanout_UpdatesTrackEntryLargestObject(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 7)
	go drainAll(t.Context(), newCam1Subscriber(t, pubSess))

	// Objects 0 and 2 of group 4 make the watermark {4, 2}. Object 1 is left
	// for later: sending it twice with another Payload or Subgroup would make
	// the track malformed (§9.1).
	writeSubgroupObjects(t, pub, message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 4},
		[]uint64{0, 2})
	waitRelayLargest(t, pubSess, ns("video"), []byte("cam1"), 4, 2)

	// installSubscribeParams snapshots the watermark for this filter, so of
	// Object 1 (below {4, 2}) and Object 3 (above), only 3 passes. They go on
	// a fresh subgroup, as subgroup 0 already ended.
	sub2 := newCam1Subscriber(t, pubSess, message.LocationFilterParam(&message.LocationFilter{Fields: 2}))
	reads := readNextSubgroup(t, sub2)
	writeSubgroupObjects(t, pub,
		message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 4, SubgroupID: 1},
		[]uint64{1, 3})
	if r := awaitSubgroupRead(t, reads); !slices.Equal(r.ids, []uint64{3}) {
		t.Fatalf("sub2 saw Objects %v, want [3]: the watermark must have been {4, 2} "+
			"so only 3 passes FilterLargestObject", r.ids)
	}
	if ds, ok := tryAcceptDataStream(t, sub2, 200*time.Millisecond); ok {
		t.Fatalf("sub2 got a second data stream %T; only Object 3 passes FilterLargestObject", ds)
	}
}
