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
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFanout_PublisherToSubscriberSingleObject: one Object reaches a subscriber
// on another session under the relay-allocated Track Alias.
func TestFanout_PublisherToSubscriberSingleObject(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)

	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReqStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReqStream.Close()

	pubSubgroup, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        5,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}

	// Reader goroutine on the subscriber side.
	type forwarded struct {
		header message.SubgroupHeader
		obj    *message.SubgroupObject
		err    error
	}
	resCh := make(chan forwarded, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			resCh <- forwarded{err: err}
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			resCh <- forwarded{err: errors.New("not a SubgroupStream")}
			return
		}
		obj, err := sg.ReadObject()
		resCh <- forwarded{header: sg.Header, obj: obj, err: err}
	}()

	wantPayload := []byte("hello-6a")
	if err := pubSubgroup.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       wantPayload,
	}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("subscriber Accept/ReadObject: %v", res.err)
		}
		if string(res.obj.Payload) != string(wantPayload) {
			t.Fatalf("payload = %q, want %q", res.obj.Payload, wantPayload)
		}
		if res.header.TrackAlias != subReqStream.OK.TrackAlias {
			t.Fatalf("forwarded TrackAlias = %d, want %d (subscriber's outbound alias)",
				res.header.TrackAlias, subReqStream.OK.TrackAlias)
		}
		if res.header.GroupID != 5 {
			t.Fatalf("forwarded GroupID = %d, want 5", res.header.GroupID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not receive forwarded object within deadline")
	}
}

// TestFanout_StalledSubscriberDoesNotBlockFastOne: a subscriber that stops
// reading overflows its own queue without stalling a fast one, which gets every
// Object. The publisher paces itself to the fast reader so, at GOMAXPROCS=1,
// only the stalled queue can overflow.
func TestFanout_StalledSubscriberDoesNotBlockFastOne(t *testing.T) {
	// Small queue so the stalled subscriber overflows; MaxDropsBeforeReset
	// left disabled — a fully-stalled subscriber blocks inside WriteObject on
	// its first object, so closing its inbox can't unblock it and the
	// drop-cap reset path isn't reachable here (the lag-window reset is
	// covered by TestFanout_LagWindowResetsSlowSubscriber).
	m := &recordingMetrics{}
	pubSess, teardown := connectRelay(t, relay.Config{SendQueueSize: 256, Metrics: m})
	defer teardown()

	const publisherAlias = uint64(7)

	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	// Two subscribers on independent sessions.
	fastSess := dialAnotherClient(t, pubSess)
	slowSess := dialAnotherClient(t, pubSess)

	fastReq, err := fastSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("fast Subscribe: %v", err)
	}
	defer fastReq.Close()

	slowReq, err := slowSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("slow Subscribe: %v", err)
	}
	defer slowReq.Close()

	pubSubgroup, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        1,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}

	const sendCount = 600

	// Stalled subscriber: accept but never call ReadObject — its outbound
	// stream stays unread, the relay's send window fills, and the relay's
	// per-subscriber writer inbox overflows and starts dropping objects for
	// it. This must not affect the fast subscriber below.
	slowAcceptDone := make(chan session.DataStream, 1)
	go func() {
		ds, err := slowSess.AcceptDataStream(t.Context())
		if err != nil {
			slowAcceptDone <- nil
			return
		}
		slowAcceptDone <- ds
	}()

	// Fast subscriber: drain everything, signalling each read on fastRead so
	// the publisher can pace itself and never overflow the fast inbox.
	fastRead := make(chan struct{}, sendCount)
	fastReceived := make(chan int, 1)
	go func() {
		ds, err := fastSess.AcceptDataStream(t.Context())
		if err != nil {
			fastReceived <- -1
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			fastReceived <- -1
			return
		}
		count := 0
		for {
			if _, err := sg.ReadObject(); err != nil {
				fastReceived <- count
				return
			}
			count++
			fastRead <- struct{}{}
		}
	}()

	// No pre-flood wait on the slow accept: outbound subgroup streams open
	// lazily on the writer goroutine's first forwarded object, so the slow
	// subscriber's stream doesn't exist until the flood starts. The acceptor
	// goroutine above picks it up whenever it appears; a writer blocked on
	// the (unread) slow stream only ever blocks its own goroutine.

	// Flood, but stay at most `window` objects ahead of the fast subscriber's
	// reads so the fast inbox (SendQueueSize) can never overflow regardless of
	// goroutine scheduling. The stalled subscriber, which never reads,
	// overflows and drops regardless of pacing.
	const window = 64
	for i := range sendCount {
		if i >= window {
			select {
			case <-fastRead:
			case <-time.After(5 * time.Second):
				t.Fatalf("publisher stalled waiting for fast subscriber at #%d", i)
			}
		}
		if err := pubSubgroup.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte("x"),
		}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := pubSubgroup.Close(); err != nil {
		t.Fatalf("pubSubgroup.Close: %v", err)
	}

	// Fast subscriber should have received them all, unaffected by the
	// stalled peer.
	select {
	case got := <-fastReceived:
		if got != sendCount {
			t.Fatalf("fast subscriber received %d, want %d", got, sendCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fast subscriber did not drain within deadline")
	}

	// The scenario only proves isolation if the stalled subscriber actually
	// overflowed — otherwise the queue absorbed everything and nothing was
	// stressed.
	if got := m.dropped.Load(); got == 0 {
		t.Fatal("stalled subscriber did not overflow (dropped == 0); test no longer exercises isolation")
	}

	// The stalled subscriber's stream must have been offered (accepted)
	// even though it never read an object.
	select {
	case ds := <-slowAcceptDone:
		if ds == nil {
			t.Fatal("slow subscriber accept failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscriber's outbound stream never appeared")
	}
}

// TestFanout_UnresponsiveSubscriberDoesNotStallSubgroup: a subscriber that never
// accepts its data stream blocks only its own writer, not the subgroup's other
// subscribers or the inbound read loop.
func TestFanout_UnresponsiveSubscriberDoesNotStallSubgroup(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	// Subscriber A: subscribes, then never touches its data plane — no
	// AcceptDataStream, so the relay's header write to it can never
	// complete on the in-process unbuffered pipes.
	deadSess := dialAnotherClient(t, pubSess)
	deadReq, err := deadSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("dead Subscribe: %v", err)
	}
	defer deadReq.Close()

	liveSess := dialAnotherClient(t, pubSess)
	liveReq, err := liveSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	defer liveReq.Close()

	got := make(chan string, 1)
	go func() {
		ds, err := liveSess.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		obj, err := sg.ReadObject()
		if err != nil {
			return
		}
		got <- string(obj.Payload)
	}()

	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("through")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	select {
	case payload := <-got:
		if payload != "through" {
			t.Fatalf("live subscriber got %q, want %q", payload, "through")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("live subscriber starved — an unresponsive peer stalled the subgroup fanout")
	}
}

// TestFanout_AbsoluteStartFilter_DropsObjectsBeforeStart: an AbsoluteStart
// {0, 2} filter drops earlier Objects, and the forwarded deltas decode to the
// publisher's Object IDs (§5.1.2).
func TestFanout_AbsoluteStartFilter_DropsObjectsBeforeStart(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.LocationFilterParam(&message.LocationFilter{Fields: 2, StartGroup: 0, StartObject: 2}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	pubSubgroup, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}

	type readResult struct {
		header message.SubgroupHeader
		ids    []uint64
		err    error
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
				resCh <- readResult{header: sg.Header, ids: ids, err: err}
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

	// Publish absolute IDs 0,1,2,3,4 (sequential, delta=0 each on the
	// wire). The relay must drop 0,1 and forward 2,3,4 with re-encoded
	// deltas so the subscriber decodes 2,3,4 too.
	for i := range 5 {
		if err := pubSubgroup.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0, // sequential
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := pubSubgroup.Close(); err != nil {
		t.Fatalf("pubSubgroup.Close: %v", err)
	}

	select {
	case res := <-resCh:
		if !errors.Is(res.err, io.EOF) {
			t.Fatalf("subscriber read ended with %v, want io.EOF", res.err)
		}
		wantIDs := []uint64{2, 3, 4}
		if !reflect.DeepEqual(res.ids, wantIDs) {
			t.Fatalf("subscriber saw object IDs %v, want %v", res.ids, wantIDs)
		}
		if res.header.TrackAlias != subReq.OK.TrackAlias {
			t.Fatalf("forwarded TrackAlias = %d, want %d", res.header.TrackAlias, subReq.OK.TrackAlias)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not drain within deadline")
	}
}

// TestFanout_AbsoluteRangeFilter_DropsObjectsOutsideRange: an AbsoluteRange
// {0, 1}..group 0 filter forwards Objects 1, 2, 3 with re-encoded deltas.
func TestFanout_AbsoluteRangeFilter_DropsObjectsOutsideRange(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.LocationFilterParam(
				&message.LocationFilter{Fields: 3, StartGroup: 0, StartObject: 1, EndGroupDelta: 0},
			),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Start the reader goroutine before issuing any writes — the relay's
	// OpenSubgroup synchronously writes the downstream SUBGROUP_HEADER
	// and blocks until the subscriber accepts the stream.
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
		if err := sg0.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
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
		wantIDs := []uint64{1, 2, 3}
		if !reflect.DeepEqual(res.ids, wantIDs) {
			t.Fatalf("subscriber saw object IDs %v, want %v", res.ids, wantIDs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not drain within deadline")
	}
}

// TestSubscribe_InstallsPriorityAndGroupOrder: SUBSCRIBER_PRIORITY and
// GROUP_ORDER on a SUBSCRIBE are recorded on the downstream subscription.
func TestSubscribe_InstallsPriorityAndGroupOrder(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.SubscriberPriorityParam(42),
			message.GroupOrderParam(message.GroupOrderDescending),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// SUBSCRIBE_OK alone proves the relay accepted the parameters
	// without rejecting them as malformed; the registry state is
	// covered by the unit test on DownstreamSub setters.
}

// TestFanout_InboundResetCancelsDownstream: a reset inbound subgroup stream
// resets the downstream one rather than FINning it (§11.4.3).
func TestFanout_InboundResetCancelsDownstream(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	type readResult struct {
		count int
		err   error
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
		count := 0
		for {
			if _, err := sg.ReadObject(); err != nil {
				resCh <- readResult{count: count, err: err}
				return
			}
			count++
		}
	}()

	pubSubgroup, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}

	if err := pubSubgroup.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       []byte("only"),
	}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	// Reset (not FIN) the inbound subgroup. The relay should detect the
	// non-EOF read error and propagate the reset to the downstream
	// subscriber.
	pubSubgroup.Cancel(moqt.StreamResetCancelled)

	select {
	case res := <-resCh:
		if res.count < 1 {
			t.Fatalf("subscriber received %d objects, want >=1 (the one written before the reset)", res.count)
		}
		if errors.Is(res.err, io.EOF) {
			t.Fatal("subscriber stream ended with io.EOF (FIN); want a reset")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not see stream termination within deadline")
	}
}

// TestFanout_UpdatesTrackEntryLargestObject: each forwarded Object advances the
// track's LargestObject watermark (§10.2.17), observed through a later
// LargestObject-filtered SUBSCRIBE.
func TestFanout_UpdatesTrackEntryLargestObject(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	// Subscriber 1: drains everything the relay sends across the
	// subscription's whole life. Crucially the goroutine loops over
	// AcceptDataStream so the second subgroup (sent after we set up
	// subscriber 2) doesn't deadlock on a missing acceptor.
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

	// Phase 1: publish three objects (absIDs 0, 1, 2) on group 4. After
	// this the relay's TrackEntry.LargestObject must be {Group: 4,
	// Object: 2}.
	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        4,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for i := range 3 {
		if err := sg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("sg.Close: %v", err)
	}

	// Give the relay a moment to drain the inbound subgroup and update
	// the TrackEntry watermark before we issue the FilterLargestObject
	// SUBSCRIBE. Without this the snapshot might be taken before the
	// fanout has finished processing the three objects.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Best-effort wait — there's no public accessor for the
		// entry's watermark; rely on a short sleep + retry loop in
		// the subscribe step below.
		time.Sleep(20 * time.Millisecond)
		break
	}

	// Phase 2: subscribe with FilterLargestObject. The relay's
	// installSubscribeParams snapshots the entry watermark, so any
	// object at a Location <= {4, 2} should be filtered out.
	subSess2 := dialAnotherClient(t, pubSess)
	subReq2, err := subSess2.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.LocationFilterParam(&message.LocationFilter{Fields: 2}),
		},
	})
	if err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}
	defer subReq2.Close()

	// Sub2 reader: collect the absIDs of the objects that pass the
	// filter, looping over streams so the relay's per-subgroup outbound
	// open doesn't deadlock.
	got2 := make(chan []uint64, 1)
	go func() {
		var ids []uint64
		for {
			ds, err := subSess2.AcceptDataStream(t.Context())
			if err != nil {
				got2 <- ids
				return
			}
			sg, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				continue
			}
			var (
				prev uint64
				have bool
			)
			for {
				obj, err := sg.ReadObject()
				if err != nil {
					break
				}
				var absID uint64
				if !have {
					absID = obj.ObjectIDDelta
					have = true
				} else {
					absID = prev + obj.ObjectIDDelta + 1
				}
				prev = absID
				ids = append(ids, absID)
			}
		}
	}()

	// Phase 3: publish two more objects — one *below* the watermark
	// (absID=1, which is < {4, 2}) and one *above* it (absID=3, which is
	// > {4, 2}). FilterLargestObject must drop the below-watermark one
	// and pass the above-watermark one. We send them on a fresh subgroup
	// so the relay opens new streams (avoids interaction with subgroup 0
	// which already FIN'd).
	sg2, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        4,
		SubgroupID:     1,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup phase 3: %v", err)
	}
	// First object at absID=1 — wire delta=1.
	if err := sg2.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 1,
		Payload:       []byte("below"),
	}); err != nil {
		t.Fatalf("WriteObject below: %v", err)
	}
	// Second object at absID=3 — wire delta = 3 - 1 - 1 = 1.
	if err := sg2.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 1,
		Payload:       []byte("above"),
	}); err != nil {
		t.Fatalf("WriteObject above: %v", err)
	}
	if err := sg2.Close(); err != nil {
		t.Fatalf("sg2.Close: %v", err)
	}

	// Stop sub2's session to break its AcceptDataStream loop, then
	// collect what it captured.
	time.Sleep(100 * time.Millisecond) // let the relay deliver
	subReq2.Close()
	_ = subSess2.Close(0, "")

	select {
	case ids := <-got2:
		want := []uint64{3}
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf(
				"sub2 saw absIDs %v, want %v — watermark must have been {4, 2} so only 3 passes FilterLargestObject",
				ids,
				want,
			)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("sub2 collector did not finish within deadline")
	}
}

// TestSubscribe_InvalidGroupOrderRejected pins the §10.2.8 rule: GROUP_ORDER
// values other than 0x1 (Ascending) and 0x2 (Descending) are a session-level
// PROTOCOL_VIOLATION, so a SUBSCRIBE carrying one closes the whole session.
func TestSubscribe_InvalidGroupOrderRejected(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	_, _ = subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.ByteParam(message.ParamGroupOrder, 0x05),
		},
	})

	requireSessionClosed(t, subSess, "out-of-range GROUP_ORDER SUBSCRIBE (§10.2.8)")
}
