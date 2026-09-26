package relay_test

import (
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Downstream stream lifecycle: a subscriber that stops reading, or never
// accepts, only ever blocks its own writer, and an inbound reset reaches the
// subscriber as a reset.

// TestFanout_StalledSubscriberDoesNotBlockFastOne: a subscriber that stops
// reading overflows its own queue without stalling a fast one, which gets every
// Object. The publisher paces itself to the fast reader so, at GOMAXPROCS=1,
// only the stalled queue can overflow.
func TestFanout_StalledSubscriberDoesNotBlockFastOne(t *testing.T) {
	// MaxDropsBeforeReset stays disabled: the stalled writer blocks inside
	// WriteObject on its first Object, so closing its inbox cannot unblock it
	// and the drop-cap reset is unreachable here (the lag-window reset is
	// TestFanout_LagWindowResetsSlowSubscriber's).
	m := &recordingMetrics{}
	pubSess, teardown := connectRelay(t, relay.Config{SendQueueSize: 256, Metrics: m})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 7)
	fastSess := newCam1Subscriber(t, pubSess)
	slowSess := newCam1Subscriber(t, pubSess)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 1})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}

	const sendCount = 600

	// The stalled subscriber accepts its stream but never reads it, so the
	// relay's send window to it fills and its writer inbox overflows. Outbound
	// streams open lazily on the writer's first Object, so the stream only
	// appears once the flood starts.
	slowAccepted := make(chan session.DataStream, 1)
	go func() {
		ds, _ := slowSess.AcceptDataStream(t.Context())
		slowAccepted <- ds
	}()

	// The fast subscriber signals each read on fastRead so the publisher can
	// pace itself to it.
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

	// Staying at most window Objects ahead of the fast reader keeps its inbox
	// (SendQueueSize) from overflowing however goroutines are scheduled.
	const window = 64
	for i := range sendCount {
		if i >= window {
			select {
			case <-fastRead:
			case <-time.After(5 * time.Second):
				t.Fatalf("publisher stalled waiting for fast subscriber at #%d", i)
			}
		}
		if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case got := <-fastReceived:
		if got != sendCount {
			t.Fatalf("fast subscriber received %d, want %d", got, sendCount)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fast subscriber did not drain within deadline")
	}
	// Isolation is only shown if the stalled subscriber actually overflowed.
	if got := m.dropped.Load(); got == 0 {
		t.Fatal("stalled subscriber did not overflow (dropped == 0); test no longer exercises isolation")
	}
	select {
	case ds := <-slowAccepted:
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
	publishVideoTrack(t, pubSess, "cam1", 7)

	// Never accepting, the dead subscriber leaves the relay's header write to
	// it blocked for good on the unbuffered in-process pipes.
	newCam1Subscriber(t, pubSess)
	liveSess := newCam1Subscriber(t, pubSess)

	reads := readNextSubgroup(t, liveSess)
	publishObjects(t, pubSess, 7, 0, 1)
	if r := awaitSubgroupRead(t, reads); !slices.Equal(r.payloads, []string{"A"}) {
		t.Fatalf("live subscriber got %q, want [A]", r.payloads)
	}
}

// TestFanout_InboundResetCancelsDownstream: a reset inbound subgroup stream
// resets the downstream one rather than FINning it (§11.4.3).
func TestFanout_InboundResetCancelsDownstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 7)
	subSess := newCam1Subscriber(t, pubSess)

	reads := readNextSubgroup(t, subSess)
	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, GroupID: 0})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("only")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	sg.Cancel(moqt.StreamResetCancelled)

	r := awaitSubgroupRead(t, reads)
	if len(r.ids) < 1 {
		t.Fatalf("subscriber received %d objects, want >=1 (the one written before the reset)", len(r.ids))
	}
	if errors.Is(r.end, io.EOF) {
		t.Fatal("subscriber stream ended with io.EOF (FIN); want a reset")
	}
}
