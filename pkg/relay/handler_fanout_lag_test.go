package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFanout_LagWindowResetsSlowSubscriber pins §8 latency-window
// backpressure. A subscriber that accepts its stream but then stalls lets the
// relay's send queue back up; once a queued object has waited longer than
// MaxFanoutLag the relay resets the outbound stream and terminates the
// subscription. This is a latency measure, not a cumulative drop count: a
// subscriber that keeps up (the fast path in
// TestFanout_SlowSubscriberGetsResetWithoutBlockingFastOne) is left alone.
//
// The in-process transport is synchronous (a write blocks until the peer
// reads), so while the subscriber stalls the relay's first WriteObject blocks
// and the remaining objects age in the queue. When the subscriber finally
// reads the first object the writer dequeues the next — now aged past the
// window — and escalates.
func TestFanout_LagWindowResetsSlowSubscriber(t *testing.T) {
	const lag = 100 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{MaxFanoutLag: lag})
	defer teardown()

	const alias = uint64(7)
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: ns, Name: name, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Publish several objects on one subgroup. The first blocks in the relay's
	// WriteObject (subscriber not reading yet); the rest queue and age.
	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
			GroupID:        0,
			SubgroupID:     0,
		})
		if err != nil {
			return
		}
		for range 6 {
			if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
				return
			}
		}
		_ = sg.Close()
	}()

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	// Stall well past the window so the backlog ages out, then read. The first
	// object (dequeued before the stall) still arrives; the next one the relay
	// tries to deliver has aged past MaxFanoutLag, so the stream is reset.
	time.Sleep(5 * lag)

	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := sg.ReadObject(); err != nil {
			return // stream reset / closed — the lag escalation fired
		}
		if time.Now().After(deadline) {
			t.Fatal("slow subscriber was not reset by the lag window")
		}
	}
}
