package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// The §8 delivery timeouts and the §8 lag window are both "this subscriber is
// too slow", and the relay must not confuse them. §3.3.4 draws the line by the
// reset code it attaches: TOO_FAR_BEHIND is defined as "the corresponding
// subscription has exceeded the publisher's resource limits and is being
// terminated", whereas DELIVERY_TIMEOUT says only "a delivery timeout was
// exceeded for this stream". The tests below pin the difference from the
// subscriber's side, which is the only side that can observe it: after a
// delivery timeout the track keeps flowing, after a lag breach it does not
// (see TestFanout_LagWindowResetsSlowSubscriber).
//
// MaxFanoutLag is deliberately left at its zero value throughout, so the only
// escalation that can fire is the one under test.

// publishOneSubgroup writes n one-byte objects on (group, subgroup) and closes
// the stream. Run in a goroutine: the in-process transport is synchronous, so
// the first WriteObject blocks until the subscriber reads.
func publishOneSubgroup(sess *session.Session, alias, group uint64, n int) {
	sg, err := sess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     alias,
		GroupID:        group,
		SubgroupID:     0,
	})
	if err != nil {
		return
	}
	for range n {
		if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			return
		}
	}
	_ = sg.Close()
}

// countUntilEnd reads objects until the stream ends and returns how many
// arrived. The in-process pipe transport does not carry §3.3.4 reset codes, so
// a reset and a clean FIN look alike to the reader — the count is what
// separates them: a stream cut short by a timeout delivers fewer objects than
// the publisher wrote, and one that ran to completion delivers all of them.
// That is also the difference a real subscriber cares about.
func countUntilEnd(sg *session.IncomingSubgroupStream, within time.Duration) int {
	deadline := time.Now().Add(within)
	got := 0
	for time.Now().Before(deadline) {
		if _, err := sg.ReadObject(); err != nil {
			return got
		}
		got++
	}
	return got
}

// TestFanout_DeliveryTimeoutKeepsSubscriptionAlive pins the §8 /
// §3.3.4 distinction that makes a per-subgroup timeout usable: a subscriber
// that stalls past OBJECT_DELIVERY_TIMEOUT loses the subgroup it stalled on,
// and nothing else. The relay resets that one stream and keeps forwarding, so
// the next group arrives without the subscriber having to re-SUBSCRIBE.
//
// Before delivery timeouts were sourced in the fanout, the only escalation the
// relay had was the lag window, which terminates the subscription outright —
// so a publisher had no way to mark one subgroup as sheddable without risking
// the whole track.
func TestFanout_DeliveryTimeoutKeepsSubscriptionAlive(t *testing.T) {
	const timeout = 100 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{})
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

	// The subscriber asks for the timeout (§10.2.4); the publisher declares
	// none, so §8's "smaller of the two non-zero values" resolves to this one.
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns, Name: name,
		Parameters: message.Parameters{message.ObjectDeliveryTimeoutParam(timeout)},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	const objects = 6
	go publishOneSubgroup(pubSess, alias, 0, objects)

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	// Stall past the timeout, then drain. The objects behind the first one sit
	// in the relay's queue for the whole stall, so by the time it tries to
	// write them they are older than the timeout and the stream is reset — the
	// subscriber sees the subgroup cut short. Their age is what fails them, not
	// the stream's: see TestObjectDeliveryTimeoutIsPerObjectNotPerStream.
	time.Sleep(3 * timeout)
	if got := countUntilEnd(sg, 2*time.Second); got >= objects {
		t.Fatalf("subscriber received all %d objects; the delivery timeout did "+
			"not cut the stalled subgroup short", got)
	}

	// The subscription must have survived: a fresh group still reaches us.
	// Read promptly this time so the timeout has no chance to fire again.
	go publishOneSubgroup(pubSess, alias, 1, 2)

	ds2, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("second group never arrived — the delivery timeout terminated "+
			"the subscription, which is TOO_FAR_BEHIND's behaviour, not "+
			"DELIVERY_TIMEOUT's: %v", err)
	}
	sg2, ok := ds2.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("second AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds2)
	}
	if _, err := sg2.ReadObject(); err != nil {
		t.Fatalf("second group's first object: %v", err)
	}
}

// TestFanout_PublisherTrackDeliveryTimeoutApplies pins the other half of the
// §8 resolution: the value can come from the publisher's Track Properties
// (§12.2) rather than the subscriber's parameters, and the relay must apply it
// to the streams it opens downstream. The subscriber here asks for nothing.
//
// This is the direction a publisher uses to mark its own data sheddable, so
// the relay sourcing it is what the whole mechanism rests on.
func TestFanout_PublisherTrackDeliveryTimeoutApplies(t *testing.T) {
	const timeout = 100 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const alias = uint64(7)
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")

	props := message.AppendTrackProperties([]wire.KVPair{{
		Type:   message.PropertyObjectDeliveryTimeout,
		IntVal: uint64(timeout / time.Millisecond),
	}})
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: ns, Name: name, TrackAlias: alias, TrackProperties: props,
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

	const objects = 6
	go publishOneSubgroup(pubSess, alias, 0, objects)

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	time.Sleep(3 * timeout)
	if got := countUntilEnd(sg, 2*time.Second); got >= objects {
		t.Fatalf("subscriber received all %d objects; the publisher's Track-level "+
			"OBJECT_DELIVERY_TIMEOUT was not applied to the downstream stream", got)
	}
}

// TestFanout_NoDeliveryTimeoutLeavesStalledSubscriberAlone is the control for
// both tests above: with neither side declaring a timeout and no MaxFanoutLag,
// the same stall must cost the subscriber nothing. Without this, a bug that
// reset every slow stream unconditionally would still pass the two tests that
// assert a reset happens.
func TestFanout_NoDeliveryTimeoutLeavesStalledSubscriberAlone(t *testing.T) {
	pubSess, teardown := connectRelay(t, relay.Config{})
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

	const objects = 4
	go publishOneSubgroup(pubSess, alias, 0, objects)

	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	time.Sleep(300 * time.Millisecond)
	if got := countUntilEnd(sg, 2*time.Second); got != objects {
		t.Fatalf("stalled subscriber got %d of %d objects with no timeout "+
			"configured; nothing should have cut the stream short", got, objects)
	}
}
