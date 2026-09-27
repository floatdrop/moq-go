package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A §8 delivery timeout resets one stream and the track keeps flowing, unlike a
// lag-window breach, which ends the subscription (§3.3.4; see
// TestFanout_LagWindowResetsSlowSubscriber). MaxFanoutLag stays zero here, so
// only the escalation under test can fire.

// countUntilEnd reads Objects until the stream ends or within elapses and
// returns how many arrived. The pipe transport carries no reset codes, so a
// short count is what shows a reset.
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

// TestFanout_DeliveryTimeoutKeepsSubscriptionAlive: a subscriber stalled past
// OBJECT_DELIVERY_TIMEOUT loses only that subgroup; the next group still
// arrives (§8, §3.3.4).
func TestFanout_DeliveryTimeoutKeepsSubscriptionAlive(t *testing.T) {
	const timeout = 100 * time.Millisecond
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const alias = uint64(7)
	video := ns("video")
	name := []byte("cam1")

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: name, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	// The subscriber asks for the timeout (§10.2.4); the publisher declares
	// none, so §8's "smaller of the two non-zero values" resolves to this one.
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: video, Name: name,
		Parameters: message.Parameters{message.ObjectDeliveryTimeoutParam(timeout)},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	const objects = 6
	go sendObjects(pubSess, alias, 0, objects)

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
	go sendObjects(pubSess, alias, 1, 2)

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

// TestFanout_PublisherTrackDeliveryTimeoutApplies: a delivery timeout from the
// publisher's Track Properties (§12.2) applies downstream too.
func TestFanout_PublisherTrackDeliveryTimeoutApplies(t *testing.T) {
	const timeout = 100 * time.Millisecond
	prop := wire.KVPair{Type: message.PropertyObjectDeliveryTimeout, IntVal: uint64(timeout / time.Millisecond)}
	for name, props := range map[string][]wire.KVPair{
		"mutable": {prop},
		// §12.7: "processors MUST search both the mutable properties and the
		// contents of Immutable Properties".
		"inside Immutable Properties": {{
			Type:    message.PropertyImmutableProperties,
			ByteVal: message.AppendTrackProperties([]wire.KVPair{prop}),
		}},
	} {
		t.Run(name, func(t *testing.T) { testPublisherTrackDeliveryTimeout(t, timeout, props) })
	}
}

// testPublisherTrackDeliveryTimeout requires a stalled subscriber's stream to
// be cut short by the delivery timeout the publisher's trackProps set.
func testPublisherTrackDeliveryTimeout(t *testing.T, timeout time.Duration, trackProps []wire.KVPair) {
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const alias = uint64(7)
	video := ns("video")
	name := []byte("cam1")

	props := message.AppendTrackProperties(trackProps)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: name, TrackAlias: alias, TrackProperties: props,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	const objects = 6
	go sendObjects(pubSess, alias, 0, objects)

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

// TestFanout_NoDeliveryTimeoutLeavesStalledSubscriberAlone: the control — with
// no timeout and no MaxFanoutLag, the same stall resets nothing.
func TestFanout_NoDeliveryTimeoutLeavesStalledSubscriberAlone(t *testing.T) {
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const alias = uint64(7)
	video := ns("video")
	name := []byte("cam1")

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: video, Name: name, TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	const objects = 4
	go sendObjects(pubSess, alias, 0, objects)

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

// TestFanout_ReplayStreamKeepsFirstObjectDeliveryTimeout: an
// OBJECT_DELIVERY_TIMEOUT on a Subgroup's first Object overrides the Track
// for the whole Subgroup (§8: "the publisher's value is the Object Property
// when present on the first object of the subgroup"; §12.2), including on a
// replay stream that starts past that Object: a subscriber that joined after
// it, or a stream reopened after a gap (§11.4.3).
func TestFanout_ReplayStreamKeepsFirstObjectDeliveryTimeout(t *testing.T) {
	const timeout = 100 * time.Millisecond
	props := message.AppendTrackProperties([]wire.KVPair{{
		Type: message.PropertyObjectDeliveryTimeout, IntVal: uint64(timeout / time.Millisecond),
	}})
	// Enough Objects behind the stall that the timeout, if applied, cuts the
	// stream short.
	const objects = 6

	t.Run("joiner", func(t *testing.T) {
		pubSess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		pub := publishVideoTrack(t, pubSess, "cam1", 1) // no Track-level timeout

		// early proves the relay forwarded Object 0 before late subscribes.
		early := dialAnotherClient(t, pubSess)
		subscribeCam1(t, early)
		events := make(chan objEvent, 2*objects)
		go readSubgroups(t.Context(), early, events)

		sg, err := pub.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, Properties: true,
		})
		if err != nil {
			t.Fatalf("OpenSubgroup: %v", err)
		}
		if err := sg.WriteObjectAt(0, &message.SubgroupObject{Properties: props, Payload: []byte("0")}); err != nil {
			t.Fatalf("WriteObjectAt 0: %v", err)
		}
		if id := awaitObject(t, events); id != 0 {
			t.Fatalf("early subscriber got Object %d, want 0", id)
		}

		late := dialAnotherClient(t, pubSess)
		subscribeCam1(t, late)
		go func() {
			for id := uint64(1); id <= objects; id++ {
				if sg.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte("x")}) != nil {
					return
				}
			}
			_ = sg.Close()
		}()

		in := acceptSubgroup(t, late)
		if !in.Header.ReplayingSubgroup {
			t.Fatal("late subscriber's stream claims FIRST_OBJECT; want a replay stream")
		}
		time.Sleep(3 * timeout)
		if got := countUntilEnd(in, 2*time.Second); got >= objects {
			t.Fatalf("stalled joiner received all %d objects; its replay stream "+
				"lost the first Object's OBJECT_DELIVERY_TIMEOUT", got)
		}
	})

	t.Run("gap reopen", func(t *testing.T) {
		pubSess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		pub := publishVideoTrack(t, pubSess, "cam1", 1) // no Track-level timeout
		subSess := dialAnotherClient(t, pubSess)
		subscribeCam1(t, subSess)

		hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, Properties: true}
		first, err := pub.OpenSubgroup(hdr)
		if err != nil {
			t.Fatalf("OpenSubgroup: %v", err)
		}
		if err := first.WriteObjectAt(0, &message.SubgroupObject{Properties: props, Payload: []byte("0")}); err != nil {
			t.Fatalf("WriteObjectAt 0: %v", err)
		}
		in := acceptSubgroup(t, subSess)
		if _, err := in.ReadObject(); err != nil {
			t.Fatalf("Object 0: %v", err)
		}

		// A second stream of the same Subgroup resumes at Object 2: Object 1
		// is not known to be skipped, so the relay resets the subscriber's
		// stream and reopens a replay one (§11.4.3).
		hdr.ReplayingSubgroup = true
		replay, err := pub.OpenSubgroup(hdr)
		if err != nil {
			t.Fatalf("OpenSubgroup (replay): %v", err)
		}
		go func() {
			// A FIN would end the Subgroup at Object 0 (§2.4.2), so the first
			// stream resets, leaving the replay stream's FIN to end it.
			defer first.Cancel(moqt.StreamResetCancelled)
			for id := uint64(2); id < 2+objects; id++ {
				if replay.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte("x")}) != nil {
					return
				}
			}
			_ = replay.Close()
		}()

		if got := countUntilEnd(in, 2*time.Second); got != 0 {
			t.Fatalf("first stream carried %d more Objects, want it reset at the gap", got)
		}
		reopened := acceptSubgroup(t, subSess)
		time.Sleep(3 * timeout)
		if got := countUntilEnd(reopened, 2*time.Second); got >= objects {
			t.Fatalf("stalled subscriber received all %d objects after the gap; "+
				"the reopened stream lost the first Object's OBJECT_DELIVERY_TIMEOUT", got)
		}
	})
}

// acceptSubgroup returns sess's next data stream, which must be a subgroup.
func acceptSubgroup(t *testing.T, sess *session.Session) *session.IncomingSubgroupStream {
	t.Helper()
	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}
	return sg
}
