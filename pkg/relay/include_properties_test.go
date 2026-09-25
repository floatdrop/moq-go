package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §10.2.21: INCLUDE_PROPERTIES "specifies whether the OK message sent in
// response includes Track Properties or whether the resulting PUBLISH
// messages include Track Properties in the case of SUBSCRIBE_TRACKS. If
// INCLUDE_PROPERTIES is 0, the Track Properties are still present in the
// message, but they SHOULD be empty."
//
// Without Track Properties the subscriber reads DEFAULT_PUBLISHER_PRIORITY as
// 128 (§12.4), so for such a subscription the relay writes the priority
// inline on the subgroups and datagrams it forwards instead of inheriting it.

const trackDefaultPriority = 7

func priorityTrackProps() []wire.KVPair {
	return []wire.KVPair{{Type: message.PropertyDefaultPublisherPriority, IntVal: trackDefaultPriority}}
}

func noProps() message.Parameter { return message.IncludePropertiesParam(false) }

func TestIncludeProperties_SubscribeOK(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, priorityTrackProps())
	subSess := dialAnotherClient(t, pubSess)
	sub := subscribeCam1Req(t, subSess, noProps())
	if len(sub.OK.TrackProperties) != 0 {
		t.Fatalf("SUBSCRIBE_OK Track Properties %x with INCLUDE_PROPERTIES=0, want empty", sub.OK.TrackProperties)
	}
	with := subscribeCam1Req(t, dialAnotherClient(t, pubSess))
	if len(with.OK.TrackProperties) == 0 {
		t.Fatal("SUBSCRIBE_OK lost its Track Properties without INCLUDE_PROPERTIES")
	}

	// The subgroup the publisher sends with the default priority reaches
	// the subscriber with the priority spelled out.
	go sendObject(pubSess, alias, 1)
	ds, ok := tryAcceptDataStream(t, subSess, 2*time.Second)
	if !ok {
		t.Fatal("no subgroup forwarded")
	}
	sg := ds.(*session.IncomingSubgroupStream)
	if !sg.Header.InlinePriority || sg.Header.PublisherPriority != trackDefaultPriority {
		t.Fatalf("forwarded header inline=%v priority=%d, want inline priority %d",
			sg.Header.InlinePriority, sg.Header.PublisherPriority, trackDefaultPriority)
	}
}

func TestIncludeProperties_Datagram(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, priorityTrackProps())
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1Req(t, subSess, noProps())
	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type: message.DatagramDefaultPriorityBit, TrackAlias: alias, GroupID: 1, ObjectPayload: []byte("x"),
	}); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	d, err := subSess.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceiveDatagram: %v", err)
	}
	if d.HasDefaultPriority() || d.PublisherPriority != trackDefaultPriority {
		t.Fatalf("forwarded datagram default=%v priority=%d, want explicit priority %d",
			d.HasDefaultPriority(), d.PublisherPriority, trackDefaultPriority)
	}
}

func TestIncludeProperties_FetchAndTrackStatus(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, priorityTrackProps())
	subscribeCam1(t, pubSess) // keeps the track's upstream alive
	publishSubgroupObject(t, pubSess, alias, 1, -1)
	time.Sleep(50 * time.Millisecond)
	c := dialAnotherClient(t, pubSess)

	ts, err := c.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
		Parameters: message.Parameters{noProps()},
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	if len(ts.OK.TrackProperties) != 0 {
		t.Fatalf("TRACK_STATUS_OK Track Properties %x with INCLUDE_PROPERTIES=0, want empty", ts.OK.TrackProperties)
	}

	fr, err := c.Fetch(t.Context(), &message.Fetch{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
		Parameters: message.Parameters{noProps(),
			fetchRangeFilter(message.Location{Group: 1}, message.Location{Group: 1})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	if len(fr.OK.TrackProperties) != 0 {
		t.Fatalf("FETCH_OK Track Properties %x with INCLUDE_PROPERTIES=0, want empty", fr.OK.TrackProperties)
	}
	go drainAll(t.Context(), c)
}

func TestIncludeProperties_SubscribeTracks(t *testing.T) {
	t.Parallel()
	holder, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, holder)
	openSubscribeTracks(t, holder, ns("video"), noProps())

	pubSess := dialAnotherClient(t, holder)
	p, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: ns("video"), Name: []byte("cam"), TrackAlias: 7,
		TrackProperties: message.AppendTrackProperties(priorityTrackProps()),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer p.Close()
	fwd := awaitForwarded(t, reqs)
	if tp := fwd.First.(*message.Publish).TrackProperties; len(tp) != 0 {
		t.Fatalf("forwarded PUBLISH Track Properties %x with INCLUDE_PROPERTIES=0, want empty", tp)
	}
	acceptForwarded(t, fwd)

	go sendObject(pubSess, 7, 1)
	ds, ok := tryAcceptDataStream(t, holder, 2*time.Second)
	if !ok {
		t.Fatal("no subgroup forwarded")
	}
	if h := ds.(*session.IncomingSubgroupStream).Header; !h.InlinePriority ||
		h.PublisherPriority != trackDefaultPriority {
		t.Fatalf("forwarded header inline=%v priority=%d, want inline priority %d",
			h.InlinePriority, h.PublisherPriority, trackDefaultPriority)
	}
}

// TestIncludeProperties_TrackStatusStillAnswers: INCLUDE_PROPERTIES only
// empties the Track Properties; it does not change whether the track is known.
// A PUBLISHed track with properties and no Objects yet is answered
// TRACK_STATUS_OK either way.
func TestIncludeProperties_TrackStatusStillAnswers(t *testing.T) {
	t.Parallel()
	pubSess, _ := publishWithTrackProps(t, priorityTrackProps())
	c := dialAnotherClient(t, pubSess)
	ts, err := c.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")}, Name: []byte("cam1"),
		Parameters: message.Parameters{noProps()},
	})
	if err != nil {
		t.Fatalf("TRACK_STATUS with INCLUDE_PROPERTIES=0 on a known track: %v", err)
	}
	if len(ts.OK.TrackProperties) != 0 {
		t.Fatalf("TRACK_STATUS_OK Track Properties %x, want empty", ts.OK.TrackProperties)
	}
}
