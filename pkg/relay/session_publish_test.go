package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Inbound PUBLISH, and the PUBLISH the relay forwards to SUBSCRIBE_TRACKS
// holders on a new bidi stream of their own (§6.1, §9.5, §10.20).

// TestPublish_AcceptedAndRegistered: a PUBLISH is accepted, its request stream
// stays open, and closing it lets the handler exit cleanly.
func TestPublish_AcceptedAndRegistered(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	stream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("stream.Close: %v", err)
	}
}

// TestPublish_DuplicateAliasRejected: reusing a Track Alias on the session for
// another track refuses the PUBLISH (§11.1).
func TestPublish_DuplicateAliasRejected(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	publishVideoTrack(t, clientSess, "cam1", 7)
	_, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam2"),
		TrackAlias: 7,
	})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)
}

// TestPublish_SavesLargestObjectFromPublish: LARGEST_OBJECT on an inbound
// PUBLISH feeds the relay's watermark before any Object arrives, and the next
// SUBSCRIBE_OK carries it (§10.2.17).
func TestPublish_SavesLargestObjectFromPublish(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	publishVideoTrack(t, pubSess, "cam1", 42, message.LargestObjectParam(5, 9))
	subReq := subscribeCam1(t, dialAnotherClient(t, pubSess))

	p, ok := subReq.OK.Parameters.Find(message.ParamLargestObject)
	if !ok {
		t.Fatalf("SUBSCRIBE_OK omitted LARGEST_OBJECT; the PUBLISH's value never "+
			"reached the entry (params=%v)", subReq.OK.Parameters)
	}
	if p.Group != 5 || p.Object != 9 {
		t.Errorf("SUBSCRIBE_OK LARGEST_OBJECT = {%d,%d}, want {5,9}", p.Group, p.Object)
	}
}

// nextForwardedPublish accepts the next request the relay opens to sess, within
// 2s, and requires it to be a PUBLISH.
func nextForwardedPublish(t *testing.T, sess *session.Session) *message.Publish {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	req, err := sess.AcceptRequest(ctx)
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	pub, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("got %T, want *message.Publish", req.First)
	}
	return pub
}

// forwardedCam7 has a SUBSCRIBE_TRACKS holder for video, sent with params,
// receive the PUBLISH the relay forwards when another client publishes
// video/cam7 rtp on alias 99.
func forwardedCam7(t *testing.T, params ...message.Parameter) *message.Publish {
	t.Helper()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	subscribeTracks(t, subSess, ns("video"), params...)

	publish(t, dialAnotherClient(t, subSess), &message.Publish{
		Namespace: ns("video", "cam7"), Name: []byte("rtp"), TrackAlias: 99,
	})
	return nextForwardedPublish(t, subSess)
}

// TestPublish_ForwardsToSubscribeTracks: a PUBLISH matching a SUBSCRIBE_TRACKS
// is forwarded on its own new bidi stream, not on the SUBSCRIBE_TRACKS stream
// (§6.1, §9.5).
func TestPublish_ForwardsToSubscribeTracks(t *testing.T) {
	t.Parallel()
	pub := forwardedCam7(t)
	if string(pub.Name) != "rtp" {
		t.Fatalf("forwarded Name = %q, want %q", pub.Name, "rtp")
	}
	// §11.1: the alias is per session, so the relay allocates its own on the
	// subscriber's session (never 0) rather than copying the publisher's 99;
	// see TestPublish_ForwardedAliasDoesNotCollide.
	if pub.TrackAlias == 0 {
		t.Fatal("forwarded TrackAlias is 0; want one allocated on the subscriber's session")
	}
	// §10.20.1: the SUBSCRIBE_TRACKS omitted FORWARD and GROUP_ORDER.
	// FORWARD defaults to 1 and is omitted; GROUP_ORDER is the publisher's
	// preference (§10.2.8), Ascending for this track (§12.5), and stated.
	if p, ok := pub.Parameters.Find(message.ParamForward); ok {
		t.Errorf("forwarded FORWARD present (=%d), want omitted", p.Byte)
	}
	if p, ok := pub.Parameters.Find(message.ParamGroupOrder); !ok ||
		message.GroupOrder(p.Byte) != message.GroupOrderAscending {
		t.Errorf("forwarded GROUP_ORDER = %d (present=%v), want Ascending (0x1)", p.Byte, ok)
	}
}

// TestPublish_ForwardsSubscribeTracksParams pins §10.20.1: the FORWARD
// (§10.2.18) and GROUP_ORDER (§10.2.8) parameters on a SUBSCRIBE_TRACKS are
// copied onto the PUBLISH the relay generates for that subscriber.
func TestPublish_ForwardsSubscribeTracksParams(t *testing.T) {
	t.Parallel()
	pub := forwardedCam7(t, message.ForwardParam(false), message.GroupOrderParam(message.GroupOrderDescending))
	if p, ok := pub.Parameters.Find(message.ParamForward); !ok || p.Byte != 0 {
		t.Errorf("forwarded FORWARD = %d (present=%v), want 0", p.Byte, ok)
	}
	if p, ok := pub.Parameters.Find(message.ParamGroupOrder); !ok ||
		message.GroupOrder(p.Byte) != message.GroupOrderDescending {
		t.Errorf("forwarded GROUP_ORDER = %d (present=%v), want Descending (0x2)", p.Byte, ok)
	}
}

// TestPublish_ForwardedAliasDoesNotCollide: a forwarded PUBLISH's Track Alias
// comes from the subscriber session's own alias space, so it cannot collide
// with one the relay handed out in SUBSCRIBE_OK (§11.1).
func TestPublish_ForwardedAliasDoesNotCollide(t *testing.T) {
	t.Parallel()
	pub1, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pub1, "cam1", 0)

	subSess := dialAnotherClient(t, pub1)
	subReq := subscribeCam1(t, subSess)
	subscribeTracks(t, subSess, ns("video"))

	publish(t, dialAnotherClient(t, pub1), &message.Publish{
		Namespace:  ns("video", "cam7"),
		Name:       []byte("rtp"),
		TrackAlias: subReq.OK.TrackAlias, // the alias the subscriber already holds for cam1
	})
	// cam1 is already published, so its PUBLISH is forwarded too (§10.20), in
	// either order: register each forwarded alias until the rtp one's. This is
	// the alias registration AcceptPublish performs, without its REQUEST_OK
	// write: the relay does not yet read its end of a forwarded PUBLISH stream,
	// so on the unbuffered in-process pipe that write would never complete.
	for range 2 {
		fwd := nextForwardedPublish(t, subSess)
		if err := subSess.RegisterInboundTrackAlias(fwd.TrackAlias, track.NewKey(fwd.Namespace, fwd.Name)); err != nil {
			t.Fatalf("registering the forwarded PUBLISH's alias for %s: %v", fwd.Name, err)
		}
		if string(fwd.Name) == "rtp" {
			return
		}
	}
	t.Fatal("the rtp PUBLISH was not forwarded")
}

// TestSubscribeTracks_InvalidGroupOrderClosesSession pins §10.2.8: a
// SUBSCRIBE_TRACKS carrying a GROUP_ORDER outside {Ascending, Descending} is a
// session-level PROTOCOL_VIOLATION, so the relay closes the whole session.
func TestSubscribeTracks_InvalidGroupOrderClosesSession(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, _ = subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
		Parameters: message.Parameters{
			message.GroupOrderParam(message.GroupOrder(0x07)), // out of range
		},
	})
	requireSessionClosed(t, subSess, "out-of-range GROUP_ORDER SUBSCRIBE_TRACKS (§10.2.8)")
}

// TestPublish_ForwardedPublishCarriesEntryLargestObject: a forwarded PUBLISH
// carries the largest LARGEST_OBJECT the relay observed, not the upstream
// PUBLISH's own (§10.2.17). Two publishers, the second with a lower value.
func TestPublish_ForwardedPublishCarriesEntryLargestObject(t *testing.T) {
	t.Parallel()
	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	tns := ns("video", "cam7")
	publish(t, pubA, &message.Publish{
		Namespace: tns, Name: []byte("rtp"), TrackAlias: 99,
		Parameters: message.Parameters{message.LargestObjectParam(9, 9)},
	})
	publish(t, dialAnotherClient(t, pubA), &message.Publish{
		Namespace: tns, Name: []byte("rtp"), TrackAlias: 100,
		Parameters: message.Parameters{message.LargestObjectParam(3, 4)},
	})

	// Existing tracks are forwarded when the SUBSCRIBE_TRACKS arrives (§10.20).
	subSess := dialAnotherClient(t, pubA)
	subscribeTracks(t, subSess, ns("video"))
	pub := nextForwardedPublish(t, subSess)

	// Exactly one LARGEST_OBJECT: the upstream's copy is stripped and the
	// entry's is appended, so a duplicate would mean the strip broke.
	var seen int
	for _, p := range pub.Parameters {
		if p.Type != message.ParamLargestObject {
			continue
		}
		seen++
		if p.Group != 9 || p.Object != 9 {
			t.Errorf("forwarded LARGEST_OBJECT = {%d,%d}, want {9,9} — the relay "+
				"advertised publisher B's lower value instead of its own maximum",
				p.Group, p.Object)
		}
	}
	if seen != 1 {
		t.Errorf("forwarded PUBLISH carried %d LARGEST_OBJECT parameters, want exactly 1", seen)
	}
}
