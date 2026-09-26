package relay_test

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestPublish_AcceptedAndRegistered drives a single PUBLISH through the relay
// and verifies REQUEST_OK comes back, the request stream stays open, and
// closing it lets the handler exit cleanly.
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

// TestSubscribe_RejectsWhenNoUpstream: with no publisher of the track and no
// matching namespace publisher, SUBSCRIBE is refused DOES_NOT_EXIST.
func TestSubscribe_RejectsWhenNoUpstream(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_ServedFromExistingUpstream: a SUBSCRIBE to an already
// published track is answered from the existing upstream (§9.4).
func TestSubscribe_ServedFromExistingUpstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
		Name:            []byte("cam1"),
		TrackAlias:      42,
		TrackProperties: opaqueProps("hello props"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)

	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	if subStream.OK == nil {
		t.Fatal("SubscribeOK is nil")
	}
	// §9.6 — properties must be echoed back. The relay treats them
	// opaquely so the bytes round-trip verbatim.
	if got, want := subStream.OK.TrackProperties, opaqueProps("hello props"); !bytes.Equal(got, want) {
		t.Fatalf("TrackProperties = %x, want %x", got, want)
	}
	// The relay's outbound alias for the subscriber's session is
	// independent of the publisher's alias (§11.1). We don't check
	// its value, only that it was allocated (i.e. monotonic — the
	// session starts at 0).
	if subStream.OK.TrackAlias == 42 {
		// Coincidence is allowed but extremely unlikely on a fresh
		// session whose AllocOutboundTrackAlias started at 0.
		t.Logf("note: subscriber alias happened to equal publisher alias (%d)", subStream.OK.TrackAlias)
	}
}

// TestPublish_ForwardsToSubscribeTracks: a PUBLISH matching a SUBSCRIBE_TRACKS
// is forwarded on its own new bidi stream, not on the SUBSCRIBE_TRACKS stream
// (§6.1, §9.5).
func TestPublish_ForwardsToSubscribeTracks(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, subSess)

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video", "cam7"),
		Name:       []byte("rtp"),
		TrackAlias: 99,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	// The forwarded PUBLISH arrives as a fresh inbound request on the
	// subscriber session.
	req, err := subSess.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	pub, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("got %T, want *message.Publish", req.First)
	}
	if string(pub.Name) != "rtp" {
		t.Fatalf("forwarded Name = %q, want %q", pub.Name, "rtp")
	}
	if pub.TrackAlias == 0 {
		// §11.1: the alias is per session, so the relay allocates its own on
		// the subscriber's session (never 0, see AllocOutboundTrackAlias)
		// rather than copying the publisher's 99 — see
		// TestPublish_ForwardedAliasDoesNotCollide.
		t.Fatal("forwarded TrackAlias is 0; want one allocated on the subscriber's session")
	}
	// §10.20.1: the SUBSCRIBE_TRACKS omitted FORWARD and GROUP_ORDER, so the
	// forwarded PUBLISH carries neither (FORWARD defaults to 1, GROUP_ORDER to
	// the publisher's preference).
	if p, ok := pub.Parameters.Find(message.ParamForward); ok {
		t.Errorf("forwarded FORWARD present (=%d), want omitted", p.Byte)
	}
	if p, ok := pub.Parameters.Find(message.ParamGroupOrder); ok {
		t.Errorf("forwarded GROUP_ORDER present (=%d), want omitted", p.Byte)
	}
}

// TestPublish_ForwardedAliasDoesNotCollide: a forwarded PUBLISH's Track Alias
// comes from the subscriber session's own alias space, so it cannot collide
// with one the relay handed out in SUBSCRIBE_OK (§11.1).
func TestPublish_ForwardedAliasDoesNotCollide(t *testing.T) {
	t.Parallel()
	pub1, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub1Req, err := pub1.Publish(t.Context(), &message.Publish{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Publish cam1: %v", err)
	}
	defer pub1Req.Close()

	subSess := dialAnotherClient(t, pub1)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe cam1: %v", err)
	}
	defer subReq.Close()

	tracksReq, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer tracksReq.Close()

	pub2 := dialAnotherClient(t, pub1)
	pub2Req, err := pub2.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video", "cam7"),
		Name:       []byte("rtp"),
		TrackAlias: subReq.OK.TrackAlias, // the alias the subscriber already holds for cam1
	})
	if err != nil {
		t.Fatalf("Publish rtp: %v", err)
	}
	defer pub2Req.Close()

	req, err := subSess.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	fwd, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("got %T, want *message.Publish", req.First)
	}
	// The alias registration AcceptPublish performs, without its REQUEST_OK
	// write: the relay does not yet read its end of a forwarded PUBLISH stream,
	// so on the unbuffered in-process pipe that write would never complete.
	if err := subSess.RegisterInboundTrackAlias(fwd.TrackAlias, track.NewKey(fwd.Namespace, fwd.Name)); err != nil {
		t.Fatalf("registering the forwarded PUBLISH's alias: %v", err)
	}
}

// TestPublish_ForwardsSubscribeTracksParams pins §10.20.1: the FORWARD
// (§10.2.18) and GROUP_ORDER (§10.2.8) parameters on a SUBSCRIBE_TRACKS are
// copied onto the PUBLISH the relay generates for that subscriber.
func TestPublish_ForwardsSubscribeTracksParams(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
		Parameters: message.Parameters{
			message.ForwardParam(false),
			message.GroupOrderParam(message.GroupOrderDescending),
		},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, subSess)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video", "cam7"),
		Name:       []byte("rtp"),
		TrackAlias: 99,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	req, err := subSess.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	pub, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("got %T, want *message.Publish", req.First)
	}
	if p, ok := pub.Parameters.Find(message.ParamForward); !ok || p.Byte != 0 {
		t.Errorf("forwarded FORWARD = %d (present=%v), want 0", p.Byte, ok)
	}
	if p, ok := pub.Parameters.Find(message.ParamGroupOrder); !ok ||
		message.GroupOrder(p.Byte) != message.GroupOrderDescending {
		t.Errorf("forwarded GROUP_ORDER = %d (present=%v), want Descending (0x2)", p.Byte, ok)
	}
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

// TestPublish_DuplicateAliasRejected: reusing a Track Alias on the session for
// another track refuses the PUBLISH (§11.1).
func TestPublish_DuplicateAliasRejected(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	stream1, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	defer stream1.Close()

	_, err = clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam2"),
		TrackAlias: 7,
	})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)
}

// TestSubscribe_OnDemandUpstreamSubscribe: a SUBSCRIBE under a published
// namespace makes the relay SUBSCRIBE upstream and answer downstream only after
// the upstream SUBSCRIBE_OK.
func TestSubscribe_OnDemandUpstreamSubscribe(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// Publisher advertises ("video",) via PUBLISH_NAMESPACE, then runs a
	// goroutine that accepts the upstream SUBSCRIBE the relay will issue
	// and replies SUBSCRIBE_OK.
	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	pubResponded := make(chan struct{})
	go func() {
		defer close(pubResponded)
		req, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			t.Errorf("publisher AcceptRequest: %v", err)
			return
		}
		sub, ok := req.First.(*message.Subscribe)
		if !ok {
			t.Errorf("publisher received %T, want *message.Subscribe", req.First)
			return
		}
		if string(sub.Name) != "cam1" {
			t.Errorf("publisher upstream SUBSCRIBE name = %q", sub.Name)
		}
		if err := req.Reply(&message.SubscribeOK{
			TrackAlias:      77,
			TrackProperties: opaqueProps("upstream props"),
		}); err != nil {
			t.Errorf("publisher SubscribeOK reply: %v", err)
			return
		}
	}()

	subSess := dialAnotherClient(t, pubSess)

	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	<-pubResponded

	// §9.6: Track Properties must be echoed back. The relay captured them
	// from the upstream SUBSCRIBE_OK and replays them on the downstream
	// reply.
	if got, want := subStream.OK.TrackProperties, opaqueProps("upstream props"); !bytes.Equal(got, want) {
		t.Fatalf("downstream TrackProperties = %x, want %x", got, want)
	}
}

// upstreamForwardValue answers one upstream SUBSCRIBE on pubSess and delivers
// the FORWARD it carried as (value, present); absent means 1 (§10.2.18).
func upstreamForwardValue(t *testing.T, pubSess *session.Session) <-chan [2]int {
	t.Helper()
	out := make(chan [2]int, 1)
	go func() {
		req, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			t.Errorf("publisher AcceptRequest: %v", err)
			return
		}
		sub, ok := req.First.(*message.Subscribe)
		if !ok {
			t.Errorf("publisher received %T, want *message.Subscribe", req.First)
			return
		}
		present := 0
		val := 0
		if p, ok := sub.Parameters.Find(message.ParamForward); ok {
			present = 1
			val = int(p.Byte)
		}
		out <- [2]int{val, present}
		if err := req.Reply(&message.SubscribeOK{TrackAlias: 77}); err != nil {
			t.Errorf("publisher SubscribeOK reply: %v", err)
			return
		}
		for {
			if _, err := message.Parse(req.Stream); err != nil {
				return
			}
		}
	}()
	return out
}

// TestSubscribe_UpstreamForwardPausedWhenDownstreamForwardZero pins §9.2: when
// the only downstream subscriber sets Forward=0, the relay exercises its
// discretion and pauses the upstream with an explicit FORWARD=0.
func TestSubscribe_UpstreamForwardPausedWhenDownstreamForwardZero(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	fwd := upstreamForwardValue(t, pubSess)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.ForwardParam(false)},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	got := <-fwd
	if got != [2]int{0, 1} {
		t.Fatalf("upstream FORWARD = {val:%d present:%d}, want {0, 1} (explicit Forward=0)", got[0], got[1])
	}
}

// TestSubscribe_UpstreamForwardOmittedWhenDownstreamForwards pins §9.2: when a
// downstream subscriber wants forwarding (FORWARD omitted → default 1), the
// relay's upstream SUBSCRIBE omits FORWARD too (implicit 1).
func TestSubscribe_UpstreamForwardOmittedWhenDownstreamForwards(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	fwd := upstreamForwardValue(t, pubSess)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subStream.Close()

	got := <-fwd
	if got[1] != 0 {
		t.Fatalf("upstream FORWARD present (=%d), want omitted (implicit 1)", got[0])
	}
}

// TestSubscribe_UpstreamResumedWhenForwardingSubscriberJoins: a paused upstream
// is resumed with REQUEST_UPDATE FORWARD=1 when a Forward=1 subscriber joins
// (§9.2).
func TestSubscribe_UpstreamResumedWhenForwardingSubscriberJoins(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	// Publisher: accept the upstream SUBSCRIBE (expect explicit Forward=0),
	// reply SUBSCRIBE_OK, then read the resume REQUEST_UPDATE (expect Forward=1)
	// and answer it with REQUEST_OK.
	type result struct {
		initialForward  int
		resumeForward   int
		initialHasParam bool
	}
	res := make(chan result, 1)
	go func() {
		req, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			t.Errorf("publisher AcceptRequest: %v", err)
			return
		}
		sub, ok := req.First.(*message.Subscribe)
		if !ok {
			t.Errorf("publisher received %T, want *message.Subscribe", req.First)
			return
		}
		var r result
		if p, ok := sub.Parameters.Find(message.ParamForward); ok {
			r.initialHasParam = true
			r.initialForward = int(p.Byte)
		}
		if err := req.Reply(&message.SubscribeOK{TrackAlias: 77}); err != nil {
			t.Errorf("publisher SubscribeOK: %v", err)
			return
		}
		m, err := message.Parse(req.Stream)
		if err != nil {
			t.Errorf("publisher read follow-up: %v", err)
			return
		}
		upd, ok := m.(*message.RequestUpdate)
		if !ok {
			t.Errorf("publisher follow-up = %T, want *message.RequestUpdate", m)
			return
		}
		if p, ok := upd.Parameters.Find(message.ParamForward); ok {
			r.resumeForward = int(p.Byte)
		}
		// Answer the §10.9 REQUEST_UPDATE so the relay's resume Update() call
		// completes rather than timing out.
		if err := message.Marshal(req.Stream, &message.RequestOK{}); err != nil {
			t.Errorf("publisher REQUEST_OK: %v", err)
		}
		res <- r
	}()

	// Subscriber A (Forward=0) establishes the paused upstream.
	subA := dialAnotherClient(t, pubSess)
	subAStream, err := subA.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.ForwardParam(false)},
	})
	if err != nil {
		t.Fatalf("subscriber A Subscribe: %v", err)
	}
	defer subAStream.Close()

	// Subscriber B (Forward omitted → 1) reuses the upstream and must resume it.
	subB := dialAnotherClient(t, pubSess)
	subBStream, err := subB.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("subscriber B Subscribe: %v", err)
	}
	defer subBStream.Close()

	select {
	case got := <-res:
		if !got.initialHasParam || got.initialForward != 0 {
			t.Errorf("upstream initial FORWARD = {val:%d present:%v}, want explicit 0",
				got.initialForward, got.initialHasParam)
		}
		if got.resumeForward != 1 {
			t.Errorf("upstream resume REQUEST_UPDATE FORWARD = %d, want 1", got.resumeForward)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("publisher did not observe the §9.2 upstream resume REQUEST_UPDATE")
	}
}

// acceptUpstreamSubscribe answers one upstream SUBSCRIBE on pubSess with alias
// and drains its follow-ups; the channel closes when the relay ends it.
func acceptUpstreamSubscribe(t *testing.T, pubSess *session.Session, alias uint64) <-chan struct{} {
	t.Helper()
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		req, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if err := req.Reply(&message.SubscribeOK{TrackAlias: alias}); err != nil {
			t.Errorf("publisher SubscribeOK reply: %v", err)
			return
		}
		for {
			if _, err := message.Parse(req.Stream); err != nil {
				return
			}
		}
	}()
	return ended
}

// TestSubscribe_UpstreamSurvivesInitiatingSubscriber: an on-demand upstream
// outlives the subscriber that triggered it while another still uses it (§9.4).
func TestSubscribe_UpstreamSurvivesInitiatingSubscriber(t *testing.T) {
	t.Parallel()
	closed := &recordingMetrics{}
	pubSess, teardown := connectRelay(t, relay.Config{Metrics: closed})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	const upstreamAlias = uint64(77)
	acceptUpstreamSubscribe(t, pubSess, upstreamAlias)

	subA := dialAnotherClient(t, pubSess)
	subAStream, err := subA.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("subscriber A Subscribe: %v", err)
	}
	defer subAStream.Close()

	subB := dialAnotherClient(t, pubSess)
	subBStream, err := subB.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("subscriber B Subscribe: %v", err)
	}
	defer subBStream.Close()

	// A leaves entirely. The relay must NOT tear the upstream down — B
	// still depends on it. Wait until the relay has actually evicted A's
	// subscription (SubscriptionClosed fires in handleSubscribe's defer)
	// so the publish below exercises the post-removal state.
	_ = subA.Close(0, "subscriber A leaving")
	waitFor(t, 2*time.Second, func() bool { return closed.subsClosed.Load() >= 1 },
		"relay never evicted subscriber A's subscription")

	// B must still be able to receive: publish one object upstream and
	// expect it on B's data path. B's acceptor starts FIRST — the in-process
	// pipes are unbuffered, so the relay's fanout write to B completes only
	// once B reads.
	got := make(chan string, 1)
	go func() {
		ds, err := subB.AcceptDataStream(t.Context())
		if err != nil {
			return
		}
		sgIn, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		obj, err := sgIn.ReadObject()
		if err != nil {
			return
		}
		got <- string(obj.Payload)
	}()

	sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     upstreamAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("alive")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}

	select {
	case payload := <-got:
		if payload != "alive" {
			t.Fatalf("subscriber B got %q, want %q", payload, "alive")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber B received nothing after A left — upstream was torn down with A")
	}
}

// TestSubscribe_LastDownstreamTearsDownUpstream: when the last downstream
// subscriber leaves, the relay ends its upstream subscription.
func TestSubscribe_LastDownstreamTearsDownUpstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	subscriptionEnded := acceptUpstreamSubscribe(t, pubSess, 77)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	// The only downstream unsubscribes (FINs its request stream). The relay
	// must propagate the teardown upstream.
	_ = subStream.Close()

	select {
	case <-subscriptionEnded:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher's subscription still open 2s after the last downstream left")
	}
}

// TestSubscribe_NoMatchingPublisher_RejectsDoesNotExist: with no matching
// PUBLISH_NAMESPACE, SUBSCRIBE is refused DOES_NOT_EXIST.
func TestSubscribe_NoMatchingPublisher_RejectsDoesNotExist(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	_, err = subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("audio"), // no publisher for this namespace
		Name:      []byte("mic"),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_UpstreamRejects_PropagatesRejection: an upstream REQUEST_ERROR
// code about the track passes downstream; one about the relay's own hop becomes
// INTERNAL_ERROR (§10.6.2). The Retry Interval is kept either way.
func TestSubscribe_UpstreamRejects_PropagatesRejection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		upstream, want moqt.RequestErrorCode
		retry          uint64
	}{
		{moqt.RequestDoesNotExist, moqt.RequestDoesNotExist, 0},
		{moqt.RequestExcessiveLoad, moqt.RequestExcessiveLoad, 501},
		{moqt.RequestTimeout, moqt.RequestTimeout, 1},
		{moqt.RequestMalformedTrack, moqt.RequestMalformedTrack, 0},
		{moqt.RequestUnauthorized, moqt.RequestInternalError, 0},
		{moqt.RequestExpiredAuthToken, moqt.RequestInternalError, 2001},
		{moqt.RequestGoingAway, moqt.RequestInternalError, 0},
		{moqt.RequestInvalidRange, moqt.RequestInternalError, 0},
		{moqt.RequestErrorCode(0x7777), moqt.RequestInternalError, 31},
	} {
		t.Run(fmt.Sprintf("%#x", uint64(tc.upstream)), func(t *testing.T) {
			t.Parallel()
			pubSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()

			pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
				Namespace: ns("video"),
			})
			if err != nil {
				t.Fatalf("PublishNamespace: %v", err)
			}
			defer pubNSStream.Close()

			go func() {
				req, err := pubSess.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				_ = req.Reject(&session.RequestRejectedError{
					Code: tc.upstream, Reason: "upstream says no", RetryInterval: tc.retry,
				})
			}()

			subSess := dialAnotherClient(t, pubSess)
			_, err = subSess.Subscribe(t.Context(), &message.Subscribe{
				Namespace: ns("video"),
				Name:      []byte("cam1"),
			})
			requireRejectedWithCode(t, err, tc.want)
			if rej, _ := errors.AsType[*session.RequestRejectedError](err); rej.RetryInterval != tc.retry {
				t.Fatalf("downstream Retry Interval %d, want the upstream's %d", rej.RetryInterval, tc.retry)
			}
		})
	}
}

// TestSubscribe_PublisherDisappears_EmitsPublishDone: when the publisher's
// session ends, each downstream subscriber gets PUBLISH_DONE TRACK_ENDED
// (§10.12).
func TestSubscribe_PublisherDisappears_EmitsPublishDone(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Tear down the publisher session. The relay's per-session
	// cleanup runs TrackRegistry.RemoveSession, which detects that
	// the track has no remaining upstream publisher and writes
	// PUBLISH_DONE on every dependent downstream's request stream.
	_ = pubSess.Close(0, "publisher leaving")

	done := make(chan message.Message, 1)
	go func() {
		msg, _ := message.Parse(subReq)
		done <- msg
	}()
	select {
	case msg := <-done:
		pd, ok := msg.(*message.PublishDone)
		if !ok {
			t.Fatalf("got %T, want *message.PublishDone", msg)
		}
		if pd.StatusCode != moqt.PublishDoneTrackEnded {
			t.Errorf("PublishDone.StatusCode = %v, want PublishDoneTrackEnded", pd.StatusCode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not see PUBLISH_DONE within 2s of publisher leaving")
	}
}

// TestSubscribe_PublisherDisappears_StreamClosesAfterPublishDone: the relay
// FINs the request stream after PUBLISH_DONE.
func TestSubscribe_PublisherDisappears_StreamClosesAfterPublishDone(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	_ = pubSess.Close(0, "publisher leaving")

	// First message: PUBLISH_DONE.
	first, err := message.Parse(subReq)
	if err != nil {
		t.Fatalf("Parse #1: %v", err)
	}
	if _, ok := first.(*message.PublishDone); !ok {
		t.Fatalf("first message = %T, want *message.PublishDone", first)
	}

	// Subsequent Parse should hit EOF — the relay FIN'd the stream
	// right after PUBLISH_DONE.
	if _, err := message.Parse(subReq); err == nil {
		t.Fatal("second Parse returned nil error; expected EOF after FIN")
	} else if !errors.Is(err, io.EOF) {
		// Some transports surface FIN as a different sentinel
		// (pipe-closed, etc.). Accept anything non-nil as long as
		// the parse path didn't succeed.
		t.Logf("second Parse returned %v (acceptable non-nil error after FIN)", err)
	}
}

// TestSubscribe_NoAliasCollisionWhenAlsoPublishing: the relay's outbound alias
// space is independent of the aliases a session PUBLISHes with (§11.1), so
// both starting at 0 is no collision.
func TestSubscribe_NoAliasCollisionWhenAlsoPublishing(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// The client publishes its own track, taking inbound alias 0.
	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("room", "self"),
		Name:       []byte("video"),
		TrackAlias: 0,
	})
	if err != nil {
		t.Fatalf("Publish own track: %v", err)
	}
	defer pubStream.Close()

	// A peer publishes a track the client will subscribe to.
	peerSess := dialAnotherClient(t, clientSess)
	peerStream, err := peerSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("room", "peer"),
		Name:       []byte("video"),
		TrackAlias: 0,
	})
	if err != nil {
		t.Fatalf("peer Publish: %v", err)
	}
	defer peerStream.Close()

	// Subscribing to the peer's track on the same session that already
	// published alias 0 must succeed — the relay's outbound alias (also
	// starting at 0) must not collide with the inbound alias 0.
	subStream, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("room", "peer"),
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Subscribe to peer track (alias collision regression): %v", err)
	}
	defer subStream.Close()
}

// TestPublish_SavesLargestObjectFromPublish: LARGEST_OBJECT on an inbound
// PUBLISH feeds the relay's watermark before any Object arrives, and the next
// SUBSCRIBE_OK carries it (§10.2.17).
func TestPublish_SavesLargestObjectFromPublish(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	video := ns("video")
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  video,
		Name:       []byte("cam1"),
		TrackAlias: 42,
		Parameters: message.Parameters{message.LargestObjectParam(5, 9)},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	p, ok := subReq.OK.Parameters.Find(message.ParamLargestObject)
	if !ok {
		t.Fatalf("SUBSCRIBE_OK omitted LARGEST_OBJECT; the PUBLISH's value never "+
			"reached the entry (params=%v)", subReq.OK.Parameters)
	}
	if p.Group != 5 || p.Object != 9 {
		t.Errorf("SUBSCRIBE_OK LARGEST_OBJECT = {%d,%d}, want {5,9}", p.Group, p.Object)
	}
}

// TestPublish_ForwardedPublishCarriesEntryLargestObject: a forwarded PUBLISH
// carries the largest LARGEST_OBJECT the relay observed, not the upstream
// PUBLISH's own (§10.2.17). Two publishers, the second with a lower value.
func TestPublish_ForwardedPublishCarriesEntryLargestObject(t *testing.T) {
	t.Parallel()
	pubA, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	tns := ns("video", "cam7")

	// First publisher sets the entry's watermark to {9,9}.
	pubStreamA, err := pubA.Publish(t.Context(), &message.Publish{
		Namespace:  tns,
		Name:       []byte("rtp"),
		TrackAlias: 99,
		Parameters: message.Parameters{message.LargestObjectParam(9, 9)},
	})
	if err != nil {
		t.Fatalf("Publish A: %v", err)
	}
	defer pubStreamA.Close()

	// Second publisher on the SAME track announces a lower one.
	pubB := dialAnotherClient(t, pubA)
	pubStreamB, err := pubB.Publish(t.Context(), &message.Publish{
		Namespace:  tns,
		Name:       []byte("rtp"),
		TrackAlias: 100,
		Parameters: message.Parameters{message.LargestObjectParam(3, 4)},
	})
	if err != nil {
		t.Fatalf("Publish B: %v", err)
	}
	defer pubStreamB.Close()

	// The subscriber gets one PUBLISH for the track (§10.20: existing tracks
	// are forwarded when the SUBSCRIBE_TRACKS arrives).
	subSess := dialAnotherClient(t, pubA)
	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()
	req, err := subSess.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	pub, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("got %T, want *message.Publish", req.First)
	}
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

// opaqueProps is well-formed Track Properties holding one property of a type
// the relay does not interpret, so it must pass through byte for byte.
func opaqueProps(v string) []byte {
	return message.AppendTrackProperties([]wire.KVPair{{Type: 0x101, ByteVal: []byte(v)}})
}
