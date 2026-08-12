package relay_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
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
		Namespace:  wire.TrackNamespace{[]byte("video")},
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

// TestSubscribe_RejectsWhenNoUpstream: when no publisher has touched
// the track AND no namespace match is available, SUBSCRIBE returns
// RequestDoesNotExist (the on-demand upstream subscribe path only
// kicks in when a matching namespace publisher exists).
func TestSubscribe_RejectsWhenNoUpstream(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_ServedFromExistingUpstream is the canonical aggregation
// test: a publisher claims a track, then a subscriber arrives on a separate
// session and receives SUBSCRIBE_OK immediately from the cached upstream
// state. No on-demand upstream subscribe is involved here; the upstream
// was already Established.
func TestSubscribe_ServedFromExistingUpstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackAlias:      42,
		TrackProperties: []byte("hello props"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)

	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
	if got := string(subStream.OK.TrackProperties); got != "hello props" {
		t.Fatalf("TrackProperties = %q, want %q", got, "hello props")
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

// TestPublish_ForwardsToSubscribeTracks verifies §6.1 / §9.5: when a
// SUBSCRIBE_TRACKS is open and a PUBLISH arrives for a matching namespace, the
// relay forwards the PUBLISH to the subscriber on its OWN new bidirectional
// stream (accepted via AcceptRequest), NOT multiplexed onto the
// SUBSCRIBE_TRACKS request stream. This is the precondition for PUBLISH_SKIPPED
// (§10.20): the forward consumes the subscriber's bidi-stream credit.
func TestPublish_ForwardsToSubscribeTracks(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, subSess)

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video"), []byte("cam7")},
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
	if pub.TrackAlias != 99 {
		// PUBLISH forwarding does not remap the alias on this layer.
		// Pin the current behaviour so we notice when remapping arrives.
		t.Fatalf("forwarded TrackAlias = %d, want 99 (preserves the publisher's alias)", pub.TrackAlias)
	}
	// §10.19.1: the SUBSCRIBE_TRACKS omitted FORWARD and GROUP_ORDER, so the
	// forwarded PUBLISH carries neither (FORWARD defaults to 1, GROUP_ORDER to
	// the publisher's preference).
	if p, ok := pub.Parameters.Find(message.ParamForward); ok {
		t.Errorf("forwarded FORWARD present (=%d), want omitted", p.Byte)
	}
	if p, ok := pub.Parameters.Find(message.ParamGroupOrder); ok {
		t.Errorf("forwarded GROUP_ORDER present (=%d), want omitted", p.Byte)
	}
}

// TestPublish_ForwardsSubscribeTracksParams pins §10.19.1: the FORWARD
// (§10.2.17) and GROUP_ORDER (§10.2.8) parameters on a SUBSCRIBE_TRACKS are
// copied onto the PUBLISH the relay generates for that subscriber.
func TestPublish_ForwardsSubscribeTracksParams(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
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
		Namespace:  wire.TrackNamespace{[]byte("video"), []byte("cam7")},
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
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
		Parameters: message.Parameters{
			message.GroupOrderParam(message.GroupOrder(0x07)), // out of range
		},
	})

	select {
	case <-subSess.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("session not closed after out-of-range GROUP_ORDER SUBSCRIBE_TRACKS (§10.2.8)")
	}
}

// TestPublish_DuplicateAliasRejected pins the §11.1 duplicate-alias rule:
// reusing the same Track Alias on the same session for a different
// {namespace, name} pair must fail. The session-level RegisterInboundTrackAlias
// already enforces this; here we verify the request-level surfacing.
func TestPublish_DuplicateAliasRejected(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	stream1, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("first Publish: %v", err)
	}
	defer stream1.Close()

	_, err = clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam2"),
		TrackAlias: 7,
	})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)
}

// TestSubscribe_OnDemandUpstreamSubscribe is the canonical test for the
// on-demand upstream subscribe path: a
// publisher advertises a namespace via PUBLISH_NAMESPACE; a subscriber on a
// different session asks for a track under that namespace; the relay must
// issue an upstream SUBSCRIBE to the publisher, wait for SUBSCRIBE_OK, and
// only then reply SUBSCRIBE_OK downstream.
func TestSubscribe_OnDemandUpstreamSubscribe(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// Publisher advertises ("video",) via PUBLISH_NAMESPACE, then runs a
	// goroutine that accepts the upstream SUBSCRIBE the relay will issue
	// and replies SUBSCRIBE_OK.
	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
			TrackProperties: []byte("upstream props"),
		}); err != nil {
			t.Errorf("publisher SubscribeOK reply: %v", err)
			return
		}
	}()

	subSess := dialAnotherClient(t, pubSess)

	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
	if got := string(subStream.OK.TrackProperties); got != "upstream props" {
		t.Fatalf("downstream TrackProperties = %q, want %q", got, "upstream props")
	}
}

// upstreamForwardValue runs the publisher side of one on-demand upstream
// SUBSCRIBE and reports the FORWARD parameter the relay sent: (0/1, present)
// when FORWARD is on the SUBSCRIBE, or (0, false) when it is omitted (§10.2.17
// default 1). It replies SUBSCRIBE_OK and drains follow-ups. The result arrives
// on the returned channel once the relay's upstream SUBSCRIBE is accepted.
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
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	fwd := upstreamForwardValue(t, pubSess)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  wire.TrackNamespace{[]byte("video")},
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
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	fwd := upstreamForwardValue(t, pubSess)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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

// TestSubscribe_UpstreamResumedWhenForwardingSubscriberJoins pins §9.2: a
// Forward=0 subscriber establishes a paused (Forward=0) upstream; when a second
// Forward=1 subscriber reuses that upstream, the relay MUST resume it by
// sending an upstream REQUEST_UPDATE with Forward=1.
func TestSubscribe_UpstreamResumedWhenForwardingSubscriberJoins(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
		Namespace:  wire.TrackNamespace{[]byte("video")},
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
		Namespace: wire.TrackNamespace{[]byte("video")},
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

// acceptUpstreamSubscribe runs the publisher side of one on-demand upstream
// SUBSCRIBE: accept the relay's request, reply SUBSCRIBE_OK with the given
// alias, then drain follow-ups. The returned channel closes when the relay
// ends the subscription (reset / FIN errors the drain).
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

// TestSubscribe_UpstreamSurvivesInitiatingSubscriber is the §9.4 aggregation
// lifetime test: subscriber A triggers the on-demand upstream SUBSCRIBE,
// subscriber B reuses it, then A's whole session goes away. The upstream
// subscription serves B, so it must survive — B keeps receiving objects and
// does NOT get a spurious PUBLISH_DONE "upstream gone".
func TestSubscribe_UpstreamSurvivesInitiatingSubscriber(t *testing.T) {
	t.Parallel()
	closed := &recordingMetrics{}
	pubSess, teardown := connectRelay(t, relay.Config{Metrics: closed})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	const upstreamAlias = uint64(77)
	acceptUpstreamSubscribe(t, pubSess, upstreamAlias)

	subA := dialAnotherClient(t, pubSess)
	subAStream, err := subA.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("subscriber A Subscribe: %v", err)
	}
	defer subAStream.Close()

	subB := dialAnotherClient(t, pubSess)
	subBStream, err := subB.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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

// TestSubscribe_LastDownstreamTearsDownUpstream pins the inverse lifetime
// rule: when the LAST downstream subscriber of an on-demand upstream leaves,
// the relay ends its upstream subscription (closes the request stream,
// §10.7) instead of letting the publisher stream into a void forever.
func TestSubscribe_LastDownstreamTearsDownUpstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	subscriptionEnded := acceptUpstreamSubscribe(t, pubSess, 77)

	subSess := dialAnotherClient(t, pubSess)
	subStream, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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

// TestSubscribe_NoMatchingPublisher_RejectsDoesNotExist pins the 5e
// fallback: if no PUBLISH_NAMESPACE matches, the relay still rejects with
// RequestDoesNotExist. The Discovery Store path will relax this for
// cross-relay tracks.
func TestSubscribe_NoMatchingPublisher_RejectsDoesNotExist(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNSStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	_, err = subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("audio")}, // no publisher for this namespace
		Name:      []byte("mic"),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_UpstreamRejects_PropagatesRejection verifies the failure
// path: when the upstream publisher rejects the relay's SUBSCRIBE, the
// downstream subscriber must also see a REQUEST_ERROR. The error code may
// not match exactly (the relay normalises it) but it should signal failure.
func TestSubscribe_UpstreamRejects_PropagatesRejection(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubNSStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
		_ = req.RejectError(moqt.RequestUnauthorized, "policy denial")
	}()

	subSess := dialAnotherClient(t, pubSess)
	_, err = subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	// The relay normalises upstream rejection to RequestDoesNotExist
	// since "no upstream is available" is the right downstream signal.
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestSubscribe_AuthDenialUsesPolicyCode pins auth precedence on the
// SUBSCRIBE arm even when the track does not exist locally — the auth check
// runs before the track lookup.
func TestSubscribe_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: errors.New("token expired")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.subscribeCalls.Load(); got != 1 {
		t.Errorf("subscribeCalls = %d, want 1", got)
	}
}

// TestSubscribe_PublisherDisappears_EmitsPublishDone pins §10.11
// publisher-side termination. When the upstream publisher's session
// dies (here: we explicitly close the publisher session), the relay
// must notify every dependent downstream subscriber by writing a
// PUBLISH_DONE message with [moqt.PublishDoneTrackEnded] on each
// subscriber's request stream. Before this fix, the subscriber would
// see an idle stream that never produced another byte.
func TestSubscribe_PublisherDisappears_EmitsPublishDone(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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

// TestSubscribe_PublisherDisappears_StreamClosesAfterPublishDone pins
// the second half of the contract: after the relay sends
// PUBLISH_DONE it must also FIN the request stream so the subscriber
// can release its handler. message.Parse on a FIN'd stream returns
// io.EOF after the last message is consumed.
func TestSubscribe_PublisherDisappears_StreamClosesAfterPublishDone(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 1,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
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

// TestSubscribe_NoAliasCollisionWhenAlsoPublishing is a regression test for a
// conferencing client that PUBLISHes and SUBSCRIBEs on the same session. The
// relay's outbound alias space (used to deliver tracks downstream) is
// independent of the inbound aliases the client chose for its own PUBLISHes
// (§11.1). Both spaces start at 0, so a session that publishes alias 0 and
// then subscribes to a peer track (relay allocates outbound alias 0) must not
// see a spurious "alias collision" that rejects the SUBSCRIBE.
func TestSubscribe_NoAliasCollisionWhenAlsoPublishing(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// The client publishes its own track, taking inbound alias 0.
	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("room"), []byte("self")},
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
		Namespace:  wire.TrackNamespace{[]byte("room"), []byte("peer")},
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
		Namespace: wire.TrackNamespace{[]byte("room"), []byte("peer")},
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Subscribe to peer track (alias collision regression): %v", err)
	}
	defer subStream.Close()
}

// TestPublish_SavesLargestObjectFromPublish pins §10.2.16 item 1 on the PUBLISH
// path: a LARGEST_OBJECT on an inbound PUBLISH is one of the values the relay's
// own watermark MUST be the largest of, so it has to reach the track entry even
// though no object has arrived yet.
//
// Observable through the next SUBSCRIBE: §10.2.16 requires the relay to include
// LARGEST_OBJECT once objects exist on the track, and that value is the
// subscriber's Joining Location for a §10.12.2 Joining FETCH. Before the fix the
// parameter was dropped, so the relay claimed to know nothing and the backfill
// was unreachable.
func TestPublish_SavesLargestObjectFromPublish(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	ns := wire.TrackNamespace{[]byte("video")}
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns,
		Name:       []byte("cam1"),
		TrackAlias: 42,
		Parameters: message.Parameters{message.LargestObjectParam(5, 9)},
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: []byte("cam1")})
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

// TestPublish_ForwardedPublishCarriesEntryLargestObject pins the other half of
// §10.2.16 for the PUBLISH-forwarding path: the relay MUST send the largest of
// everything it has observed, so the LARGEST_OBJECT on a PUBLISH it generates for
// a SUBSCRIBE_TRACKS holder is re-derived from the track entry rather than copied
// through from the upstream's own PUBLISH.
//
// Two publishers on one track (§9.5) is what separates the two behaviours. The
// second announces a *lower* watermark than the first, so copying it through
// would advertise {3,4} when the relay has already observed {9,9} — a value below
// its own maximum, which is exactly what §10.2.16 forbids. With one publisher the
// two readings coincide and the bug is invisible, which is why this test needs
// the second one.
func TestPublish_ForwardedPublishCarriesEntryLargestObject(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	ns := wire.TrackNamespace{[]byte("video"), []byte("cam7")}

	// First publisher sets the entry's watermark to {9,9}.
	pubA := dialAnotherClient(t, subSess)
	pubStreamA, err := pubA.Publish(t.Context(), &message.Publish{
		Namespace:  ns,
		Name:       []byte("rtp"),
		TrackAlias: 99,
		Parameters: message.Parameters{message.LargestObjectParam(9, 9)},
	})
	if err != nil {
		t.Fatalf("Publish A: %v", err)
	}
	defer pubStreamA.Close()
	if _, err := subSess.AcceptRequest(t.Context()); err != nil {
		t.Fatalf("AcceptRequest (A): %v", err)
	}

	// Second publisher on the SAME track announces a lower one.
	pubB := dialAnotherClient(t, subSess)
	pubStreamB, err := pubB.Publish(t.Context(), &message.Publish{
		Namespace:  ns,
		Name:       []byte("rtp"),
		TrackAlias: 100,
		Parameters: message.Parameters{message.LargestObjectParam(3, 4)},
	})
	if err != nil {
		t.Fatalf("Publish B: %v", err)
	}
	defer pubStreamB.Close()

	req, err := subSess.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest (B): %v", err)
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
