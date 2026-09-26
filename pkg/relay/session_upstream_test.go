package relay_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// On-demand upstream SUBSCRIBE: a SUBSCRIBE under a namespace a client
// advertised with PUBLISH_NAMESPACE makes the relay SUBSCRIBE that client, and
// the upstream follows its downstream subscribers (§9.2, §9.4).

// namespacePublisher starts a relay whose first client advertises video, so a
// SUBSCRIBE under it makes the relay SUBSCRIBE that client upstream.
func namespacePublisher(t *testing.T, cfg relay.Config) *session.Session {
	t.Helper()
	pubSess, teardown := connectRelay(t, cfg)
	t.Cleanup(teardown)
	publishNS(t, pubSess, "video")
	return pubSess
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

// TestSubscribe_OnDemandUpstreamSubscribe: a SUBSCRIBE under a published
// namespace makes the relay SUBSCRIBE upstream and answer downstream only after
// the upstream SUBSCRIBE_OK.
func TestSubscribe_OnDemandUpstreamSubscribe(t *testing.T) {
	t.Parallel()
	pubSess := namespacePublisher(t, relay.Config{})

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
		}
	}()

	subReq := subscribeCam1(t, dialAnotherClient(t, pubSess))
	<-pubResponded
	// §9.6: the Track Properties from the upstream SUBSCRIBE_OK are echoed on
	// the downstream one.
	if got, want := subReq.OK.TrackProperties, opaqueProps("upstream props"); !bytes.Equal(got, want) {
		t.Fatalf("downstream TrackProperties = %x, want %x", got, want)
	}
}

// upstreamForwardFor SUBSCRIBEs a downstream client with params and returns the
// FORWARD the relay's upstream SUBSCRIBE carried; absent means 1 (§10.2.18).
func upstreamForwardFor(t *testing.T, params ...message.Parameter) (value uint8, present bool) {
	t.Helper()
	pubSess := namespacePublisher(t, relay.Config{})
	type forward struct {
		value   uint8
		present bool
	}
	got := make(chan forward, 1)
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
		var f forward
		if p, ok := sub.Parameters.Find(message.ParamForward); ok {
			f = forward{value: p.Byte, present: true}
		}
		got <- f
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

	newCam1Subscriber(t, pubSess, params...)
	f := <-got
	return f.value, f.present
}

// TestSubscribe_UpstreamForwardPausedWhenDownstreamForwardZero pins §9.2: when
// the only downstream subscriber sets Forward=0, the relay exercises its
// discretion and pauses the upstream with an explicit FORWARD=0.
func TestSubscribe_UpstreamForwardPausedWhenDownstreamForwardZero(t *testing.T) {
	t.Parallel()
	if v, ok := upstreamForwardFor(t, message.ForwardParam(false)); !ok || v != 0 {
		t.Fatalf("upstream FORWARD = %d (present=%v), want an explicit 0", v, ok)
	}
}

// TestSubscribe_UpstreamForwardOmittedWhenDownstreamForwards pins §9.2: when a
// downstream subscriber wants forwarding (FORWARD omitted → default 1), the
// relay's upstream SUBSCRIBE omits FORWARD too (implicit 1).
func TestSubscribe_UpstreamForwardOmittedWhenDownstreamForwards(t *testing.T) {
	t.Parallel()
	if v, ok := upstreamForwardFor(t); ok {
		t.Fatalf("upstream FORWARD present (=%d), want omitted (implicit 1)", v)
	}
}

// TestSubscribe_UpstreamResumedWhenForwardingSubscriberJoins: a paused upstream
// is resumed with REQUEST_UPDATE FORWARD=1 when a Forward=1 subscriber joins
// (§9.2).
func TestSubscribe_UpstreamResumedWhenForwardingSubscriberJoins(t *testing.T) {
	t.Parallel()
	pubSess := namespacePublisher(t, relay.Config{})

	// The publisher expects an explicit Forward=0 SUBSCRIBE, then a resuming
	// REQUEST_UPDATE with Forward=1.
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
		// Answer the §10.9 REQUEST_UPDATE so the relay's resume completes
		// rather than timing out.
		if err := message.Marshal(req.Stream, &message.RequestOK{}); err != nil {
			t.Errorf("publisher REQUEST_OK: %v", err)
		}
		res <- r
	}()

	// Subscriber A (Forward=0) establishes the paused upstream; B (Forward
	// omitted, so 1) reuses it and must resume it.
	newCam1Subscriber(t, pubSess, message.ForwardParam(false))
	newCam1Subscriber(t, pubSess)

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

// TestSubscribe_UpstreamSurvivesInitiatingSubscriber: an on-demand upstream
// outlives the subscriber that triggered it while another still uses it (§9.4).
func TestSubscribe_UpstreamSurvivesInitiatingSubscriber(t *testing.T) {
	t.Parallel()
	closed := &recordingMetrics{}
	pubSess := namespacePublisher(t, relay.Config{Metrics: closed})
	const upstreamAlias = uint64(77)
	acceptUpstreamSubscribe(t, pubSess, upstreamAlias)

	subA := newCam1Subscriber(t, pubSess)
	subB := newCam1Subscriber(t, pubSess)

	// A leaves entirely, and the relay must keep the upstream B depends on.
	// Wait for it to evict A's subscription (SubscriptionClosed fires in
	// handleSubscribe's defer) so the publish below sees the post-removal
	// state.
	_ = subA.Close(0, "subscriber A leaving")
	waitFor(t, 2*time.Second, func() bool { return closed.subsClosed.Load() >= 1 },
		"relay never evicted subscriber A's subscription")

	reads := readNextSubgroup(t, subB)
	publishObjects(t, pubSess, upstreamAlias, 0, 1)
	if r := awaitSubgroupRead(t, reads); !slices.Equal(r.payloads, []string{"A"}) {
		t.Fatalf("subscriber B got %q after A left, want [A]", r.payloads)
	}
}

// TestSubscribe_LastDownstreamTearsDownUpstream: when the last downstream
// subscriber leaves, the relay ends its upstream subscription.
func TestSubscribe_LastDownstreamTearsDownUpstream(t *testing.T) {
	t.Parallel()
	pubSess := namespacePublisher(t, relay.Config{})
	subscriptionEnded := acceptUpstreamSubscribe(t, pubSess, 77)

	subStream := subscribeCam1(t, dialAnotherClient(t, pubSess))
	_ = subStream.Close()

	select {
	case <-subscriptionEnded:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher's subscription still open 2s after the last downstream left")
	}
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
			pubSess := namespacePublisher(t, relay.Config{})
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
			_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
			requireRejectedWithCode(t, err, tc.want)
			if rej, _ := errors.AsType[*session.RequestRejectedError](err); rej.RetryInterval != tc.retry {
				t.Fatalf("downstream Retry Interval %d, want the upstream's %d", rej.RetryInterval, tc.retry)
			}
		})
	}
}
