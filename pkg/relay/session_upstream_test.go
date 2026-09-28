package relay_test

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
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

// TestSubscribe_UpstreamAliasReusableAfterTeardown: once the relay ends its
// upstream subscription, the publisher may use its Track Alias for a different
// Track (§11.1) without the relay closing the session with
// DUPLICATE_TRACK_ALIAS.
func TestSubscribe_UpstreamAliasReusableAfterTeardown(t *testing.T) {
	t.Parallel()
	pubSess := namespacePublisher(t, relay.Config{})
	subscriptionEnded := acceptUpstreamSubscribe(t, pubSess, 77)

	subSess := dialAnotherClient(t, pubSess)
	_ = subscribeCam1(t, subSess).Close()
	select {
	case <-subscriptionEnded:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher's subscription still open 2s after the last downstream left")
	}

	acceptUpstreamSubscribe(t, pubSess, 77)
	if _, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"), Name: []byte("cam2"),
	}); err != nil {
		t.Fatalf("Subscribe cam2 on the reused alias: %v", err)
	}
	select {
	case <-pubSess.Done():
		t.Fatal("relay closed the publisher's session on a reused alias")
	case <-time.After(50 * time.Millisecond):
	}
}

// TestSubscribe_UpstreamRejects_PropagatesRejection: an upstream REQUEST_ERROR
// code about the track passes downstream, GOING_AWAY among them (an upstream
// relay draining, as the relay answers for a draining publisher itself); one
// about the relay's own hop, or not defined for SUBSCRIBE (MALFORMED_TRACK
// answers a FETCH), becomes INTERNAL_ERROR (§10.6.2). The Retry Interval is
// kept either way.
func TestSubscribe_UpstreamRejects_PropagatesRejection(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		upstream, want moqt.RequestErrorCode
		retry          uint64
	}{
		{moqt.RequestDoesNotExist, moqt.RequestDoesNotExist, 0},
		{moqt.RequestExcessiveLoad, moqt.RequestExcessiveLoad, 501},
		{moqt.RequestTimeout, moqt.RequestTimeout, 1},
		{moqt.RequestMalformedTrack, moqt.RequestInternalError, 0},
		{moqt.RequestUnauthorized, moqt.RequestInternalError, 0},
		{moqt.RequestExpiredAuthToken, moqt.RequestInternalError, 2001},
		{moqt.RequestGoingAway, moqt.RequestGoingAway, 7001},
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

// TestSubscribe_NoPublisherYetRanking: when every candidate only says the
// track has no publisher yet, the refusal is the most actionable of their
// answers whatever the order they answer in: GOING_AWAY (retry soon), then
// TIMEOUT, then DOES_NOT_EXIST.
func TestSubscribe_NoPublisherYetRanking(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		a, b, want moqt.RequestErrorCode
	}{
		{moqt.RequestDoesNotExist, moqt.RequestGoingAway, moqt.RequestGoingAway},
		{moqt.RequestDoesNotExist, moqt.RequestTimeout, moqt.RequestTimeout},
		{moqt.RequestTimeout, moqt.RequestGoingAway, moqt.RequestGoingAway},
	} {
		for _, order := range [][2]moqt.RequestErrorCode{{tc.a, tc.b}, {tc.b, tc.a}} {
			t.Run(fmt.Sprintf("%#x then %#x", uint64(order[0]), uint64(order[1])), func(t *testing.T) {
				t.Parallel()
				subSess, teardown := connectRelay(t, relay.Config{})
				t.Cleanup(teardown)
				for _, code := range order {
					pub := dialAnotherClient(t, subSess)
					publishNS(t, pub, "video")
					go func() {
						for {
							req, err := pub.AcceptRequest(t.Context())
							if err != nil {
								return
							}
							_ = req.RejectError(code, "no cam1")
						}
					}()
				}
				_, err := subSess.Subscribe(
					t.Context(),
					&message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")},
				)
				requireRejectedWithCode(t, err, tc.want)
			})
		}
	}
}

// refusingPublishers adds, on subSess's relay, one PUBLISH_NAMESPACE video
// publisher per answer, in order; each answers every SUBSCRIBE with it.
func refusingPublishers(t *testing.T, subSess *session.Session, answers ...func(*session.Request)) {
	t.Helper()
	for _, answer := range answers {
		pub := dialAnotherClient(t, subSess)
		publishNS(t, pub, "video")
		go func() {
			for {
				req, err := pub.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				answer(req)
			}
		}()
	}
}

func refuse(code moqt.RequestErrorCode, retry uint64) func(*session.Request) {
	return func(r *session.Request) {
		_ = r.Reject(&session.RequestRejectedError{Code: code, RetryInterval: retry, Reason: "no"})
	}
}

// reset cancels the request at the transport (§3.3.3): no REQUEST_ERROR.
func reset(r *session.Request) {
	r.Stream.CancelRead(uint64(moqt.StreamResetCancelled))
	r.Stream.CancelWrite(uint64(moqt.StreamResetCancelled))
}

func subscribeCam1Rejected(t *testing.T, subSess *session.Session) *session.RequestRejectedError {
	t.Helper()
	_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok {
		t.Fatalf("Subscribe = %v, want a REQUEST_ERROR", err)
	}
	return rej
}

// TestSubscribe_UpstreamGoingAwayWithoutRetry: an upstream GOING_AWAY that says
// not to retry (Retry Interval 0, §10.6.2) speaks for the upstream's own
// draining hop, so the subscriber is told to retry after the relay's own
// interval, as when the relay sees the drain itself.
func TestSubscribe_UpstreamGoingAwayWithoutRetry(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	refusingPublishers(t, subSess, refuse(moqt.RequestGoingAway, 0))
	rej := subscribeCam1Rejected(t, subSess)
	if rej.Code != moqt.RequestGoingAway || rej.RetryInterval < 1001 || rej.RetryInterval > 1501 {
		t.Fatalf("got %#x with Retry Interval %d, want GOING_AWAY retrying after about 1s",
			uint64(rej.Code), rej.RetryInterval)
	}
}

// TestSubscribe_TiedRefusalsSoonestRetry: of refusals with the same code, the
// subscriber gets the one allowing the soonest retry, whatever the order;
// "SHOULD NOT be retried" (0, §10.6.2) only when every one says so.
func TestSubscribe_TiedRefusalsSoonestRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		code       moqt.RequestErrorCode
		a, b, want uint64
	}{
		{moqt.RequestDoesNotExist, 0, 501, 501},
		{moqt.RequestDoesNotExist, 0, 0, 0},
		{moqt.RequestTimeout, 3001, 1001, 1001},
	} {
		for _, order := range [][2]uint64{{tc.a, tc.b}, {tc.b, tc.a}} {
			t.Run(fmt.Sprintf("%#x %d then %d", uint64(tc.code), order[0], order[1]), func(t *testing.T) {
				t.Parallel()
				subSess, teardown := connectRelay(t, relay.Config{})
				t.Cleanup(teardown)
				refusingPublishers(t, subSess, refuse(tc.code, order[0]), refuse(tc.code, order[1]))
				if rej := subscribeCam1Rejected(t, subSess); rej.Code != tc.code || rej.RetryInterval != tc.want {
					t.Fatalf("got %#x with Retry Interval %d, want %#x with %d",
						uint64(rej.Code), rej.RetryInterval, uint64(tc.code), tc.want)
				}
			})
		}
	}
}

// TestSubscribe_TransportFailureIsNoPublisherYet: a candidate whose request
// fails at the transport, with no REQUEST_ERROR, says nothing about the
// track, so it ranks as DOES_NOT_EXIST (the code it is answered with): it
// does not mask another candidate's GOING_AWAY, and a RENDEZVOUS_TIMEOUT hold
// goes on (§10.2.6).
func TestSubscribe_TransportFailureIsNoPublisherYet(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"reset first", "reset last"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			answers := []func(*session.Request){reset, refuse(moqt.RequestGoingAway, 2001)}
			if order == "reset last" {
				answers[0], answers[1] = answers[1], answers[0]
			}
			refusingPublishers(t, subSess, answers...)
			if rej := subscribeCam1Rejected(t, subSess); rej.Code != moqt.RequestGoingAway {
				t.Fatalf("got %#x, want the other candidate's GOING_AWAY", uint64(rej.Code))
			}
		})
	}
	t.Run("hold", func(t *testing.T) {
		t.Parallel()
		subSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		refusingPublishers(t, subSess, reset)
		done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
		requireHeld(t, done)
		publishVideoTrack(t, dialAnotherClient(t, subSess), "cam1", 7)
		if err := awaitAnswer(t, done); err != nil {
			t.Fatalf("held SUBSCRIBE: %v", err)
		}
	})
}

// TestSubscribe_UnsupportedMandatoryPropertyOutranksMalformed: of two
// candidates whose SUBSCRIBE_OKs carry bad Track Properties, an unknown
// Mandatory Track Property (§2.5.1: UNSUPPORTED_EXTENSION, a MUST) wins over
// Properties that do not parse, whatever the order.
func TestSubscribe_UnsupportedMandatoryPropertyOutranksMalformed(t *testing.T) {
	t.Parallel()
	accept := func(props []byte) func(*session.Request) {
		return func(r *session.Request) {
			_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackAlias: 5, TrackProperties: props})
		}
	}
	for _, order := range []string{"mandatory first", "mandatory last"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			answers := []func(*session.Request){accept(mandatoryProps()), accept(malformedProps)}
			if order == "mandatory last" {
				answers[0], answers[1] = answers[1], answers[0]
			}
			refusingPublishers(t, subSess, answers...)
			if rej := subscribeCam1Rejected(t, subSess); rej.Code != moqt.RequestUnsupportedExtension {
				t.Fatalf("got %#x, want UNSUPPORTED_EXTENSION", uint64(rej.Code))
			}
		})
	}
}

// requireRetryableExcessiveLoad fails unless err is EXCESSIVE_LOAD inviting a
// retry after about a second (§10.6.2).
func requireRetryableExcessiveLoad(t *testing.T, err error) {
	t.Helper()
	requireRejectedWithCode(t, err, moqt.RequestExcessiveLoad)
	if rej, _ := errors.AsType[*session.RequestRejectedError](
		err,
	); rej.RetryInterval < 1001 ||
		rej.RetryInterval > 1500 {
		t.Fatalf("Retry Interval = %d, want a retry after about 1s", rej.RetryInterval)
	}
}

// TestRendezvous_NoStreamCreditEndsHold: a SUBSCRIBE the relay cannot even
// open to a live publisher, for want of bidi-stream credit, is not a sign the
// track has no publisher, so a RENDEZVOUS_TIMEOUT hold does not wait out its
// budget on it: the subscriber is answered at once, with EXCESSIVE_LOAD and a
// retry, since the relay "cannot process the request at this time"
// (§10.6.2).
func TestRendezvous_NoStreamCreditEndsHold(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	pub := dialAnotherClientWithLimits(t, subSess, -1, 0) // the relay may open no stream to it
	publishNS(t, pub, "video")
	done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
	select {
	case err := <-done:
		requireRetryableExcessiveLoad(t, err)
	case <-time.After(time.Second):
		t.Fatal("SUBSCRIBE held against a live publisher the relay had no stream credit for")
	}
}

// TestSubscribe_NoStreamCreditOutranksNoPublisherYet: a candidate the relay
// has no stream credit for is a live publisher, so its EXCESSIVE_LOAD wins
// over another candidate's GOING_AWAY, whatever the order; and a forwarded
// TRACK_STATUS gets the same answer.
func TestSubscribe_NoStreamCreditOutranksNoPublisherYet(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"credit-less first", "credit-less last"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			creditless := func() {
				pub := dialAnotherClientWithLimits(t, subSess, -1, 0)
				publishNS(t, pub, "video")
			}
			if order == "credit-less first" {
				creditless()
			}
			refusingPublishers(t, subSess, refuse(moqt.RequestGoingAway, 2001))
			if order == "credit-less last" {
				creditless()
			}
			_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
			requireRetryableExcessiveLoad(t, err)
		})
	}
	t.Run("TRACK_STATUS", func(t *testing.T) {
		t.Parallel()
		subSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		pub := dialAnotherClientWithLimits(t, subSess, -1, 0)
		publishNS(t, pub, "video")
		_, err := trackStatusCam1(t, subSess)
		requireRetryableExcessiveLoad(t, err)
	})
}

// TestSubscribe_OtherRefusalTieIsOrderFree: two refusals of the "any other"
// kind with the same Retry Interval, one answered as INTERNAL_ERROR (an
// UNAUTHORIZED about the relay's hop) and one passed on (EXCESSIVE_LOAD),
// give the subscriber the specific code whatever the order.
func TestSubscribe_OtherRefusalTieIsOrderFree(t *testing.T) {
	t.Parallel()
	for _, order := range []string{"specific first", "specific last"} {
		t.Run(order, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			answers := []func(*session.Request){
				refuse(moqt.RequestExcessiveLoad, 0),
				refuse(moqt.RequestUnauthorized, 0),
			}
			if order == "specific last" {
				answers[0], answers[1] = answers[1], answers[0]
			}
			refusingPublishers(t, subSess, answers...)
			if rej := subscribeCam1Rejected(t, subSess); rej.Code != moqt.RequestExcessiveLoad {
				t.Fatalf("got %#x, want EXCESSIVE_LOAD", uint64(rej.Code))
			}
		})
	}
}

// TestRendezvous_TransportFailureAskedAgain: a publisher whose SUBSCRIBE failed
// at the transport is asked again on the hold's next look, and serves the
// subscriber if it has the track by then.
func TestRendezvous_TransportFailureAskedAgain(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	var asked atomic.Int32
	refusingPublishers(t, subSess, func(r *session.Request) {
		if asked.Add(1) == 1 {
			reset(r)
			return
		}
		_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackAlias: 9})
	})
	done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
	requireHeld(t, done)
	// Another publisher of the namespace arrives, waking the hold.
	refusingPublishers(t, subSess, refuse(moqt.RequestDoesNotExist, 0))
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
	if n := asked.Load(); n != 2 {
		t.Fatalf("the reset publisher was asked %d times, want 2", n)
	}
}
