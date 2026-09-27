package relay_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// RENDEZVOUS_TIMEOUT (§10.2.6): a SUBSCRIBE for a track with no publisher is
// held for one to appear, up to the requested duration.

// subscribeRendezvous sends a SUBSCRIBE for video/cam1 with RENDEZVOUS_TIMEOUT d from
// sess under ctx, and delivers its outcome.
func subscribeRendezvous(ctx context.Context, sess *session.Session, d time.Duration) <-chan error {
	done := make(chan error, 1)
	go func() {
		sub, err := sess.Subscribe(ctx, &message.Subscribe{
			Namespace: ns("video"), Name: []byte("cam1"),
			Parameters: message.Parameters{message.RendezvousTimeoutParam(d)},
		})
		if err == nil {
			defer sub.Close()
		}
		done <- err
	}()
	return done
}

// requireHeld fails if the SUBSCRIBE is answered within 200ms.
func requireHeld(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		t.Fatalf("SUBSCRIBE answered while no publisher is available: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

// awaitAnswer returns the SUBSCRIBE's outcome, failing after 2s.
func awaitAnswer(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("held SUBSCRIBE never answered")
		return nil
	}
}

// TestRendezvous_PublisherArrives: a held SUBSCRIBE "proceeds with the
// subscription normally" once a publisher appears, whether it PUBLISH_NAMESPACEs
// a covering namespace or PUBLISHes the track itself.
func TestRendezvous_PublisherArrives(t *testing.T) {
	t.Parallel()
	t.Run("PUBLISH_NAMESPACE", func(t *testing.T) {
		t.Parallel()
		subSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
		requireHeld(t, done)

		_, subs := publishNamespaceLate(t, subSess, ns("video"))
		awaitAcceptedSubscribe(t, subs, "video/cam1")
		if err := awaitAnswer(t, done); err != nil {
			t.Fatalf("held SUBSCRIBE: %v", err)
		}
	})
	t.Run("PUBLISH", func(t *testing.T) {
		t.Parallel()
		subSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
		requireHeld(t, done)

		publishVideoTrack(t, dialAnotherClient(t, subSess), "cam1", 7)
		if err := awaitAnswer(t, done); err != nil {
			t.Fatalf("held SUBSCRIBE: %v", err)
		}
	})
}

// TestRendezvous_KeepsWaitingPastDoesNotExist: a namespace publisher that
// answers DOES_NOT_EXIST leaves the track without a publisher, so the
// SUBSCRIBE stays held, and is not asked again when another publisher of the
// namespace arrives and serves it.
func TestRendezvous_KeepsWaitingPastDoesNotExist(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	refuser := dialAnotherClient(t, subSess)
	publishNS(t, refuser, "video")
	var (
		refused   atomic.Int32
		forwarded atomic.Bool
	)
	go func() {
		for {
			req, err := refuser.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			refused.Add(1)
			// The budget goes only to upstream relays; see
			// TestCrossRelay_RendezvousForwardsBudget.
			if sub, ok := req.First.(*message.Subscribe); ok {
				if _, has := sub.Parameters.Find(message.ParamRendezvousTimeout); has {
					forwarded.Store(true)
				}
			}
			_ = req.RejectError(moqt.RequestDoesNotExist, "not here")
		}
	}()

	done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
	requireHeld(t, done)
	if n := refused.Load(); n != 1 {
		t.Fatalf("the refusing publisher got %d SUBSCRIBEs, want 1", n)
	}

	_, subs := publishNamespaceLate(t, subSess, ns("video"))
	awaitAcceptedSubscribe(t, subs, "video/cam1")
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
	if n := refused.Load(); n != 1 {
		t.Fatalf("the refusing publisher got %d SUBSCRIBEs, want 1", n)
	}
	if forwarded.Load() {
		t.Fatal("a local publisher's SUBSCRIBE carried RENDEZVOUS_TIMEOUT")
	}
}

// TestRendezvous_OtherErrorEndsHold: a candidate failing with anything but
// DOES_NOT_EXIST ends the hold with its error, whichever order the candidates
// answer in, since the track may well have a publisher.
func TestRendezvous_OtherErrorEndsHold(t *testing.T) {
	t.Parallel()
	for _, codes := range [][2]moqt.RequestErrorCode{
		{moqt.RequestDoesNotExist, moqt.RequestInternalError},
		{moqt.RequestInternalError, moqt.RequestDoesNotExist},
	} {
		t.Run(fmt.Sprintf("%#x then %#x", uint64(codes[0]), uint64(codes[1])), func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			for _, code := range codes {
				pub := dialAnotherClient(t, subSess)
				publishNS(t, pub, "video")
				go func() {
					for {
						req, err := pub.AcceptRequest(t.Context())
						if err != nil {
							return
						}
						_ = req.RejectError(code, "refused")
					}
				}()
			}
			done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
			select {
			case err := <-done:
				requireRejectedWithCode(t, err, moqt.RequestInternalError)
			case <-time.After(time.Second):
				t.Fatal("SUBSCRIBE held, want INTERNAL_ERROR at once")
			}
		})
	}
}

// TestRendezvous_DrainingPublisherKeepsHold: a namespace's only publisher
// having sent GOAWAY leaves the track without a current publisher (§10.4: the
// relay initiates no request to it), so the SUBSCRIBE is held until another
// arrives, rather than refused with GOING_AWAY.
func TestRendezvous_DrainingPublisherKeepsHold(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	draining := dialAnotherClient(t, subSess)
	publishNS(t, draining, "video")
	go func() {
		for {
			req, err := draining.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			_ = req.RejectError(moqt.RequestDoesNotExist, "not yet draining")
		}
	}()
	if err := draining.SendGoaway(0, ""); err != nil {
		t.Fatalf("SendGoaway: %v", err)
	}
	// The relay has read the GOAWAY once a SUBSCRIBE without a hold is
	// refused with GOING_AWAY rather than the publisher's DOES_NOT_EXIST.
	waitFor(t, 2*time.Second, func() bool {
		_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
		rej, ok := errors.AsType[*session.RequestRejectedError](err)
		return ok && rej.Code == moqt.RequestGoingAway
	}, "the relay never read the publisher's GOAWAY")

	done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
	requireHeld(t, done)
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam1", 7)
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
}

// TestRendezvous_Expires: with no publisher by the deadline the relay answers
// REQUEST_ERROR TIMEOUT (§10.2.6), at the requested duration or at
// [relay.Config.MaxRendezvousTimeout] if shorter ("The relay MAY use a shorter
// timeout than requested").
func TestRendezvous_Expires(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		cfg       relay.Config
		requested time.Duration
	}{
		{"requested", relay.Config{}, 300 * time.Millisecond},
		{"capped", relay.Config{MaxRendezvousTimeout: 300 * time.Millisecond}, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, tc.cfg)
			t.Cleanup(teardown)
			start := time.Now()
			done := subscribeRendezvous(t.Context(), subSess, tc.requested)
			requireHeld(t, done)
			requireRejectedWithCode(t, awaitAnswer(t, done), moqt.RequestTimeout)
			if held := time.Since(start); held < 300*time.Millisecond {
				t.Fatalf("answered after %v, want the 300ms hold", held)
			}
		})
	}
}

// TestRendezvous_NoHold: RENDEZVOUS_TIMEOUT 0 "MUST immediately return
// REQUEST_ERROR with error code DOES_NOT_EXIST" (§10.2.6), as does any
// SUBSCRIBE on a relay configured to hold none.
func TestRendezvous_NoHold(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		cfg       relay.Config
		requested time.Duration
	}{
		{"zero", relay.Config{}, 0},
		{"relay holds none", relay.Config{MaxRendezvousTimeout: -1}, 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, tc.cfg)
			t.Cleanup(teardown)
			done := subscribeRendezvous(t.Context(), subSess, tc.requested)
			select {
			case err := <-done:
				requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
			case <-time.After(200 * time.Millisecond):
				t.Fatal("SUBSCRIBE held, want DOES_NOT_EXIST at once")
			}
		})
	}
}

// TestRendezvous_CancelEndsHold: a subscriber cancelling its held SUBSCRIBE
// (§3.3.3) ends the hold, so a publisher arriving afterwards is not
// SUBSCRIBEd for it.
func TestRendezvous_CancelEndsHold(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	ctx, cancel := context.WithCancel(t.Context())
	done := subscribeRendezvous(ctx, subSess, 5*time.Second)
	requireHeld(t, done)
	cancel()
	<-done
	time.Sleep(100 * time.Millisecond) // the relay reads the STOP_SENDING

	_, subs := publishNamespaceLate(t, subSess, ns("video"))
	requireNoSubscribe(t, subs)
}

// TestCrossRelay_RendezvousForwardsBudget: relay A forwards what is left of
// the hold on its upstream SUBSCRIBE, so relay B, whose namespace publisher
// has no such track yet, holds it too, and a PUBLISH on B then serves the
// subscriber on A. Without it B answers DOES_NOT_EXIST at once and A, having
// asked B, times out.
func TestCrossRelay_RendezvousForwardsBudget(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)
	// Stopped before t.Context ends, which would end the sessions under Stop.
	defer func() { relayA.stop(t); relayB.stop(t) }()

	pub := dialClient(t, relayB)
	publishNS(t, pub, "video")
	var refused atomic.Int32
	go func() {
		for {
			req, err := pub.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			refused.Add(1)
			_ = req.RejectError(moqt.RequestDoesNotExist, "no cam1 yet")
		}
	}()

	done := subscribeRendezvous(t.Context(), dialClient(t, relayA), 5*time.Second)
	requireHeld(t, done)
	if refused.Load() == 0 {
		t.Fatal("relay B never asked its namespace publisher")
	}

	publishVideoTrack(t, pub, "cam1", 7)
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
}

// TestCrossRelay_RendezvousRemoteNamespaceArrives: a namespace another relay
// comes to advertise through Discovery after the SUBSCRIBE arrived ends the
// hold, and the SUBSCRIBE goes there.
func TestCrossRelay_RendezvousRemoteNamespaceArrives(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)
	// Stopped before t.Context ends, which would end the sessions under Stop.
	defer func() { relayA.stop(t); relayB.stop(t) }()

	done := subscribeRendezvous(t.Context(), dialClient(t, relayA), 5*time.Second)
	requireHeld(t, done)

	pub := dialClient(t, relayB)
	subs := acceptSubscribes(t, pub)
	publishNS(t, pub, "video")
	awaitAcceptedSubscribe(t, subs, "video/cam1")
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
}

// TestCrossRelay_RendezvousArrivalCutsRemoteHoldShort: a publisher arriving
// on relay A while A's upstream SUBSCRIBE is held on relay B serves the
// subscriber at once, rather than once B's hold runs out ("If a publisher
// becomes available within this time, the relay proceeds with the
// subscription normally", §10.2.6).
func TestCrossRelay_RendezvousArrivalCutsRemoteHoldShort(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)
	// Stopped before t.Context ends, which would end the sessions under Stop.
	defer func() { relayA.stop(t); relayB.stop(t) }()

	refuser := dialClient(t, relayB)
	publishNS(t, refuser, "video")
	var refused atomic.Int32
	go func() {
		for {
			req, err := refuser.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			refused.Add(1)
			_ = req.RejectError(moqt.RequestDoesNotExist, "no cam1")
		}
	}()

	subSess := dialClient(t, relayA)
	done := subscribeRendezvous(t.Context(), subSess, 5*time.Second)
	requireHeld(t, done)
	if refused.Load() == 0 {
		t.Fatal("relay B never asked its namespace publisher")
	}

	publishVideoTrack(t, dialClient(t, relayA), "cam1", 7)
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
}

// TestCrossRelay_RendezvousReasksRemoteAfterCutShort: a publisher arriving on
// relay A that turns out not to have the track cuts A's upstream SUBSCRIBE
// held on relay B short, and A asks B again with what is left of the hold, so
// a PUBLISH on B afterwards still serves the subscriber.
func TestCrossRelay_RendezvousReasksRemoteAfterCutShort(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)
	// Stopped before t.Context ends, which would end the sessions under Stop.
	defer func() { relayA.stop(t); relayB.stop(t) }()

	refuse := func(sess *session.Session) *atomic.Int32 {
		var n atomic.Int32
		go func() {
			for {
				req, err := sess.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				n.Add(1)
				_ = req.RejectError(moqt.RequestDoesNotExist, "no cam1")
			}
		}()
		return &n
	}
	pubB := dialClient(t, relayB)
	publishNS(t, pubB, "video")
	refusedB := refuse(pubB)

	done := subscribeRendezvous(t.Context(), dialClient(t, relayA), 5*time.Second)
	requireHeld(t, done)

	pubA := dialClient(t, relayA)
	refusedA := refuse(pubA)
	publishNS(t, pubA, "video")
	waitFor(t, 2*time.Second, func() bool { return refusedA.Load() == 1 && refusedB.Load() >= 2 },
		"relay B's publisher was not asked again after the local publisher refused")

	publishVideoTrack(t, pubB, "cam1", 7)
	if err := awaitAnswer(t, done); err != nil {
		t.Fatalf("held SUBSCRIBE: %v", err)
	}
}
