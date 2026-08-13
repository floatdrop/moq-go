package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestPublish_FailedRequestOKRollsBackRegistration pins both halves of the
// rollback in handlePublish's REQUEST_OK failure branch.
//
// The registration is deliberately performed inside the broker's setup
// closure, so by the time the REQUEST_OK write fails the track already has an
// upstream and the publisher's §11.1 Track Alias is already claimed — on a
// session the publisher does not believe is publishing anything. Both must be
// undone, and they fail differently:
//
//   - A leaked alias is claimed for the life of the session, so the publisher
//     can never use that alias for another track.
//   - A leaked upstream advertises a track no publisher is feeding, so
//     subscribers are accepted onto a track that will never produce an object.
//
// Probing them takes some care. Re-offering the SAME track under the same
// alias proves nothing, because RegisterInboundTrackAlias is idempotent when
// the alias still maps to the same track key — an earlier version of this test
// did exactly that and stayed green with both rollback lines deleted.
func TestPublish_FailedRequestOKRollsBackRegistration(t *testing.T) {
	t.Parallel()
	fault, fired, _ := requestStreamWriteFault()
	l := newPipeListener()
	l.faultFor = faultConn(1, fault)

	pubSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	const alias = 7
	const ns, faulted, other = "video", "cam1", "cam2"
	publish := func(name string) func() *message.Publish {
		return func() *message.Publish {
			return &message.Publish{
				Namespace:  wire.TrackNamespace{[]byte(ns)},
				Name:       []byte(name),
				TrackAlias: alias,
			}
		}
	}

	// The faulted attempt. Its REQUEST_OK never lands, so the call hangs
	// (Reply has no teardown of its own); run it detached and sync on the hook.
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pubSess.Publish(ctx, publish(faulted)())
	}()
	awaitFault(t, fired)

	// Half one: a DIFFERENT track under the SAME alias. If the alias is still
	// bound to the faulted track's key, RegisterInboundTrackAlias reports a
	// duplicate and the relay rejects this permanently.
	//
	// Retrying is not papering over flakiness: the rollback runs a few
	// instructions after the faulted write returns, so the first attempt can
	// race it, while a rollback that never happens leaves the alias claimed
	// forever. The loop converges immediately when the code is right and never
	// when it is not.
	pub := retryPublish(t, pubSess, publish(other), 3*time.Second)
	t.Cleanup(func() { _ = pub.Close() })

	// Half two: the faulted track must have no upstream. RemoveUpstream runs
	// BEFORE UnregisterInboundTrackAlias, so the accepted PUBLISH above is
	// proof it has already happened — no second barrier needed.
	_, err := dialAnotherClient(t, pubSess).Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte(ns)},
		Name:      []byte(faulted),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// retryPublish re-attempts a PUBLISH until it is accepted or the budget runs
// out, failing the test with the last rejection.
func retryPublish(
	t *testing.T,
	sess *session.Session,
	msg func() *message.Publish,
	budget time.Duration,
) *session.Publication {
	t.Helper()
	deadline := time.Now().Add(budget)
	for attempt := 1; ; attempt++ {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		pub, err := sess.Publish(ctx, msg())
		cancel()
		if err == nil {
			return pub
		}
		if time.Now().After(deadline) {
			t.Fatalf("PUBLISH of a second track under the same Track Alias still rejected "+
				"after %d attempts (last: %v) — the failed REQUEST_OK leaked its alias",
				attempt, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
