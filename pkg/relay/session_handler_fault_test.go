package relay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

var errRejectWrite = errors.New("transport gone")

// firstRequestStream is the relay-side [sessiontest.FaultOp] stream ordinal of
// a client's first bidirectional request stream. The MoQT control stream is a
// pair of unidirectional streams, so the relay's per-conn ordinals run: 1 the
// inbound control stream it accepts, 2 the outbound one it opens, and 3 the
// first request stream. Faulting by ordinal is only deterministic while the
// test drives one request at a time on that conn.
const firstRequestStream = 3

// TestSessionHandler_FailedRejectWriteKeepsTheSessionAlive pins the contract
// rejectAuth's doc comment states: "Any write failure is logged but otherwise
// swallowed — the stream is being torn down anyway." Swallowed means
// *stream*-scoped. A REQUEST_ERROR the relay cannot deliver must not cost the
// peer its whole session, because §9.5's "one bad request must not break an
// unrelated subscription" is exactly as true when the failure is ours.
//
// Nothing else in the suite reaches that branch: an in-process pipe never
// fails a write, so before [sessiontest.Faulty] the error arm was unreachable
// and turning the swallow into a session close would have gone unnoticed.
//
// The relay's own writes are faulted by ordinal — [firstRequestStream] is the
// stream carrying this REQUEST_ERROR.
//
// KNOWN GAP, deliberately not frozen here: [session.Request.RejectError]
// returns as soon as the REQUEST_ERROR marshal fails, skipping the CancelRead
// and FIN that §3.3.3 asks for, so the peer is left waiting on a stream that
// can never produce anything. This test therefore asserts only that the first
// request fails, not how.
func TestSessionHandler_FailedRejectWriteKeepsTheSessionAlive(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: relay.Deny(moqt.RequestUnauthorized, "test denial")}

	l := newPipeListener()
	l.faultFor = faultConn(1, func(f sessiontest.FaultOp) error {
		if f.Op == sessiontest.OpStreamWrite && f.Stream == firstRequestStream {
			return errRejectWrite
		}
		return nil
	})
	clientSess, teardown := connectRelayOn(t, relay.Config{Authorizer: auth}, l)
	defer teardown()

	sub := func() *message.Subscribe {
		return &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		}
	}

	// First SUBSCRIBE: denied by policy, and the relay's REQUEST_ERROR write
	// fails. It must not succeed; how it fails is left unasserted while the
	// gap above stands — today the deadline below is what ends this call.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := clientSess.Subscribe(ctx, sub()); err == nil {
		t.Fatal("first Subscribe succeeded; want failure — its REQUEST_ERROR write was faulted")
	}

	// Second SUBSCRIBE on a fresh request stream: the session must still be
	// serving, and this rejection must arrive intact.
	_, err := clientSess.Subscribe(t.Context(), sub())
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)

	if got := auth.subscribeCalls.Load(); got != 2 {
		t.Errorf("AuthorizeSubscribe called %d times, want 2 — the session died with the first reject", got)
	}
}
