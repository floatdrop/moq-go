package relay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/relay"
)

var errRejectWrite = errors.New("transport gone")

// firstRequestStream is the relay-side [sessiontest.FaultOp] ordinal of a
// client's first request stream: 1 and 2 are the control streams. It is
// deterministic only while one request at a time runs on the conn.
const firstRequestStream = 3

// TestSessionHandler_FailedRejectWriteKeepsTheSessionAlive: a REQUEST_ERROR the
// relay cannot write fails only that request, not the peer's session.
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
			Namespace: ns("video"),
			Name:      []byte("cam1"),
		}
	}

	// First SUBSCRIBE: denied by policy, and the relay's REQUEST_ERROR write
	// fails. The client cannot be told the reason, but it must be told the
	// request is over — RejectError resets the stream when it cannot send the
	// error (§3.3.3), so this call ends because the relay ended it, not
	// because the deadline below expired.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	switch _, err := clientSess.Subscribe(ctx, sub()); {
	case err == nil:
		t.Fatal("first Subscribe succeeded; want failure — its REQUEST_ERROR write was faulted")
	case errors.Is(err, context.DeadlineExceeded):
		t.Fatal("first Subscribe ended on its own deadline: the relay left the request " +
			"stream open after failing to write the REQUEST_ERROR")
	}

	// Second SUBSCRIBE on a fresh request stream: the session must still be
	// serving, and this rejection must arrive intact.
	_, err := clientSess.Subscribe(t.Context(), sub())
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)

	if got := auth.subscribeCalls.Load(); got != 2 {
		t.Errorf("AuthorizeSubscribe called %d times, want 2 — the session died with the first reject", got)
	}
}
