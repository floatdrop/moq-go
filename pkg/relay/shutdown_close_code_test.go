package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// closeCodeConn records the first code its session closes it with.
type closeCodeConn struct {
	session.Conn

	codes chan uint64
}

func (c *closeCodeConn) CloseWithError(code uint64, reason string) error {
	select {
	case c.codes <- code:
	default:
	}
	return c.Conn.CloseWithError(code, reason)
}

// stopCloseCode starts a relay with goaway as its GoawayTimeout, connects a
// client that never leaves, stops the relay with a ctx cancelled after
// stopWithin, and returns the code the relay closed the client's session with.
func stopCloseCode(t *testing.T, goaway, stopWithin time.Duration) moqt.SessionErrorCode {
	t.Helper()
	l := newPipeListener()
	codes := make(chan uint64, 1)
	l.wrap = func(c session.Conn) session.Conn { return &closeCodeConn{Conn: c, codes: codes} }
	r := relay.New(l, relay.Config{GoawayTimeout: goaway})
	go func() { _ = r.Start(t.Context()) }()

	conn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sess, err := session.Client(t.Context(), conn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close(moqt.SessionNoError, "cleanup") })
	// A round trip: the relay registers a session before serving its
	// requests, so Stop's snapshot now holds it rather than leaving it a
	// straggler drained on its own schedule.
	if _, err := sess.TrackStatus(t.Context(), &message.TrackStatus{Namespace: ns("none"), Name: []byte("x")}); err == nil {
		t.Fatal("TRACK_STATUS for an unknown track succeeded")
	}

	ctx, cancel := context.WithTimeout(context.Background(), stopWithin)
	defer cancel()
	_ = r.Stop(ctx)
	select {
	case code := <-codes:
		return moqt.SessionErrorCode(code)
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never closed the session")
		return 0
	}
}

// TestStopClosesWithGoawayTimeoutOnlyAfterGoaway: GOAWAY_TIMEOUT means "the
// peer took too long to close the session in response to a GOAWAY" (§3.5), so
// the relay closes with it only when it sent a GOAWAY and the grace period
// ran out; otherwise NO_ERROR.
func TestStopClosesWithGoawayTimeoutOnlyAfterGoaway(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name               string
		goaway, stopWithin time.Duration
		want               moqt.SessionErrorCode
	}{
		{"no GOAWAY configured", 0, 5 * time.Second, moqt.SessionNoError},
		{"GOAWAY grace period ran out", 50 * time.Millisecond, 5 * time.Second, moqt.SessionGoawayTimeout},
		{"Stop cancelled before the grace period", 5 * time.Second, 100 * time.Millisecond, moqt.SessionNoError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := stopCloseCode(t, tc.goaway, tc.stopWithin); got != tc.want {
				t.Errorf("closed with %#x, want %#x", uint64(got), uint64(tc.want))
			}
		})
	}
}
