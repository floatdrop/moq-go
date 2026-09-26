package session_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestRequestRejectedErrorCarriesRetryInterval: REQUEST_ERROR's Retry Interval
// is the minimum retry delay plus one, 0 meaning do not retry (§10.6.2).
func TestRequestRejectedErrorCarriesRetryInterval(t *testing.T) {
	for _, tc := range []struct {
		interval  uint64
		wantAfter time.Duration
		wantRetry bool
	}{
		{0, 0, false},
		{1, 0, true},
		{501, 500 * time.Millisecond, true},
		{math.MaxUint64, time.Duration(math.MaxInt64), true}, // largest varint (§1.4.1): clamped
	} {
		client, server := openPair(t)
		go func() {
			r, err := server.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			_ = message.Marshal(r.Stream, &message.RequestError{
				ErrorCode:     moqt.RequestExcessiveLoad,
				RetryInterval: tc.interval,
				ErrorReason:   "busy",
			})
			_ = r.Stream.Close()
		}()
		_, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		rej, ok := errors.AsType[*session.RequestRejectedError](err)
		if !ok {
			t.Fatalf("interval %d: Subscribe = %v, want *RequestRejectedError", tc.interval, err)
		}
		if rej.RetryInterval != tc.interval {
			t.Errorf("RetryInterval = %d, want %d", rej.RetryInterval, tc.interval)
		}
		after, retry := rej.RetryAfter()
		if after != tc.wantAfter || retry != tc.wantRetry {
			t.Errorf("interval %d: RetryAfter() = (%v, %v), want (%v, %v)",
				tc.interval, after, retry, tc.wantAfter, tc.wantRetry)
		}
	}
}

// TestUpdateHandlerRejectionKeepsRetryInterval: an UpdateHandler's
// *RequestRejectedError goes out with its Retry Interval intact (§10.6.2).
func TestUpdateHandlerRejectionKeepsRetryInterval(t *testing.T) {
	client, server := openPair(t)
	sub, pub := subscribePair(t, client, server)
	b := pub.Broker()
	b.HandleUpdates(func(*message.RequestUpdate) (*message.RequestOK, error) {
		return nil, &session.RequestRejectedError{
			Code:          moqt.RequestExcessiveLoad,
			Reason:        "busy",
			RetryInterval: 5001,
		}
	})
	go func() { _ = b.Serve(t.Context(), nil) }()

	_, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.RetryInterval != 5001 {
		t.Fatalf("Update = %v, want REQUEST_ERROR with Retry Interval 5001", err)
	}
}

// TestRejectSendsRetryInterval: Request.Reject puts the Retry Interval on the
// wire (§10.6.2), which RejectError cannot express.
func TestRejectSendsRetryInterval(t *testing.T) {
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = r.Reject(&session.RequestRejectedError{
			Code: moqt.RequestExcessiveLoad, Reason: "busy", RetryInterval: 251,
		})
	}()
	_, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok {
		t.Fatalf("Subscribe = %v, want *RequestRejectedError", err)
	}
	if rej.Code != moqt.RequestExcessiveLoad || rej.Reason != "busy" || rej.RetryInterval != 251 {
		t.Fatalf("rejection = %+v, want EXCESSIVE_LOAD \"busy\" Retry Interval 251", *rej)
	}
}

// TestRejectRefusesRedirect: a REDIRECT needs a Redirect structure (§10.6.2)
// that RequestRejectedError cannot carry, so Reject refuses without writing
// and the request can still be refused another way.
func TestRejectRefusesRedirect(t *testing.T) {
	client, server := openPair(t)
	refused := make(chan error, 1)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			refused <- err
			return
		}
		refused <- r.Reject(&session.RequestRejectedError{Code: moqt.RequestRedirect, Reason: "go elsewhere"})
		_ = r.RejectError(moqt.RequestDoesNotExist, "no")
	}()
	_, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if rej, ok := errors.AsType[*session.RequestRejectedError](err); !ok || rej.Code != moqt.RequestDoesNotExist {
		t.Fatalf("Subscribe = %v, want the DOES_NOT_EXIST sent after the refused REDIRECT", err)
	}
	if err := <-refused; err == nil {
		t.Fatal("Reject sent a REDIRECT without a Redirect structure")
	}
	requireStaysOpen(t, client, 50*time.Millisecond)
}

var errRejectWrite = errors.New("transport gone")

// TestRejectError_ResetsTheStreamWhenTheErrorCannotBeSent: if the
// REQUEST_ERROR write fails, RejectError cancels the stream (§3.3.3) so the
// requester is not left waiting for a response that never comes.
func TestRejectError_ResetsTheStreamWhenTheErrorCannotBeSent(t *testing.T) {
	t.Parallel()

	// Responder-side stream ordinals: 1 and 2 are the control streams, so 3
	// is the first bidi request stream, where the REQUEST_ERROR is written.
	const firstRequestStream = 3
	rawClient, rawServer := sessiontest.NewConnPair()
	serverConn := sessiontest.Faulty(rawServer, func(f sessiontest.FaultOp) error {
		if f.Op == sessiontest.OpStreamWrite && f.Stream == firstRequestStream {
			return errRejectWrite
		}
		return nil
	})
	client, server := openSessions(t, rawClient, serverConn, nil, nil)

	subErr := make(chan error, 1)
	go func() {
		_, err := client.Subscribe(t.Context(), &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		})
		subErr <- err
	}()

	req, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	if err := req.RejectError(moqt.RequestDoesNotExist, "no such track"); !errors.Is(err, errRejectWrite) {
		t.Fatalf("RejectError err = %v, want the faulted write error", err)
	}

	select {
	case err := <-subErr:
		if err == nil {
			t.Fatal("Subscribe succeeded, but its REQUEST_ERROR was never delivered")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Subscribe is still waiting: RejectError left the request stream open " +
			"after failing to write the REQUEST_ERROR, so the peer can never learn the request failed")
	}
}
