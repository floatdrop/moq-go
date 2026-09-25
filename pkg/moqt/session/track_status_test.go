package session_test

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestTrackStatusRoundTrip exercises the full TRACK_STATUS flow:
//
//  1. Client calls Session.TrackStatus → sends TRACK_STATUS on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies REQUEST_OK.
//  3. Client receives the REQUEST_OK (TrackStatusOK) from TrackStatus().
func TestTrackStatusRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	req := &message.TrackStatus{
		Namespace: ns,
		Name:      []byte("video"),
	}

	wantOK := &message.RequestOK{}

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.TrackStatusOK
	)

	// Server: accept TRACK_STATUS, verify, reply REQUEST_OK.
	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		ts, ok := r.First.(*message.TrackStatus)
		if !ok {
			serverErr = errors.New("server: expected *message.TrackStatus, got " + r.First.Type().String())
			return
		}
		// RequestID must have been assigned by the client (even, starts at 0).
		if ts.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}
		if string(ts.Name) != string(req.Name) {
			serverErr = errors.New("server: Name mismatch")
			return
		}
		serverErr = r.Reply(wantOK)
	})

	// Client: call TrackStatus, check result.
	wg.Go(func() {
		ts, err := cli.TrackStatus(ctx, req)
		if err != nil {
			clientErr = err
			return
		}
		defer ts.Close()
		gotOK = ts.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client TrackStatus: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// TestTrackStatusRejected verifies that Session.TrackStatus returns a
// *RequestRejectedError when the server replies with REQUEST_ERROR.
func TestTrackStatusRejected(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.RejectError(moqt.RequestDoesNotExist, "track not found")
	})

	wg.Go(func() {
		_, err := cli.TrackStatus(ctx, &message.TrackStatus{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("missing"),
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("TrackStatus error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}

// §10.15: "The bidi stream is closed with a FIN after TRACK_STATUS_OK or
// REQUEST_ERROR are sent" — "the subscriber cannot send REQUEST_UPDATE". And
// §3.3.2: a requester "MAY FIN immediately after sending a message if it will
// not send a REQUEST_UPDATE".

// TestTrackStatusStreamClosesBothWays: after TRACK_STATUS_OK the responder
// FINs its side, and the requester, which can never send a REQUEST_UPDATE,
// FINs its own.
func TestTrackStatusStreamClosesBothWays(t *testing.T) {
	client, server := openPair(t)
	accepted := make(chan session.Stream, 1)
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		accepted <- req.Stream
		_ = req.AcceptTrackStatus(nil)
	}()

	ts, err := client.TrackStatus(t.Context(), &message.TrackStatus{Name: []byte("t")})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	if msg, err := readWithin(ts.Stream, time.Second); !errors.Is(err, io.EOF) {
		t.Errorf("requester read after TRACK_STATUS_OK = (%v, %v), want the responder's FIN", msg, err)
	}
	if msg, err := readWithin(<-accepted, time.Second); !errors.Is(err, io.EOF) {
		t.Errorf("responder read = (%v, %v), want the requester's FIN", msg, err)
	}
}

// TestTrackStatusRequestUpdateClosesSession pins §10.9: "An endpoint that
// receives a REQUEST_UPDATE other than in the two cases above MUST close the
// session with a PROTOCOL_VIOLATION." TRACK_STATUS is not one of them.
func TestTrackStatusRequestUpdateClosesSession(t *testing.T) {
	client, server := openPair(t)
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = req.AcceptTrackStatus(nil)
	}()

	stream, err := session.OpenRequestForTest(client, &message.TrackStatus{
		RequestID: client.AllocRequestID(),
		Name:      []byte("t"),
	})
	if err != nil {
		t.Fatalf("open TRACK_STATUS: %v", err)
	}
	if _, err := message.Parse(stream); err != nil { // TRACK_STATUS_OK
		t.Fatalf("read TRACK_STATUS_OK: %v", err)
	}
	// From a goroutine: on the unbuffered test pipe the write only completes
	// if the server reads it.
	go func() { _ = message.Marshal(stream, &message.RequestUpdate{RequestID: client.AllocRequestID()}) }()
	requireClosedProtocolViolation(t, server)
}

// readWithin parses one message from s, failing the wait after d.
func readWithin(s session.Stream, d time.Duration) (message.Message, error) {
	type result struct {
		msg message.Message
		err error
	}
	got := make(chan result, 1)
	go func() {
		msg, err := message.Parse(s)
		got <- result{msg, err}
	}()
	select {
	case r := <-got:
		return r.msg, r.err
	case <-time.After(d):
		return nil, errors.New("timed out: the stream is still open")
	}
}

// TestTrackStatusMalformedFollowupClosesSession: bytes after TRACK_STATUS
// that do not even parse — an unknown type — close the session too. "An
// endpoint that receives an unknown message type MUST close the session"
// (§10); only the requester's FIN, a reset or session close end quietly.
func TestTrackStatusMalformedFollowupClosesSession(t *testing.T) {
	client, server := openPair(t)
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = req.AcceptTrackStatus(nil)
	}()

	stream, err := session.OpenRequestForTest(client, &message.TrackStatus{
		RequestID: client.AllocRequestID(),
		Name:      []byte("t"),
	})
	if err != nil {
		t.Fatalf("open TRACK_STATUS: %v", err)
	}
	if _, err := message.Parse(stream); err != nil {
		t.Fatalf("read TRACK_STATUS_OK: %v", err)
	}
	go func() { _ = wire.WriteFrame(stream, 0x3F00, nil) }() // unassigned type
	requireClosedProtocolViolation(t, server)
}
