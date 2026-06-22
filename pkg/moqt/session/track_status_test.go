package session_test

import (
	"errors"
	"sync"
	"testing"

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
