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

// TestTrackStatusStreamClosesBothWays: the responder FINs after
// TRACK_STATUS_OK (§10.15), and the requester, which cannot send
// REQUEST_UPDATE, FINs its side too (§3.3.2).
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

// TestTrackStatusFollowupClosesSession: anything the requester sends after
// TRACK_STATUS closes the session with PROTOCOL_VIOLATION — a REQUEST_UPDATE
// (§10.9) or an unknown message type (§10).
func TestTrackStatusFollowupClosesSession(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(session.Stream, *session.Session) error
	}{
		{"REQUEST_UPDATE", func(s session.Stream, client *session.Session) error {
			return message.Marshal(s, &message.RequestUpdate{RequestID: client.AllocRequestID()})
		}},
		{"unknown message type", func(s session.Stream, _ *session.Session) error {
			return wire.WriteFrame(s, 0x3F00, nil) // unassigned type
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
			// From a goroutine: the write completes only as the server reads.
			go func() { _ = tc.write(stream, client) }()
			requireClosedProtocolViolation(t, server)
		})
	}
}

// TestAcceptPublishTrackPropertiesRejected: a PUBLISH with an unknown
// Mandatory Track Property is refused with UNSUPPORTED_EXTENSION (§2.5.1), and
// unparseable Track Properties with MALFORMED_TRACK.
func TestAcceptPublishTrackPropertiesRejected(t *testing.T) {
	for _, tc := range []struct {
		name  string
		props []byte
		want  moqt.RequestErrorCode
	}{
		{"unknown mandatory", message.AppendTrackProperties([]wire.KVPair{
			{Type: message.MandatoryTrackPropertyMin, IntVal: 1},
		}), moqt.RequestUnsupportedExtension},
		{"malformed", []byte{0x01}, moqt.RequestMalformedTrack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t,
				session.WithKnownMandatoryTrackProperties(map[message.PropertyType]struct{}{}))
			accepted := make(chan error, 1)
			go func() {
				r, err := server.AcceptRequest(t.Context())
				if err != nil {
					accepted <- err
					return
				}
				_, err = r.AcceptPublish()
				accepted <- err
			}()
			_, err := client.Publish(t.Context(), &message.Publish{Name: []byte("t"), TrackProperties: tc.props})
			rej, ok := errors.AsType[*session.RequestRejectedError](err)
			if !ok || rej.Code != tc.want {
				t.Fatalf("Publish = %v, want REQUEST_ERROR %v", err, tc.want)
			}
			if err := <-accepted; err == nil {
				t.Error("AcceptPublish accepted the track")
			}
		})
	}
}
