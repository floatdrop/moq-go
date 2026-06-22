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

// ---------------------------------------------------------------------------
// PublishNamespace
// ---------------------------------------------------------------------------

// TestPublishNamespaceRoundTrip exercises the full PUBLISH_NAMESPACE flow:
//
//  1. Client calls Session.PublishNamespace → sends PUBLISH_NAMESPACE on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies REQUEST_OK.
//  3. Client receives the REQUEST_OK from PublishNamespace().
func TestPublishNamespaceRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	req := &message.PublishNamespace{
		Namespace: ns,
	}

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.RequestOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		pn, ok := r.First.(*message.PublishNamespace)
		if !ok {
			serverErr = errors.New("server: expected *message.PublishNamespace, got " + r.First.Type().String())
			return
		}
		if pn.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}
		serverErr = r.Reply(&message.RequestOK{})
	})

	wg.Go(func() {
		pub, err := cli.PublishNamespace(ctx, req)
		if err != nil {
			clientErr = err
			return
		}
		defer pub.Close()
		gotOK = pub.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client PublishNamespace: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// TestPublishNamespaceRejected verifies that Session.PublishNamespace returns a
// *RequestRejectedError when the server replies with REQUEST_ERROR.
func TestPublishNamespaceRejected(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.RejectError(moqt.RequestDoesNotExist, "namespace not found")
	})

	wg.Go(func() {
		_, err := cli.PublishNamespace(ctx, &message.PublishNamespace{
			Namespace: wire.TrackNamespace{[]byte("ns")},
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("PublishNamespace error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}

// ---------------------------------------------------------------------------
// SubscribeNamespace
// ---------------------------------------------------------------------------

// TestSubscribeNamespaceRoundTrip exercises the full SUBSCRIBE_NAMESPACE flow:
//
//  1. Client calls Session.SubscribeNamespace → sends SUBSCRIBE_NAMESPACE on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies REQUEST_OK.
//  3. Client receives the REQUEST_OK from SubscribeNamespace().
func TestSubscribeNamespaceRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	prefix := wire.TrackNamespace{[]byte("example.com")}
	req := &message.SubscribeNamespace{
		TrackNamespacePrefix: prefix,
	}

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.RequestOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		sn, ok := r.First.(*message.SubscribeNamespace)
		if !ok {
			serverErr = errors.New("server: expected *message.SubscribeNamespace, got " + r.First.Type().String())
			return
		}
		if sn.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}
		serverErr = r.Reply(&message.RequestOK{})
	})

	wg.Go(func() {
		sub, err := cli.SubscribeNamespace(ctx, req)
		if err != nil {
			clientErr = err
			return
		}
		defer sub.Close()
		gotOK = sub.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client SubscribeNamespace: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// TestSubscribeNamespaceRejected verifies that Session.SubscribeNamespace returns a
// *RequestRejectedError when the server replies with REQUEST_ERROR.
func TestSubscribeNamespaceRejected(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.RejectError(moqt.RequestDoesNotExist, "namespace not found")
	})

	wg.Go(func() {
		_, err := cli.SubscribeNamespace(ctx, &message.SubscribeNamespace{
			TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("SubscribeNamespace error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}

// ---------------------------------------------------------------------------
// SubscribeTracks
// ---------------------------------------------------------------------------

// TestSubscribeTracksRoundTrip exercises the full SUBSCRIBE_TRACKS flow:
//
//  1. Client calls Session.SubscribeTracks → sends SUBSCRIBE_TRACKS on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies REQUEST_OK.
//  3. Client receives the REQUEST_OK from SubscribeTracks().
func TestSubscribeTracksRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	prefix := wire.TrackNamespace{[]byte("example.com")}
	req := &message.SubscribeTracks{
		TrackNamespacePrefix: prefix,
	}

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.RequestOK
	)

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		st, ok := r.First.(*message.SubscribeTracks)
		if !ok {
			serverErr = errors.New("server: expected *message.SubscribeTracks, got " + r.First.Type().String())
			return
		}
		if st.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}
		serverErr = r.Reply(&message.RequestOK{})
	})

	wg.Go(func() {
		ts, err := cli.SubscribeTracks(ctx, req)
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
		t.Fatalf("client SubscribeTracks: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
}

// TestSubscribeTracksRejected verifies that Session.SubscribeTracks returns a
// *RequestRejectedError when the server replies with REQUEST_ERROR.
func TestSubscribeTracksRejected(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var wg sync.WaitGroup

	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			return
		}
		_ = r.RejectError(moqt.RequestDoesNotExist, "namespace not found")
	})

	wg.Go(func() {
		_, err := cli.SubscribeTracks(ctx, &message.SubscribeTracks{
			TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("SubscribeTracks error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}
