package session_test

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestPublishRoundTrip exercises the full PUBLISH flow:
//
//  1. Client calls Session.Publish → sends PUBLISH on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies REQUEST_OK.
//  3. Client receives the open stream from Publish().
func TestPublishRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	req := &message.Publish{
		Namespace:  ns,
		Name:       []byte("video"),
		TrackAlias: 42,
	}

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotStream session.Stream
	)

	// Server: accept PUBLISH, verify fields, reply REQUEST_OK.
	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		pub, ok := r.First.(*message.Publish)
		if !ok {
			serverErr = errors.New("server: expected *message.Publish, got " + r.First.Type().String())
			return
		}
		// RequestID must have been assigned by the client (even, starts at 0).
		if pub.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}
		if string(pub.Name) != string(req.Name) {
			serverErr = errors.New("server: Name mismatch")
			return
		}
		if pub.TrackAlias != req.TrackAlias {
			serverErr = errors.New("server: TrackAlias mismatch")
			return
		}
		serverErr = r.Reply(&message.RequestOK{})
	})

	// Client: call Publish, check the returned stream is non-nil.
	wg.Go(func() {
		stream, err := cli.Publish(ctx, req)
		if err != nil {
			clientErr = err
			return
		}
		gotStream = stream
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Publish: %v", clientErr)
	}
	if gotStream == nil {
		t.Fatal("Publish returned nil stream")
	}
	_ = gotStream.Close()
}

// TestPublishRejected verifies that Session.Publish returns a
// *RequestRejectedError when the server replies with REQUEST_ERROR.
func TestPublishRejected(t *testing.T) {
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
		_, err := cli.Publish(ctx, &message.Publish{
			Namespace:  wire.TrackNamespace{[]byte("ns")},
			Name:       []byte("missing"),
			TrackAlias: 1,
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("Publish error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}

// TestPublicationDone verifies Publication.Done writes a PUBLISH_DONE whose
// §10.11 Stream Count reflects the subgroups opened via the handle, with the
// given code and reason, and then FINs the request stream.
func TestPublicationDone(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		gotDone   *message.PublishDone
	)

	// Server: accept the PUBLISH, reply OK, drain the two subgroup streams the
	// client opens (the in-process pipe is synchronous), then read PUBLISH_DONE
	// off the request stream.
	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		if err := r.Reply(&message.RequestOK{}); err != nil {
			serverErr = err
			return
		}
		for range 2 {
			ds, err := srv.AcceptDataStream(ctx)
			if err != nil {
				serverErr = err
				return
			}
			_, _ = io.Copy(io.Discard, ds)
		}
		msg, err := message.Parse(r.Stream)
		if err != nil {
			serverErr = err
			return
		}
		pd, ok := msg.(*message.PublishDone)
		if !ok {
			serverErr = fmt.Errorf("got %T on request stream, want *message.PublishDone", msg)
			return
		}
		gotDone = pd
	})

	// Client: publish, open two subgroups via the handle, end with Done.
	wg.Go(func() {
		pub, err := cli.Publish(ctx, &message.Publish{
			Namespace: wire.Namespace("ns"),
			Name:      []byte("track"),
		})
		if err != nil {
			return
		}
		for g := range uint64(2) {
			sg, err := pub.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDImplicitZero,
				GroupID:        g,
			})
			if err != nil {
				return
			}
			_ = sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("x")})
			_ = sg.Close()
		}
		_ = pub.Done(moqt.PublishDoneTrackEnded, "bye")
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if gotDone == nil {
		t.Fatal("no PUBLISH_DONE received")
	}
	if gotDone.StatusCode != moqt.PublishDoneTrackEnded {
		t.Errorf("StatusCode = %v, want PublishDoneTrackEnded", gotDone.StatusCode)
	}
	if gotDone.StreamCount != 2 {
		t.Errorf("StreamCount = %d, want 2 (subgroups opened via the handle)", gotDone.StreamCount)
	}
	if gotDone.ErrorReason != "bye" {
		t.Errorf("ErrorReason = %q, want %q", gotDone.ErrorReason, "bye")
	}
}

// TestRequestAcceptPublish verifies AcceptPublish registers the publisher's
// Track Alias (§11.1) and replies REQUEST_OK so the client's Publish succeeds.
func TestRequestAcceptPublish(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var (
		wg        sync.WaitGroup
		serverErr error
		gotKey    track.Key
		gotOK     bool
	)
	wg.Go(func() {
		req, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		if _, err := req.AcceptPublish(); err != nil {
			serverErr = err
			return
		}
		gotKey, gotOK = srv.LookupInboundTrackAlias(7)
	})
	var clientErr error
	wg.Go(func() {
		_, clientErr = cli.Publish(ctx, &message.Publish{
			Namespace:  wire.Namespace("ns"),
			Name:       []byte("track"),
			TrackAlias: 7,
		})
	})
	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Publish: %v", clientErr)
	}
	if !gotOK {
		t.Fatal("AcceptPublish did not register the inbound Track Alias")
	}
	if want := track.NewKey(wire.Namespace("ns"), []byte("track")); gotKey != want {
		t.Errorf("registered key = %v, want %v", gotKey, want)
	}
}

// TestOpenPublish_SuccessDeliversPublish verifies the happy path: OpenPublish
// assigns a Request ID, writes PUBLISH as the stream's first message, and the
// peer accepts a bidi request carrying that exact PUBLISH.
func TestOpenPublish_SuccessDeliversPublish(t *testing.T) {
	t.Parallel()
	client, server := openPairWithLimits(t, -1)

	var (
		wg  sync.WaitGroup
		req *session.Request
		err error
	)
	wg.Go(func() { req, err = server.AcceptRequest(t.Context()) })

	m := &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 7,
	}
	stream, openErr := client.OpenPublish(m)
	if openErr != nil {
		t.Fatalf("OpenPublish: %v", openErr)
	}
	defer stream.Close()
	// Client Request IDs are even (§10.1); the first allocation is 0.
	if m.RequestID%2 != 0 {
		t.Fatalf("client Request ID %d is not even", m.RequestID)
	}

	wg.Wait()
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	pub, ok := req.First.(*message.Publish)
	if !ok {
		t.Fatalf("server got %T, want *message.Publish", req.First)
	}
	if string(pub.Name) != "cam1" || pub.TrackAlias != 7 {
		t.Fatalf("server got Publish{Name:%q, Alias:%d}, want {cam1, 7}", pub.Name, pub.TrackAlias)
	}
}

// TestOpenPublish_ExhaustedCreditReturnsErrNoStreamCredit pins the
// PUBLISH_BLOCKED trigger: with the client's bidi credit used up, OpenPublish
// returns session.ErrNoStreamCredit rather than blocking.
func TestOpenPublish_ExhaustedCreditReturnsErrNoStreamCredit(t *testing.T) {
	t.Parallel()
	client, server := openPairWithLimits(t, 1)

	// Drain accepts so the first (successful) open's delivery never backs up.
	go func() {
		for {
			if _, err := server.AcceptRequest(t.Context()); err != nil {
				return
			}
		}
	}()

	// First publish consumes the single unit of credit.
	first := &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	}
	s1, err := client.OpenPublish(first)
	if err != nil {
		t.Fatalf("OpenPublish #0: %v", err)
	}
	defer s1.Close()
	firstID := first.RequestID

	// Second publish: credit exhausted → ErrNoStreamCredit, no ID consumed.
	second := &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam2"),
	}
	_, err = client.OpenPublish(second)
	if !errors.Is(err, session.ErrNoStreamCredit) {
		t.Fatalf("OpenPublish #1 err = %v, want ErrNoStreamCredit", err)
	}

	// §6.1: a blocked attempt MUST NOT consume a Request ID. The next
	// successful allocation (via a plain AllocRequestID) must be exactly
	// firstID+2, proving the blocked OpenPublish left the sequence untouched.
	if got := client.AllocRequestID(); got != firstID+2 {
		t.Fatalf("Request ID after blocked OpenPublish = %d, want %d (firstID %d + 2)",
			got, firstID+2, firstID)
	}
}
