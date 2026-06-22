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

// ---------------------------------------------------------------------------
// Publish
// ---------------------------------------------------------------------------

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
			sg, err := pub.OpenSubgroup(ctx, message.SubgroupHeader{
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

// ---------------------------------------------------------------------------
// Subscribe
// ---------------------------------------------------------------------------

// TestSubscribeRoundTrip exercises the full SUBSCRIBE flow:
//
//  1. Client calls Session.Subscribe → sends SUBSCRIBE on a bidi stream.
//  2. Server accepts the request, verifies the first message, replies SUBSCRIBE_OK.
//  3. Client receives the open stream and the parsed SUBSCRIBE_OK.
func TestSubscribeRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	ns := wire.TrackNamespace{[]byte("example.com"), []byte("live")}
	req := &message.Subscribe{
		Namespace: ns,
		Name:      []byte("video"),
	}

	wantOK := &message.SubscribeOK{
		TrackAlias: 7,
	}

	var (
		wg        sync.WaitGroup
		serverErr error
		clientErr error
		gotOK     *message.SubscribeOK
	)

	// Server: accept SUBSCRIBE, verify fields, reply SUBSCRIBE_OK.
	wg.Go(func() {
		r, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		sub, ok := r.First.(*message.Subscribe)
		if !ok {
			serverErr = errors.New("server: expected *message.Subscribe, got " + r.First.Type().String())
			return
		}
		if sub.RequestID != 0 {
			serverErr = errors.New("server: unexpected RequestID")
			return
		}
		if string(sub.Name) != string(req.Name) {
			serverErr = errors.New("server: Name mismatch")
			return
		}
		serverErr = r.Reply(wantOK)
	})

	// Client: call Subscribe, check result.
	wg.Go(func() {
		stream, err := cli.Subscribe(ctx, req)
		if err != nil {
			clientErr = err
			return
		}
		defer stream.Close()
		gotOK = stream.OK
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client Subscribe: %v", clientErr)
	}
	if gotOK == nil {
		t.Fatal("gotOK is nil")
	}
	if gotOK.TrackAlias != wantOK.TrackAlias {
		t.Errorf("SubscribeOK.TrackAlias = %d, want %d", gotOK.TrackAlias, wantOK.TrackAlias)
	}
}

// TestSubscribeRejected verifies that Session.Subscribe returns a
// *RequestRejectedError when the server replies with REQUEST_ERROR.
func TestSubscribeRejected(t *testing.T) {
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
		_, err := cli.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.TrackNamespace{[]byte("ns")},
			Name:      []byte("missing"),
		})
		var rejected *session.RequestRejectedError
		if !errors.As(err, &rejected) {
			t.Errorf("Subscribe error = %v (%T), want *session.RequestRejectedError", err, err)
			return
		}
		if rejected.Code != moqt.RequestDoesNotExist {
			t.Errorf("Code = %v, want RequestDoesNotExist", rejected.Code)
		}
	})

	wg.Wait()
}

// TestSubscribeRequestIDIncrement verifies that successive Subscribe calls
// allocate monotonically increasing even Request IDs on the client side.
func TestSubscribeRequestIDIncrement(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var (
		wg      sync.WaitGroup
		ids     [2]uint64
		srvErrs [2]error
	)

	// Server goroutine: accept two requests and record their IDs.
	wg.Go(func() {
		for i := range 2 {
			r, err := srv.AcceptRequest(ctx)
			if err != nil {
				srvErrs[i] = err
				return
			}
			sub, ok := r.First.(*message.Subscribe)
			if !ok {
				srvErrs[i] = errors.New("expected *message.Subscribe")
				return
			}
			ids[i] = sub.RequestID
			_ = r.Reply(&message.SubscribeOK{TrackAlias: uint64(i)})
		}
	})

	// Client: two sequential Subscribe calls.
	wg.Go(func() {
		for range 2 {
			stream, err := cli.Subscribe(ctx, &message.Subscribe{
				Namespace: wire.TrackNamespace{[]byte("ns")},
				Name:      []byte("track"),
			})
			if err != nil {
				return
			}
			_ = stream.Close()
		}
	})

	wg.Wait()

	for i, err := range srvErrs {
		if err != nil {
			t.Fatalf("server request %d: %v", i, err)
		}
	}
	if ids[0] != 0 {
		t.Errorf("first RequestID = %d, want 0", ids[0])
	}
	if ids[1] != 2 {
		t.Errorf("second RequestID = %d, want 2", ids[1])
	}
}

// ---------------------------------------------------------------------------
// Duplicate Track Alias detection (§11.1)
// ---------------------------------------------------------------------------

// TestRegisterInboundTrackAlias verifies the Session.RegisterInboundTrackAlias
// method directly: same alias + same track is idempotent; same alias +
// different track returns *ErrDuplicateTrackAlias.
func TestRegisterInboundTrackAlias(t *testing.T) {
	cli, _ := openPair(t)

	keyA := track.NewKey(wire.TrackNamespace{[]byte("ns")}, []byte("trackA"))
	keyB := track.NewKey(wire.TrackNamespace{[]byte("ns")}, []byte("trackB"))

	// First registration succeeds.
	if err := cli.RegisterInboundTrackAlias(42, keyA); err != nil {
		t.Fatalf("first register: %v", err)
	}

	// Idempotent re-registration with the same key succeeds.
	if err := cli.RegisterInboundTrackAlias(42, keyA); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}

	// Same alias, different track → ErrDuplicateTrackAlias.
	err := cli.RegisterInboundTrackAlias(42, keyB)
	var dupErr *session.ErrDuplicateTrackAlias
	if !errors.As(err, &dupErr) {
		t.Fatalf("register different track: error = %v (%T), want *session.ErrDuplicateTrackAlias", err, err)
	}
	if dupErr.Alias != 42 {
		t.Errorf("Alias = %d, want 42", dupErr.Alias)
	}

	// Different alias, same track → fine (multiple aliases can point to the same track).
	if err := cli.RegisterInboundTrackAlias(99, keyA); err != nil {
		t.Fatalf("different alias same track: %v", err)
	}

	// Unregister alias 42, then re-register with a different track → succeeds.
	cli.UnregisterInboundTrackAlias(42)
	if err := cli.RegisterInboundTrackAlias(42, keyB); err != nil {
		t.Fatalf("register after unregister: %v", err)
	}

	// Unregistering a non-existent alias is a no-op.
	cli.UnregisterInboundTrackAlias(12345)
}

// TestSubscribeDuplicateTrackAlias verifies that when the server assigns the
// same Track Alias to two different tracks via SUBSCRIBE_OK, the second
// Subscribe call returns *ErrDuplicateTrackAlias.
func TestSubscribeDuplicateTrackAlias(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	const sharedAlias = uint64(7)

	// Helper: run one subscribe round-trip.
	doSubscribe := func(ns wire.TrackNamespace, name []byte) error {
		var (
			wg     sync.WaitGroup
			srvErr error
			cliErr error
		)
		wg.Go(func() {
			r, err := srv.AcceptRequest(ctx)
			if err != nil {
				srvErr = err
				return
			}
			srvErr = r.Reply(&message.SubscribeOK{TrackAlias: sharedAlias})
		})
		wg.Go(func() {
			stream, err := cli.Subscribe(ctx, &message.Subscribe{
				Namespace: ns,
				Name:      name,
			})
			if err != nil {
				cliErr = err
				return
			}
			_ = stream.Close()
		})
		wg.Wait()
		if srvErr != nil {
			t.Fatalf("server: %v", srvErr)
		}
		return cliErr
	}

	// First subscribe: alias 7 → (ns, "trackA"). Should succeed.
	if err := doSubscribe(wire.TrackNamespace{[]byte("ns")}, []byte("trackA")); err != nil {
		t.Fatalf("first Subscribe: %v", err)
	}

	// Second subscribe: alias 7 → (ns, "trackB"). Should fail with ErrDuplicateTrackAlias.
	err := doSubscribe(wire.TrackNamespace{[]byte("ns")}, []byte("trackB"))
	var dupErr *session.ErrDuplicateTrackAlias
	if !errors.As(err, &dupErr) {
		t.Fatalf("second Subscribe: error = %v (%T), want *session.ErrDuplicateTrackAlias", err, err)
	}
	if dupErr.Alias != sharedAlias {
		t.Errorf("Alias = %d, want %d", dupErr.Alias, sharedAlias)
	}
}

// TestSubscribeSameTrackAliasIdempotent verifies that subscribing to the same
// track twice with the same alias succeeds (idempotent registration).
func TestSubscribeSameTrackAliasIdempotent(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	const alias = uint64(5)
	ns := wire.TrackNamespace{[]byte("ns")}
	name := []byte("track")

	for i := range 2 {
		var (
			wg     sync.WaitGroup
			srvErr error
			cliErr error
		)
		wg.Go(func() {
			r, err := srv.AcceptRequest(ctx)
			if err != nil {
				srvErr = err
				return
			}
			srvErr = r.Reply(&message.SubscribeOK{TrackAlias: alias})
		})
		wg.Go(func() {
			stream, err := cli.Subscribe(ctx, &message.Subscribe{
				Namespace: ns,
				Name:      name,
			})
			if err != nil {
				cliErr = err
				return
			}
			_ = stream.Close()
		})
		wg.Wait()
		if srvErr != nil {
			t.Fatalf("iteration %d server: %v", i, srvErr)
		}
		if cliErr != nil {
			t.Fatalf("iteration %d client Subscribe: %v", i, cliErr)
		}
	}
}

// ---------------------------------------------------------------------------
// Accept-side handles: Request.AcceptSubscribe / Request.AcceptPublish
// ---------------------------------------------------------------------------

// TestRequestAcceptSubscribe exercises the accept side end to end: the server
// answers an inbound SUBSCRIBE with AcceptSubscribe, gets a Publication, and
// pushes an object on it; the subscriber reads it back and the auto-assigned
// Track Alias is consistent across SUBSCRIBE_OK, the Publication, and the
// inbound subgroup header.
func TestRequestAcceptSubscribe(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	var (
		wg          sync.WaitGroup
		serverErr   error
		serverAlias uint64
	)
	wg.Go(func() {
		req, err := srv.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		pub, err := req.AcceptSubscribe(nil)
		if err != nil {
			serverErr = err
			return
		}
		serverAlias = pub.TrackAlias()
		sg, err := pub.OpenSubgroup(ctx, message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDImplicitZero,
			GroupID:        3,
		})
		if err != nil {
			serverErr = err
			return
		}
		if err := sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("hi")}); err != nil {
			serverErr = err
			return
		}
		serverErr = sg.Close()
	})

	var (
		clientErr   error
		gotSubAlias uint64
		gotHdrAlias uint64
		gotPayload  string
		gotGroup    uint64
	)
	wg.Go(func() {
		sub, err := cli.Subscribe(ctx, &message.Subscribe{
			Namespace: wire.Namespace("ns"),
			Name:      []byte("track"),
		})
		if err != nil {
			clientErr = err
			return
		}
		gotSubAlias = sub.TrackAlias()
		ds, err := cli.AcceptDataStream(ctx)
		if err != nil {
			clientErr = err
			return
		}
		in, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			clientErr = fmt.Errorf("got %T, want *IncomingSubgroupStream", ds)
			return
		}
		gotHdrAlias = in.Header.TrackAlias
		obj, err := in.ReadDecoded()
		if err != nil {
			clientErr = err
			return
		}
		gotPayload = string(obj.Payload)
		gotGroup = obj.GroupID
		_ = sub.Close()
	})
	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client: %v", clientErr)
	}
	if gotSubAlias != serverAlias || gotHdrAlias != serverAlias {
		t.Errorf("alias mismatch: subscribe=%d header=%d server=%d", gotSubAlias, gotHdrAlias, serverAlias)
	}
	if gotPayload != "hi" {
		t.Errorf("payload = %q, want %q", gotPayload, "hi")
	}
	if gotGroup != 3 {
		t.Errorf("GroupID = %d, want 3", gotGroup)
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
		if err := req.AcceptPublish(); err != nil {
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

// TestAcceptWrongType verifies the accept helpers reject a mismatched request
// type without writing a response.
func TestAcceptWrongType(t *testing.T) {
	t.Run("AcceptPublish on SUBSCRIBE", func(t *testing.T) {
		client, server := openPair(t)
		sub := &message.Subscribe{
			RequestID: client.AllocRequestID(),
			Namespace: wire.Namespace("ns"),
			Name:      []byte("t"),
		}
		_, gotResp := runRequestRoundTrip(t, client, server, sub, func(r *session.Request) error {
			if err := r.AcceptPublish(); err == nil {
				t.Error("AcceptPublish on a SUBSCRIBE request: want error, got nil")
			}
			return r.RejectError(moqt.RequestDoesNotExist, "wrong type")
		})
		if _, ok := gotResp.(*message.RequestError); !ok {
			t.Errorf("client got %T, want *message.RequestError", gotResp)
		}
	})

	t.Run("AcceptSubscribe on PUBLISH", func(t *testing.T) {
		client, server := openPair(t)
		pub := &message.Publish{
			RequestID:  client.AllocRequestID(),
			Namespace:  wire.Namespace("ns"),
			Name:       []byte("t"),
			TrackAlias: 1,
		}
		_, gotResp := runRequestRoundTrip(t, client, server, pub, func(r *session.Request) error {
			if _, err := r.AcceptSubscribe(nil); err == nil {
				t.Error("AcceptSubscribe on a PUBLISH request: want error, got nil")
			}
			return r.RejectError(moqt.RequestDoesNotExist, "wrong type")
		})
		if _, ok := gotResp.(*message.RequestError); !ok {
			t.Errorf("client got %T, want *message.RequestError", gotResp)
		}
	})
}

// TestIncomingSubgroupStreamTrackKey verifies AcceptDataStream resolves a
// subgroup's Track Alias to the subscribed track via the inbound alias
// registry, and reports ok=false for an alias that was never registered.
func TestIncomingSubgroupStreamTrackKey(t *testing.T) {
	t.Run("resolved", func(t *testing.T) {
		cli, srv := openPair(t)
		ctx := t.Context()

		var (
			wg        sync.WaitGroup
			serverErr error
		)
		wg.Go(func() {
			req, err := srv.AcceptRequest(ctx)
			if err != nil {
				serverErr = err
				return
			}
			pub, err := req.AcceptSubscribe(nil)
			if err != nil {
				serverErr = err
				return
			}
			sg, err := pub.OpenSubgroup(ctx, message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDImplicitZero,
				GroupID:        0,
			})
			if err != nil {
				serverErr = err
				return
			}
			serverErr = sg.Close()
		})

		var (
			clientErr error
			gotKey    track.Key
			gotOK     bool
		)
		wg.Go(func() {
			sub, err := cli.Subscribe(ctx, &message.Subscribe{
				Namespace: wire.Namespace("room", "a"),
				Name:      []byte("video"),
			})
			if err != nil {
				clientErr = err
				return
			}
			defer sub.Close()
			ds, err := cli.AcceptDataStream(ctx)
			if err != nil {
				clientErr = err
				return
			}
			in, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				clientErr = fmt.Errorf("got %T, want *IncomingSubgroupStream", ds)
				return
			}
			gotKey, gotOK = in.TrackKey()
		})
		wg.Wait()

		if serverErr != nil {
			t.Fatalf("server: %v", serverErr)
		}
		if clientErr != nil {
			t.Fatalf("client: %v", clientErr)
		}
		if !gotOK {
			t.Fatal("TrackKey not resolved, want ok=true")
		}
		if want := track.NewKey(wire.Namespace("room", "a"), []byte("video")); gotKey != want {
			t.Errorf("TrackKey = %v, want %v", gotKey, want)
		}
	})

	t.Run("unregistered alias", func(t *testing.T) {
		cli, srv := openPair(t)
		ctx := t.Context()

		var wg sync.WaitGroup
		wg.Go(func() {
			// Push a subgroup on an alias the client never subscribed to.
			sg, err := srv.OpenSubgroup(ctx, message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDImplicitZero,
				TrackAlias:     99,
				GroupID:        0,
			})
			if err != nil {
				return
			}
			_ = sg.Close()
		})

		ds, err := cli.AcceptDataStream(ctx)
		if err != nil {
			t.Fatalf("AcceptDataStream: %v", err)
		}
		in, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			t.Fatalf("got %T, want *IncomingSubgroupStream", ds)
		}
		if _, resolved := in.TrackKey(); resolved {
			t.Error("TrackKey resolved an unregistered alias, want ok=false")
		}
		wg.Wait()
	})
}
