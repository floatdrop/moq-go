package session_test

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// runRequestRoundTrip drives a client request and a server response
// concurrently. Synchronous io.Pipe semantics in sessiontest's pipeConn
// require both sides to make progress in parallel — sequential sends would
// deadlock.
func runRequestRoundTrip(
	t *testing.T,
	client, server *session.Session,
	first message.Message,
	serve func(*session.Request) error,
) (gotFirst message.Message, clientResponse message.Message) {
	t.Helper()
	ctx := t.Context()

	var (
		wg                   sync.WaitGroup
		serverErr, clientErr error
	)
	wg.Go(func() {
		req, err := server.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		gotFirst = req.First
		if err := serve(req); err != nil {
			serverErr = err
			return
		}
	})
	wg.Go(func() {
		stream, err := session.OpenRequestForTest(client, first)
		if err != nil {
			clientErr = err
			return
		}
		resp, err := message.Parse(stream)
		if err != nil {
			clientErr = err
			return
		}
		clientResponse = resp
		_ = stream.Close()
	})
	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server: %v", serverErr)
	}
	if clientErr != nil {
		t.Fatalf("client: %v", clientErr)
	}
	return gotFirst, clientResponse
}

func TestRequestSubscribeRoundTrip(t *testing.T) {
	client, server := openPair(t)

	sub := &message.Subscribe{
		RequestID: client.AllocRequestID(),
		Namespace: wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Name:      []byte("catalog"),
		Parameters: message.Parameters{
			message.ForwardParam(true),
		},
	}
	okReply := &message.SubscribeOK{
		TrackAlias: 7,
		Parameters: message.Parameters{
			message.ExpiresParam(30 * time.Second),
		},
	}

	gotFirst, gotResp := runRequestRoundTrip(t, client, server, sub, func(r *session.Request) error {
		if _, ok := r.First.(*message.Subscribe); !ok {
			return fmt.Errorf("server got %T, want *message.Subscribe", r.First)
		}
		if err := r.Reply(okReply); err != nil {
			return err
		}
		return r.Stream.Close()
	})

	gotSub := gotFirst.(*message.Subscribe)
	if gotSub.RequestID != sub.RequestID {
		t.Errorf("RequestID = %d, want %d", gotSub.RequestID, sub.RequestID)
	}
	if !bytes.Equal(gotSub.Name, sub.Name) {
		t.Errorf("Name = %q, want %q", gotSub.Name, sub.Name)
	}
	if len(gotSub.Namespace) != 2 ||
		!bytes.Equal(gotSub.Namespace[0], sub.Namespace[0]) ||
		!bytes.Equal(gotSub.Namespace[1], sub.Namespace[1]) {
		t.Errorf("Namespace = %v, want %v", gotSub.Namespace, sub.Namespace)
	}

	gotOK, ok := gotResp.(*message.SubscribeOK)
	if !ok {
		t.Fatalf("client got %T, want *message.SubscribeOK", gotResp)
	}
	if gotOK.TrackAlias != okReply.TrackAlias {
		t.Errorf("TrackAlias = %d, want %d", gotOK.TrackAlias, okReply.TrackAlias)
	}
}

func TestRequestErrorRejectRoundTrip(t *testing.T) {
	client, server := openPair(t)

	sub := &message.Subscribe{
		RequestID: client.AllocRequestID(),
		Namespace: wire.TrackNamespace{[]byte("missing")},
		Name:      []byte("ghost"),
	}

	_, gotResp := runRequestRoundTrip(t, client, server, sub, func(r *session.Request) error {
		return r.RejectError(moqt.RequestDoesNotExist, "track not advertised")
	})

	gotErr, ok := gotResp.(*message.RequestError)
	if !ok {
		t.Fatalf("client got %T, want *message.RequestError", gotResp)
	}
	if gotErr.ErrorCode != moqt.RequestDoesNotExist {
		t.Errorf("ErrorCode = %#x, want %#x", uint64(gotErr.ErrorCode), uint64(moqt.RequestDoesNotExist))
	}
	if gotErr.ErrorReason != "track not advertised" {
		t.Errorf("ErrorReason = %q, want %q", gotErr.ErrorReason, "track not advertised")
	}
}

func TestAcceptRequestUnblocksOnSessionClose(t *testing.T) {
	ctx := t.Context()
	_, server := openPair(t)

	done := make(chan error, 1)
	go func() {
		_, err := server.AcceptRequest(ctx)
		done <- err
	}()

	// Give the goroutine time to settle into AcceptRequest.
	time.Sleep(50 * time.Millisecond)

	if err := server.Close(moqt.SessionNoError, "shutting down"); err != nil {
		t.Errorf("server Close: %v", err)
	}

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("AcceptRequest returned nil error after Close")
		}
	case <-time.After(time.Second):
		t.Fatal("AcceptRequest did not unblock after server Close")
	}
}

// TestAcceptRequestDuplicateID verifies that when the peer sends a second
// request with the same Request ID, AcceptRequest returns ErrDuplicateRequestID
// per §10.1 ("A Request ID MUST NOT be reused within a session").
func TestAcceptRequestDuplicateID(t *testing.T) {
	ctx := t.Context()
	client, server := openPair(t)

	// First request with ID=0 — valid.
	firstSub := &message.Subscribe{
		RequestID: 0,
		Namespace: wire.TrackNamespace{[]byte("test")},
		Name:      []byte("track"),
	}
	// Second request reusing ID=0 — duplicate.
	dupSub := &message.Subscribe{
		RequestID: 0,
		Namespace: wire.TrackNamespace{[]byte("test")},
		Name:      []byte("track2"),
	}

	var (
		wg        sync.WaitGroup
		firstErr  error
		secondErr error
	)

	// Server: accept two requests sequentially.
	wg.Go(func() {
		req, err := server.AcceptRequest(ctx)
		firstErr = err
		if err != nil {
			return
		}
		// Drain the first request so the client goroutine can proceed.
		_ = req.RejectError(moqt.RequestDoesNotExist, "ok")
		_, secondErr = server.AcceptRequest(ctx)
	})

	// Client: open two streams with the same Request ID.
	wg.Go(func() {
		s1, err := session.OpenRequestForTest(client, firstSub)
		if err != nil {
			return
		}
		// Read the server's response so the first round-trip completes.
		_, _ = message.Parse(s1)
		_ = s1.Close()

		s2, err := session.OpenRequestForTest(client, dupSub)
		if err != nil {
			return
		}
		_ = s2.Close()
	})

	wg.Wait()

	if firstErr != nil {
		t.Fatalf("first AcceptRequest: %v", firstErr)
	}

	var dupErr *session.ErrDuplicateRequestID
	if !errors.As(secondErr, &dupErr) {
		t.Fatalf("second AcceptRequest error = %v (%T), want *session.ErrDuplicateRequestID", secondErr, secondErr)
	}
	if dupErr.RequestID != 0 {
		t.Errorf("ErrDuplicateRequestID.RequestID = %d, want 0", dupErr.RequestID)
	}
}

// TestAcceptRequestOutOfOrderID verifies §10.1's receiver rules under
// cross-stream delivery reordering: an ID below the high-water mark is NOT a
// violation the first time it arrives — the peer allocates in +2 increments,
// but requests ride separate QUIC streams and can be delivered out of order —
// while claiming the same ID a second time is a fatal duplicate.
func TestAcceptRequestOutOfOrderID(t *testing.T) {
	ctx := t.Context()
	client, server := openPair(t)

	// First request with ID=4 (skipping ahead).
	firstSub := &message.Subscribe{
		RequestID: 4,
		Namespace: wire.TrackNamespace{[]byte("test")},
		Name:      []byte("track"),
	}
	// Second request with ID=2 — delivered out of order (less than 4).
	oooSub := &message.Subscribe{
		RequestID: 2,
		Namespace: wire.TrackNamespace{[]byte("test")},
		Name:      []byte("track2"),
	}
	// Third request reusing ID=2 — a true duplicate.
	dupSub := &message.Subscribe{
		RequestID: 2,
		Namespace: wire.TrackNamespace{[]byte("test")},
		Name:      []byte("track3"),
	}

	var (
		wg        sync.WaitGroup
		firstErr  error
		secondErr error
	)

	var thirdErr error
	wg.Go(func() {
		req, err := server.AcceptRequest(ctx)
		firstErr = err
		if err != nil {
			return
		}
		_ = req.RejectError(moqt.RequestDoesNotExist, "ok")
		req, secondErr = server.AcceptRequest(ctx)
		if secondErr != nil {
			return
		}
		_ = req.RejectError(moqt.RequestDoesNotExist, "ok")
		_, thirdErr = server.AcceptRequest(ctx)
	})

	wg.Go(func() {
		s1, err := session.OpenRequestForTest(client, firstSub)
		if err != nil {
			return
		}
		_, _ = message.Parse(s1)
		_ = s1.Close()

		s2, err := session.OpenRequestForTest(client, oooSub)
		if err != nil {
			return
		}
		_, _ = message.Parse(s2)
		_ = s2.Close()

		s3, err := session.OpenRequestForTest(client, dupSub)
		if err != nil {
			return
		}
		_ = s3.Close()
	})

	wg.Wait()

	if firstErr != nil {
		t.Fatalf("first AcceptRequest: %v", firstErr)
	}
	if secondErr != nil {
		t.Fatalf("reordered AcceptRequest: %v (out-of-order delivery must be tolerated)", secondErr)
	}

	var dupErr *session.ErrDuplicateRequestID
	if !errors.As(thirdErr, &dupErr) {
		t.Fatalf("third AcceptRequest error = %v (%T), want *session.ErrDuplicateRequestID", thirdErr, thirdErr)
	}
	if dupErr.RequestID != 2 {
		t.Errorf("ErrDuplicateRequestID.RequestID = %d, want 2", dupErr.RequestID)
	}
	if dupErr.MaxSeen != 4 {
		t.Errorf("ErrDuplicateRequestID.MaxSeen = %d, want 4", dupErr.MaxSeen)
	}
}

// TestAcceptRequestMonotonicHappyPath verifies that multiple requests with
// strictly increasing Request IDs are all accepted without error.
func TestAcceptRequestMonotonicHappyPath(t *testing.T) {
	ctx := t.Context()
	client, server := openPair(t)

	ids := []uint64{0, 2, 4}

	var (
		wg        sync.WaitGroup
		serverErr error
	)

	wg.Go(func() {
		for range ids {
			req, err := server.AcceptRequest(ctx)
			if err != nil {
				serverErr = err
				return
			}
			_ = req.RejectError(moqt.RequestDoesNotExist, "ok")
		}
	})

	wg.Go(func() {
		for _, id := range ids {
			sub := &message.Subscribe{
				RequestID: id,
				Namespace: wire.TrackNamespace{[]byte("test")},
				Name:      []byte("track"),
			}
			s, err := session.OpenRequestForTest(client, sub)
			if err != nil {
				return
			}
			_, _ = message.Parse(s)
			_ = s.Close()
		}
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server AcceptRequest: %v", serverErr)
	}
}

// TestAcceptRequestParityViolation_ServerReceivesOddID verifies that when the
// server calls AcceptRequest and the client sends a SUBSCRIBE with an odd
// Request ID (which only servers may generate per §10.1), AcceptRequest
// returns ErrRequestIDParityViolation with ExpectedEven=true.
func TestAcceptRequestParityViolation_ServerReceivesOddID(t *testing.T) {
	client, server := openPair(t)
	ctx := t.Context()

	// Odd ID — only valid for server-originated requests; client must use even.
	badID := uint64(1)

	var (
		wg        sync.WaitGroup
		acceptErr error
	)
	wg.Go(func() {
		_, acceptErr = server.AcceptRequest(ctx)
	})
	wg.Go(func() {
		// Bypass AllocRequestID and inject a wrong-parity ID directly.
		sub := &message.Subscribe{
			RequestID: badID,
			Namespace: wire.TrackNamespace{[]byte("test")},
			Name:      []byte("track"),
		}
		// OpenRequest will succeed (the client side doesn't validate parity of
		// outgoing IDs); the server's AcceptRequest will reject it.
		stream, err := session.OpenRequestForTest(client, sub)
		if err != nil {
			return // server closed the conn; that's fine
		}
		_ = stream.Close()
	})
	wg.Wait()

	var parityErr *session.ErrRequestIDParityViolation
	if !errors.As(acceptErr, &parityErr) {
		t.Fatalf(
			"server AcceptRequest error = %v (%T), want *session.ErrRequestIDParityViolation",
			acceptErr,
			acceptErr,
		)
	}
	if parityErr.RequestID != badID {
		t.Errorf("ErrRequestIDParityViolation.RequestID = %d, want %d", parityErr.RequestID, badID)
	}
	if !parityErr.ExpectedEven {
		t.Errorf("ErrRequestIDParityViolation.ExpectedEven = false, want true (server expects even IDs from client)")
	}
}

// TestAcceptRequestParityViolation_ClientReceivesEvenID verifies that when the
// client calls AcceptRequest and the server sends a PUBLISH with an even
// Request ID (which only clients may generate per §10.1), AcceptRequest
// returns ErrRequestIDParityViolation with ExpectedEven=false.
func TestAcceptRequestParityViolation_ClientReceivesEvenID(t *testing.T) {
	client, server := openPair(t)
	ctx := t.Context()

	// Even ID — only valid for client-originated requests; server must use odd.
	badID := uint64(2)

	var (
		wg        sync.WaitGroup
		acceptErr error
	)
	wg.Go(func() {
		_, acceptErr = client.AcceptRequest(ctx)
	})
	wg.Go(func() {
		pub := &message.Publish{
			RequestID: badID,
			Namespace: wire.TrackNamespace{[]byte("test")},
			Name:      []byte("track"),
		}
		stream, err := session.OpenRequestForTest(server, pub)
		if err != nil {
			return // client closed the conn; that's fine
		}
		_ = stream.Close()
	})
	wg.Wait()

	var parityErr *session.ErrRequestIDParityViolation
	if !errors.As(acceptErr, &parityErr) {
		t.Fatalf(
			"client AcceptRequest error = %v (%T), want *session.ErrRequestIDParityViolation",
			acceptErr,
			acceptErr,
		)
	}
	if parityErr.RequestID != badID {
		t.Errorf("ErrRequestIDParityViolation.RequestID = %d, want %d", parityErr.RequestID, badID)
	}
	if parityErr.ExpectedEven {
		t.Errorf("ErrRequestIDParityViolation.ExpectedEven = true, want false (client expects odd IDs from server)")
	}
}

// TestAcceptRequestParityHappyPath verifies that correct-parity IDs are
// accepted without error: client sends even (0) → server accepts; server
// sends odd (1) → client accepts.
func TestAcceptRequestParityHappyPath(t *testing.T) {
	t.Run("server receives even ID from client", func(t *testing.T) {
		client, server := openPair(t)
		sub := &message.Subscribe{
			RequestID: 0, // even — correct for client
			Namespace: wire.TrackNamespace{[]byte("test")},
			Name:      []byte("track"),
		}
		gotFirst, _ := runRequestRoundTrip(t, client, server, sub, func(r *session.Request) error {
			return r.RejectError(moqt.RequestDoesNotExist, "ok-parity test")
		})
		if gotFirst == nil {
			t.Fatal("server got nil first message")
		}
		got, ok := gotFirst.(*message.Subscribe)
		if !ok {
			t.Fatalf("server got %T, want *message.Subscribe", gotFirst)
		}
		if got.RequestID != 0 {
			t.Errorf("RequestID = %d, want 0", got.RequestID)
		}
	})

	t.Run("client receives odd ID from server", func(t *testing.T) {
		client, server := openPair(t)
		pub := &message.Publish{
			RequestID: 1, // odd — correct for server
			Namespace: wire.TrackNamespace{[]byte("test")},
			Name:      []byte("track"),
		}
		// server opens, client accepts
		var (
			wg        sync.WaitGroup
			acceptErr error
			gotFirst  message.Message
		)
		wg.Go(func() {
			req, err := client.AcceptRequest(t.Context())
			if err != nil {
				acceptErr = err
				return
			}
			gotFirst = req.First
			_ = req.RejectError(moqt.RequestDoesNotExist, "ok-parity test")
		})
		wg.Go(func() {
			stream, err := session.OpenRequestForTest(server, pub)
			if err != nil {
				return
			}
			_, _ = message.Parse(stream)
			_ = stream.Close()
		})
		wg.Wait()

		if acceptErr != nil {
			t.Fatalf("client AcceptRequest: %v", acceptErr)
		}
		got, ok := gotFirst.(*message.Publish)
		if !ok {
			t.Fatalf("client got %T, want *message.Publish", gotFirst)
		}
		if got.RequestID != 1 {
			t.Errorf("RequestID = %d, want 1", got.RequestID)
		}
	})
}

// TestRejectErrorCancelsReadSide verifies that RejectError writes
// REQUEST_ERROR, cancels the read side (CancelRead), and FINs the send
// direction. After rejection, the client's writes to the stream must fail
// because the server stopped reading (§3.3.2).
func TestRejectErrorCancelsReadSide(t *testing.T) {
	client, server := openPair(t)
	ctx := t.Context()

	sub := &message.Subscribe{
		RequestID: client.AllocRequestID(),
		Namespace: wire.TrackNamespace{[]byte("example.com")},
		Name:      []byte("track"),
	}

	type clientResult struct {
		resp     message.Message
		writeErr error
	}

	var (
		wg        sync.WaitGroup
		serverErr error
		cResult   clientResult
	)

	// Server: accept the request and reject it.
	wg.Go(func() {
		req, err := server.AcceptRequest(ctx)
		if err != nil {
			serverErr = err
			return
		}
		serverErr = req.RejectError(moqt.RequestDoesNotExist, "track not found")
	})

	// Client: open the request, read the rejection, then try to write.
	wg.Go(func() {
		stream, err := session.OpenRequestForTest(client, sub)
		if err != nil {
			cResult.writeErr = err
			return
		}
		resp, err := message.Parse(stream)
		if err != nil {
			cResult.writeErr = err
			return
		}
		cResult.resp = resp

		// The server has called CancelRead, so writing should eventually fail.
		// Give a small delay for the CancelRead to propagate through the pipe.
		time.Sleep(10 * time.Millisecond)

		// Try to marshal another message — this should fail because the
		// server cancelled the read side.
		update := &message.Subscribe{
			RequestID: sub.RequestID,
			Namespace: sub.Namespace,
			Name:      sub.Name,
		}
		cResult.writeErr = message.Marshal(stream, update)
		_ = stream.Close()
	})

	wg.Wait()

	if serverErr != nil {
		t.Fatalf("server RejectError: %v", serverErr)
	}

	// Verify the client received the REQUEST_ERROR.
	gotErr, ok := cResult.resp.(*message.RequestError)
	if !ok {
		t.Fatalf("client got %T, want *message.RequestError", cResult.resp)
	}
	if gotErr.ErrorCode != moqt.RequestDoesNotExist {
		t.Errorf("ErrorCode = %#x, want %#x", uint64(gotErr.ErrorCode), uint64(moqt.RequestDoesNotExist))
	}

	// Verify the client's write failed because the server cancelled the read side.
	if cResult.writeErr == nil {
		t.Error("client Write after RejectError should fail (server cancelled read side), but got nil")
	}
}
