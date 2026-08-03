package session

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// brokerStubStream is a minimal Stream: writes vanish, reads never happen
// (these tests exercise the waiter queue, not the Serve loop).
type brokerStubStream struct{}

func (brokerStubStream) Write(p []byte) (int, error) { return len(p), nil }
func (brokerStubStream) Close() error                { return nil }
func (brokerStubStream) CancelWrite(uint64)          {}
func (brokerStubStream) Read([]byte) (int, error)    { return 0, nil }
func (brokerStubStream) CancelRead(uint64)           {}
func (brokerStubStream) Context() context.Context    { return context.Background() }

// routeEventually retries route until a waiter consumes msg, bridging the
// gap between an Update goroutine starting and its waiter being queued.
func routeEventually(t *testing.T, b *RequestBroker, msg message.Message) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !b.route(msg) {
		if time.Now().After(deadline) {
			t.Fatal("no waiter consumed the response within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func startUpdate(b *RequestBroker) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		_, err := b.Update(context.Background(), nil)
		errCh <- err
	}()
	return errCh
}

func newTestBroker() *RequestBroker {
	// A zero-value Session suffices: Update only needs AllocRequestID.
	return (&Session{}).NewRequestBroker(brokerStubStream{})
}

// TestBrokerUpdate_OKAnswersOldest pins the happy path: one queued Update,
// one REQUEST_OK routed to it.
func TestBrokerUpdate_OKAnswersOldest(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	errCh := startUpdate(b)
	routeEventually(t, b, &message.RequestOK{})
	if err := <-errCh; err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestBrokerUpdate_ErrorFailsAllPending pins the §10.9 coalescing rule: a
// peer may answer N pipelined REQUEST_UPDATEs with a single REQUEST_ERROR,
// so one error must fail every in-flight Update, not just the oldest.
func TestBrokerUpdate_ErrorFailsAllPending(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	first := startUpdate(b)
	second := startUpdate(b)

	// Both waiters must be queued before the coalesced error is routed —
	// otherwise it fails only the first and the late second waits forever.
	deadline := time.Now().Add(2 * time.Second)
	for {
		b.mu.Lock()
		queued := len(b.waiters)
		b.mu.Unlock()
		if queued == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/2 waiters queued within 2s", queued)
		}
		time.Sleep(time.Millisecond)
	}

	routeEventually(t, b, &message.RequestError{
		ErrorCode:   moqt.RequestInternalError,
		ErrorReason: "coalesced",
	})

	for i, ch := range []<-chan error{first, second} {
		select {
		case err := <-ch:
			if _, ok := errors.AsType[*RequestRejectedError](err); !ok {
				t.Fatalf("Update #%d: got %v, want RequestRejectedError", i+1, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Update #%d still pending after coalesced REQUEST_ERROR", i+1)
		}
	}
}

// TestBrokerUpdate_CloseFailsPendingAndFuture pins Close: a pending Update
// fails with ErrRequestStreamClosed and later calls fail fast.
func TestBrokerUpdate_CloseFailsPendingAndFuture(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	pending := startUpdate(b)
	// Wait until the waiter is queued: Close before queueing would still
	// fail the call (updatesClosed check), but we want the drain path.
	time.Sleep(10 * time.Millisecond)
	b.Close(moqt.StreamResetCancelled)

	if err := <-pending; !errors.Is(err, ErrRequestStreamClosed) {
		t.Fatalf("pending Update: got %v, want ErrRequestStreamClosed", err)
	}
	if _, err := b.Update(context.Background(), nil); !errors.Is(err, ErrRequestStreamClosed) {
		t.Fatalf("Update after close: got %v, want ErrRequestStreamClosed", err)
	}
}

// TestBrokerUpdate_TimeoutRemovesWaiter pins the anti-poisoning rule: a
// ctx-timed-out Update removes its waiter from the queue, so a peer that
// never answers cannot permanently shift routing — the next response goes
// to the next live waiter, not to the abandoned one.
func TestBrokerUpdate_TimeoutRemovesWaiter(t *testing.T) {
	t.Parallel()
	b := newTestBroker()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Update(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Update: got %v, want DeadlineExceeded", err)
	}

	// The abandoned waiter must be gone: with no live waiter, a response is
	// reported as unsolicited (false), not swallowed by the stale channel.
	if b.route(&message.RequestOK{}) {
		t.Fatal("stale waiter consumed the response after its Update timed out")
	}

	// And a fresh Update still pairs with the next response.
	errCh := startUpdate(b)
	routeEventually(t, b, &message.RequestOK{})
	if err := <-errCh; err != nil {
		t.Fatalf("Update after timeout: %v", err)
	}
}

// recordingBrokerStream captures writes so a test can decode what went out.
type recordingBrokerStream struct {
	mu  sync.Mutex
	buf []byte
}

func (s *recordingBrokerStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	s.buf = append(s.buf, p...)
	s.mu.Unlock()
	return len(p), nil
}
func (s *recordingBrokerStream) Close() error             { return nil }
func (s *recordingBrokerStream) CancelWrite(uint64)       {}
func (s *recordingBrokerStream) Read([]byte) (int, error) { return 0, nil }
func (s *recordingBrokerStream) CancelRead(uint64)        {}
func (s *recordingBrokerStream) Context() context.Context { return context.Background() }

// TestBrokerUpdate_ConsumesFreshRequestIDs pins §10.1: REQUEST_UPDATE is a
// request message that consumes a Request ID, so every update the broker
// sends must carry a fresh, strictly increasing ID from this endpoint's
// space — reusing the original request's ID is a duplicate the peer must
// treat as session-fatal (INVALID_REQUEST_ID).
func TestBrokerUpdate_ConsumesFreshRequestIDs(t *testing.T) {
	t.Parallel()
	stream := &recordingBrokerStream{}
	b := (&Session{}).NewRequestBroker(stream)

	for range 3 {
		errCh := startUpdate(b)
		routeEventually(t, b, &message.RequestOK{})
		if err := <-errCh; err != nil {
			t.Fatalf("Update: %v", err)
		}
	}

	stream.mu.Lock()
	buf := append([]byte(nil), stream.buf...)
	stream.mu.Unlock()
	var ids []uint64
	rd := bytes.NewReader(buf)
	for rd.Len() > 0 {
		m, err := message.Parse(rd)
		if err != nil {
			t.Fatalf("decoding written messages: %v", err)
		}
		upd, ok := m.(*message.RequestUpdate)
		if !ok {
			t.Fatalf("wrote %T, want *message.RequestUpdate", m)
		}
		ids = append(ids, upd.RequestID)
	}
	if len(ids) != 3 {
		t.Fatalf("wrote %d updates, want 3", len(ids))
	}
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Fatalf("update Request IDs not strictly increasing: %v", ids)
		}
	}
}
