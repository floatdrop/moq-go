package registry_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// stubStream is a minimal session.Stream: writes vanish, reads never
// happen (the broker's reader lives outside the registry).
type stubStream struct{}

func (stubStream) Write(p []byte) (int, error) { return len(p), nil }
func (stubStream) Close() error                { return nil }
func (stubStream) CancelWrite(uint64)          {}
func (stubStream) Read([]byte) (int, error)    { return 0, nil }
func (stubStream) CancelRead(uint64)           {}
func (stubStream) Context() context.Context    { return context.Background() }

// routeEventually retries RouteUpdateResponse until a waiter consumes msg,
// bridging the gap between an Update goroutine starting and its waiter
// being queued.
func routeEventually(t *testing.T, sub *registry.UpstreamSub, msg message.Message) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !sub.RouteUpdateResponse(msg) {
		if time.Now().After(deadline) {
			t.Fatal("no waiter consumed the response within 2s")
		}
		time.Sleep(time.Millisecond)
	}
}

func startUpdate(sub *registry.UpstreamSub) <-chan error {
	errCh := make(chan error, 1)
	go func() {
		_, err := sub.Update(context.Background(), nil)
		errCh <- err
	}()
	return errCh
}

// TestUpstreamSubUpdate_OKAnswersOldest pins the happy path: one queued
// Update, one REQUEST_OK routed to it.
func TestUpstreamSubUpdate_OKAnswersOldest(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, stubStream{}, 0, 7)

	errCh := startUpdate(sub)
	routeEventually(t, sub, &message.RequestOK{})
	if err := <-errCh; err != nil {
		t.Fatalf("Update: %v", err)
	}
}

// TestUpstreamSubUpdate_ErrorFailsAllPending pins the §10.9 coalescing rule:
// a peer may answer N pipelined REQUEST_UPDATEs with a single REQUEST_ERROR,
// so one error must fail every in-flight Update, not just the oldest.
func TestUpstreamSubUpdate_ErrorFailsAllPending(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, stubStream{}, 0, 7)

	first := startUpdate(sub)
	second := startUpdate(sub)

	routeEventually(t, sub, &message.RequestError{
		ErrorCode:   moqt.RequestInternalError,
		ErrorReason: "coalesced",
	})

	for i, ch := range []<-chan error{first, second} {
		select {
		case err := <-ch:
			var rejected *session.RequestRejectedError
			if !errors.As(err, &rejected) {
				t.Fatalf("Update #%d: got %v, want RequestRejectedError", i+1, err)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Update #%d still pending after coalesced REQUEST_ERROR", i+1)
		}
	}
}

// TestUpstreamSubUpdate_CloseFailsPendingAndFuture pins CloseUpdates: a
// pending Update fails with ErrUpstreamClosed and later calls fail fast.
func TestUpstreamSubUpdate_CloseFailsPendingAndFuture(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, stubStream{}, 0, 7)

	pending := startUpdate(sub)
	// Wait until the waiter is queued: CloseUpdates before queueing would
	// still fail the call (updClosed check), but we want the drain path.
	time.Sleep(10 * time.Millisecond)
	sub.CloseUpdates()

	if err := <-pending; !errors.Is(err, registry.ErrUpstreamClosed) {
		t.Fatalf("pending Update: got %v, want ErrUpstreamClosed", err)
	}
	if _, err := sub.Update(context.Background(), nil); !errors.Is(err, registry.ErrUpstreamClosed) {
		t.Fatalf("Update after close: got %v, want ErrUpstreamClosed", err)
	}
}

// TestUpstreamSubUpdate_TimeoutRemovesWaiter pins the anti-poisoning rule: a
// ctx-timed-out Update removes its waiter from the queue, so an upstream
// that never answers cannot permanently shift routing — the next response
// goes to the next live waiter, not to the abandoned one.
func TestUpstreamSubUpdate_TimeoutRemovesWaiter(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, stubStream{}, 0, 7)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := sub.Update(ctx, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Update: got %v, want DeadlineExceeded", err)
	}

	// The abandoned waiter must be gone: with no live waiter, a response is
	// reported as unsolicited (false), not swallowed by the stale channel.
	if sub.RouteUpdateResponse(&message.RequestOK{}) {
		t.Fatal("stale waiter consumed the response after its Update timed out")
	}

	// And a fresh Update still pairs with the next response.
	errCh := startUpdate(sub)
	routeEventually(t, sub, &message.RequestOK{})
	if err := <-errCh; err != nil {
		t.Fatalf("Update after timeout: %v", err)
	}
}
