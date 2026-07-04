package registry_test

import (
	"context"
	"errors"
	"testing"

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

// TestUpstreamSub_UpdateDelegatesToBroker pins the delegation contract after
// the §10.9 update broker moved into the session package: UpstreamSub.Update
// rides the sub's [session.RequestBroker], so once the broker is closed
// (CloseOnDemand / the Serve loop exiting) pending and future updates fail
// with [session.ErrRequestStreamClosed]. The broker's routing semantics
// themselves are pinned in the session package's broker tests.
func TestUpstreamSub_UpdateDelegatesToBroker(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, stubStream{}, 0, 7)

	sub.CloseOnDemand()
	if !sub.IsTerminated() {
		t.Fatal("CloseOnDemand must terminate the subscription")
	}
	if _, err := sub.Update(context.Background(), nil); !errors.Is(err, session.ErrRequestStreamClosed) {
		t.Fatalf("Update after CloseOnDemand: got %v, want session.ErrRequestStreamClosed", err)
	}
}

// TestUpstreamSub_NilBrokerFixtures pins the literal-construction escape
// hatch used by tests: an UpstreamSub built without NewUpstreamSub has no
// broker, and Update / WriteMessage fail fast instead of panicking.
func TestUpstreamSub_NilBrokerFixtures(t *testing.T) {
	t.Parallel()
	sub := &registry.UpstreamSub{}
	if _, err := sub.Update(context.Background(), nil); !errors.Is(err, session.ErrRequestStreamClosed) {
		t.Fatalf("Update: got %v, want session.ErrRequestStreamClosed", err)
	}
	if err := sub.WriteMessage(nil); !errors.Is(err, session.ErrRequestStreamClosed) {
		t.Fatalf("WriteMessage: got %v, want session.ErrRequestStreamClosed", err)
	}
}
