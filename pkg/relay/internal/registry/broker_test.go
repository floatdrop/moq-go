package registry_test

import (
	"context"
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// TestUpstreamSub_UpdateDelegatesToBroker: UpstreamSub.Update rides the sub's
// §10.9 [session.RequestBroker], so once CloseOnDemand closes it updates fail
// with [session.ErrRequestStreamClosed].
func TestUpstreamSub_UpdateDelegatesToBroker(t *testing.T) {
	t.Parallel()
	sub := registry.NewUpstreamSub(1, nil, stubStream{}, 0, 7, false)

	sub.CloseOnDemand()
	if !sub.IsTerminated() {
		t.Fatal("CloseOnDemand must terminate the subscription")
	}
	if _, err := sub.Update(context.Background(), nil); !errors.Is(err, session.ErrRequestStreamClosed) {
		t.Fatalf("Update after CloseOnDemand: got %v, want session.ErrRequestStreamClosed", err)
	}
}

// TestUpstreamSub_NilBrokerFixtures: an UpstreamSub built without
// NewUpstreamSub has no broker; Update and WriteMessage fail instead of panicking.
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
