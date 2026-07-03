package session_test

import (
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestRequestMuxRoutesByType checks that RequestMux dispatches requests by their
// first message's Type, routes an unregistered type to OnUnknown, and honours a
// handler registered after Run has already started.
func TestRequestMuxRoutesByType(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)

	events := make(chan message.Type, 8)
	firstSeen := make(chan struct{}, 1)
	record := func(r *session.Request) {
		events <- r.First.Type()
		_ = r.Stream.Close()
	}

	mux := session.NewRequestMux()
	mux.Handle(message.TypeSubscribe, func(r *session.Request) {
		record(r)
		firstSeen <- struct{}{}
	})
	mux.Handle(message.TypePublishNamespace, record)
	mux.OnUnknown(record)

	go func() { _ = mux.Run(t.Context(), server) }()

	// Client sends even Request IDs (§10.1), strictly increasing.

	// 1. SUBSCRIBE on a registered type.
	openRequest(t, client, &message.Subscribe{
		RequestID:  0,
		Namespace:  wire.Namespace("ns"),
		Name:       []byte("a"),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	})
	<-firstSeen // ensure Run is live before late registration

	// 2. Register TRACK_STATUS *after* Run started, then send one.
	mux.Handle(message.TypeTrackStatus, record)
	openRequest(t, client, &message.TrackStatus{
		RequestID: 2,
		Namespace: wire.Namespace("ns"),
		Name:      []byte("a"),
	})

	// 3. PUBLISH_NAMESPACE on a registered type.
	openRequest(t, client, &message.PublishNamespace{
		RequestID: 4,
		Namespace: wire.Namespace("ns"),
	})

	// 4. SUBSCRIBE_NAMESPACE — unregistered → OnUnknown.
	openRequest(t, client, &message.SubscribeNamespace{
		RequestID:            6,
		TrackNamespacePrefix: wire.Namespace("ns"),
	})

	got := collectEvents(t, events, 4)
	for _, want := range []message.Type{
		message.TypeSubscribe,
		message.TypeTrackStatus,
		message.TypePublishNamespace,
		message.TypeSubscribeNamespace,
	} {
		if !slices.Contains(got, want) {
			t.Errorf("missing dispatched type %s; got %v", want, got)
		}
	}
}

// TestRequestMuxHandleType checks that HandleType derives the message.Type key
// from T and hands the handler the already-asserted typed message.
func TestRequestMuxHandleType(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)

	got := make(chan string, 1)
	mux := session.NewRequestMux()
	session.HandleType(mux, func(_ *session.Request, msg *message.Subscribe) {
		got <- string(msg.Name) // typed access without a manual assertion
	})
	go func() { _ = mux.Run(t.Context(), server) }()

	openRequest(t, client, &message.Subscribe{
		RequestID:  0,
		Namespace:  wire.Namespace("ns"),
		Name:       []byte("typed-track"),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	})

	select {
	case name := <-got:
		if name != "typed-track" {
			t.Errorf("handler saw Name = %q, want %q", name, "typed-track")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("typed handler was not invoked")
	}
}

// TestRequestMuxDefaultRejectsUnhandled verifies that with no OnUnknown set, an
// unhandled request type is rejected with REQUEST_ERROR NOT_SUPPORTED and its
// stream FIN'd, so the requester learns the server cannot serve it.
func TestRequestMuxDefaultRejectsUnhandled(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)

	mux := session.NewRequestMux() // no handlers, no OnUnknown
	go func() { _ = mux.Run(t.Context(), server) }()

	stream, err := session.OpenRequestForTest(client, &message.TrackStatus{
		RequestID: 0,
		Namespace: wire.Namespace("ns"),
		Name:      []byte("x"),
	})
	if err != nil {
		t.Fatalf("OpenRequest: %v", err)
	}

	resp, err := message.Parse(stream)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	rerr, ok := resp.(*message.RequestError)
	if !ok {
		t.Fatalf("got %s, want REQUEST_ERROR", resp.Type())
	}
	if rerr.ErrorCode != moqt.RequestNotSupported {
		t.Errorf("ErrorCode = %#x, want NOT_SUPPORTED (%#x)", rerr.ErrorCode, moqt.RequestNotSupported)
	}
}

// openRequest opens a bidi request stream carrying first as its initial message
// and leaves it open (the mux handler or session cleanup tears it down).
func openRequest(t *testing.T, s *session.Session, first message.Message) {
	t.Helper()
	if _, err := session.OpenRequestForTest(s, first); err != nil {
		t.Fatalf("OpenRequest(%s): %v", first.Type(), err)
	}
}
