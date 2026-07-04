package main

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestServeRequestStreamAnswersRequestUpdate pins the §10.9 obligation the
// demo publisher has as the receiver of a REQUEST_UPDATE on its PUBLISH
// request stream: every update must be answered with exactly one REQUEST_OK
// (the demo applies no parameters). A publisher that only drains the stream
// leaves the subscriber's update unanswered forever.
func TestServeRequestStreamAnswersRequestUpdate(t *testing.T) {
	pubSess, subSess := sessiontest.NewSessionPair(t)
	ctx := t.Context()

	// Subscriber side: accept the PUBLISH and keep the request stream.
	accepted := make(chan *session.Request, 1)
	go func() {
		req, err := subSess.AcceptRequest(ctx)
		if err != nil {
			return
		}
		if _, ok := req.First.(*message.Publish); !ok {
			return
		}
		if err := req.Reply(&message.RequestOK{}); err != nil {
			return
		}
		accepted <- req
	}()

	pub, err := pubSess.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("demo"),
		Name:      []byte("video"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	go servePublication(ctx, "video", pub)

	var req *session.Request
	select {
	case req = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("PUBLISH never accepted")
	}

	// Send a REQUEST_UPDATE follow-up and await the mandated REQUEST_OK.
	// The server side allocates odd Request IDs (§10.1); the update consumes one.
	if err := message.Marshal(req.Stream, &message.RequestUpdate{RequestID: 1}); err != nil {
		t.Fatalf("write REQUEST_UPDATE: %v", err)
	}
	type parsed struct {
		msg message.Message
		err error
	}
	got := make(chan parsed, 1)
	go func() {
		m, err := message.Parse(req.Stream)
		got <- parsed{m, err}
	}()
	select {
	case p := <-got:
		if p.err != nil {
			t.Fatalf("read update response: %v", p.err)
		}
		if _, ok := p.msg.(*message.RequestOK); !ok {
			t.Fatalf("update answered with %T, want *message.RequestOK", p.msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("REQUEST_UPDATE was never answered (§10.9)")
	}
}
