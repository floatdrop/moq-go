package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_NoRequestsToPeerSentGoaway: once the relay has sent a publisher
// GOAWAY it avoids initiating requests to it (§10.4: "the sender SHOULD avoid
// initiating requests unless required by migration"), so a SUBSCRIBE that
// needs that publisher is refused with GOING_AWAY rather than sent upstream.
func TestRelay_NoRequestsToPeerSentGoaway(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	r := relay.New(l, relay.Config{GoawayTimeout: 5 * time.Second})
	go func() { _ = r.Start(t.Context()) }()
	dial := func() *session.Session {
		conn, err := l.Dial()
		if err != nil {
			t.Fatalf("Dial: %v", err)
		}
		s, err := session.Client(t.Context(), conn)
		if err != nil {
			t.Fatalf("session.Client: %v", err)
		}
		return s
	}
	pubSess, subSess := dial(), dial()
	stopped := make(chan struct{})
	defer func() {
		_ = pubSess.Close(moqt.SessionNoError, "done")
		_ = subSess.Close(moqt.SessionNoError, "done")
		<-stopped
	}()

	video := ns("video")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	subscribes := make(chan *session.Request, 4)
	go func() {
		for {
			req, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			subscribes <- req
			_ = req.Reply(&message.SubscribeOK{TrackAlias: 1})
		}
	}()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
		close(stopped)
	}()
	select {
	case <-pubSess.GoawayReceived():
	case <-time.After(2 * time.Second):
		t.Fatal("the publisher never got the relay's GOAWAY")
	}

	_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
	select {
	case req := <-subscribes:
		t.Fatalf("the relay sent %s to a publisher it had sent GOAWAY", req.First.Type())
	default:
	}
	requireRejectedWithCode(t, err, moqt.RequestGoingAway)
}
