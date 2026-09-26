package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestRequestStreamGoawayViolationCloses: a second GOAWAY on one request
// stream, or a GOAWAY carrying a New Session URI received by the server,
// closes the session with PROTOCOL_VIOLATION (§10.4), read by the receiving
// handle's broker.
func TestRequestStreamGoawayViolationCloses(t *testing.T) {
	uri := &message.Goaway{NewSessionURI: []byte("https://relay.example/moq")}
	for _, tc := range []struct {
		name     string
		toServer bool
		sent     []*message.Goaway
	}{
		{"second GOAWAY to the client", false, []*message.Goaway{{}, uri}},
		{"second GOAWAY to the server", true, []*message.Goaway{{}, {}}},
		{"New Session URI to the server", true, []*message.Goaway{uri}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			sub, pub := subscribePair(t, cli, srv)
			var from session.Stream = pub
			receiver, broker := cli, sub.Broker()
			if tc.toServer {
				from = sub
				receiver, broker = srv, pub.Broker()
			}
			go func() {
				for _, m := range tc.sent {
					if err := message.Marshal(from, m); err != nil {
						return
					}
				}
			}()
			// Bounded: a GOAWAY that is ignored leaves the stream open.
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			_ = broker.Serve(ctx, nil)
			requireClosedCode(t, receiver, moqt.SessionProtocolViolation)
		})
	}
}

// TestRequestStreamGoawayOnceIsDelivered: one GOAWAY on a request stream, with
// a New Session URI when sent to the client, is legal (§10.4) and reaches
// Serve's callback.
func TestRequestStreamGoawayOnceIsDelivered(t *testing.T) {
	cli, srv := openPair(t)
	sub, pub := subscribePair(t, cli, srv)
	go func() {
		if err := message.Marshal(
			pub,
			&message.Goaway{NewSessionURI: []byte("https://relay.example/moq")},
		); err != nil {
			return
		}
		_ = pub.Done(moqt.PublishDoneGoingAway, "")
	}()
	var got []message.Message
	if err := sub.Broker().Serve(t.Context(), func(m message.Message) bool {
		got = append(got, m)
		return true
	}); err != nil {
		t.Fatalf("Serve: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Serve's callback never saw the GOAWAY")
	}
	if _, ok := got[0].(*message.Goaway); !ok {
		t.Fatalf("first message = %T, want *message.Goaway", got[0])
	}
	if err := cli.Err(); err != nil {
		t.Fatalf("one GOAWAY on a request stream closed the session: %v", err)
	}
}
