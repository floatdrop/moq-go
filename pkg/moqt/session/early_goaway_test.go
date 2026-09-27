package session_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// A GOAWAY "MAY also be sent on a request stream to initiate migration of that
// individual request" (§10.4), before the request's response too, unless the
// response MUST come first (§6.1, §6.2). The requester keeps waiting for the
// response and reads the GOAWAY as the stream's first follow-up, as it would
// one sent after the response.

// answerAfterGoaways accepts one request on sess, sends it goaways, then
// answers it with reply.
func answerAfterGoaways(t *testing.T, sess *session.Session, goaways []*message.Goaway, reply func(*session.Request)) {
	t.Helper()
	go func() {
		r, err := sess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		for _, g := range goaways {
			if message.Marshal(r.Stream, g) != nil {
				return
			}
		}
		reply(r)
	}()
}

var migrate = &message.Goaway{NewSessionURI: []byte("https://relay.example/moq"), Timeout: 1000}

// TestEarlyRequestGoawayThenResponse: the request succeeds, and the GOAWAY is
// the first follow-up its reader sees, through a broker or a direct read.
func TestEarlyRequestGoawayThenResponse(t *testing.T) {
	t.Run("SUBSCRIBE", func(t *testing.T) {
		cli, srv := openPair(t)
		answerAfterGoaways(t, srv, []*message.Goaway{migrate}, func(r *session.Request) {
			p, err := r.AcceptSubscribe(&message.SubscribeOK{TrackAlias: 1})
			if err == nil {
				_ = p.Done(moqt.PublishDoneGoingAway, "")
			}
		})
		sub, err := cli.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		if err != nil {
			t.Fatalf("Subscribe after an early GOAWAY: %v", err)
		}
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
		if g, ok := got[0].(*message.Goaway); !ok || g.Timeout != migrate.Timeout {
			t.Fatalf("first follow-up = %#v, want the early GOAWAY", got[0])
		}
		requireStaysOpen(t, cli, 50*time.Millisecond)
	})
	t.Run("FETCH", func(t *testing.T) {
		cli, srv := openPair(t)
		answerAfterGoaways(t, srv, []*message.Goaway{migrate}, func(r *session.Request) {
			_, _ = r.AcceptFetch(&message.FetchOK{})
		})
		fr, err := cli.Fetch(t.Context(), &message.Fetch{Name: []byte("t")})
		if err != nil {
			t.Fatalf("Fetch after an early GOAWAY: %v", err)
		}
		m, err := message.Parse(fr.Stream)
		if err != nil {
			t.Fatalf("read follow-up: %v", err)
		}
		if _, ok := m.(*message.Goaway); !ok {
			t.Fatalf("first follow-up = %T, want *message.Goaway", m)
		}
		requireStaysOpen(t, cli, 50*time.Millisecond)
	})
}

// TestEarlyRequestGoawayOnPublishNamespace: a PUBLISH_NAMESPACE's response
// MUST be "the first message on the bidi stream" (§6.2), so a GOAWAY ahead of
// it fails the request, leaving the session open as with any other unexpected
// first message.
func TestEarlyRequestGoawayOnPublishNamespace(t *testing.T) {
	cli, srv := openPair(t)
	answerAfterGoaways(t, srv, []*message.Goaway{migrate}, func(r *session.Request) {
		_ = r.Reply(&message.RequestOK{})
	})
	if _, err := cli.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("ns")},
	}); err == nil {
		t.Fatal("PublishNamespace succeeded after a GOAWAY ahead of its response")
	}
	requireStaysOpen(t, cli, 50*time.Millisecond)
}

// TestEarlyRequestGoawayThenRejection: a REQUEST_ERROR after the GOAWAY is the
// request's answer.
func TestEarlyRequestGoawayThenRejection(t *testing.T) {
	cli, srv := openPair(t)
	answerAfterGoaways(t, srv, []*message.Goaway{migrate}, func(r *session.Request) {
		_ = r.RejectError(moqt.RequestDoesNotExist, "gone")
	})
	_, err := cli.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.Code != moqt.RequestDoesNotExist {
		t.Fatalf("Subscribe = %v, want its DOES_NOT_EXIST rejection", err)
	}
	requireStaysOpen(t, cli, 50*time.Millisecond)
}

// TestEarlyRequestGoawayViolationCloses: the §10.4 checks apply from the
// first message: a second GOAWAY before the response, one after an early one,
// or a New Session URI to the server closes the session with
// PROTOCOL_VIOLATION.
func TestEarlyRequestGoawayViolationCloses(t *testing.T) {
	t.Run("two before the response", func(t *testing.T) {
		cli, srv := openPair(t)
		answerAfterGoaways(t, srv, []*message.Goaway{{}, {}}, func(r *session.Request) {
			_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackAlias: 1})
		})
		if _, err := cli.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")}); err == nil {
			t.Fatal("Subscribe succeeded after two GOAWAYs")
		}
		requireClosedCode(t, cli, moqt.SessionProtocolViolation)
	})
	t.Run("one before and one after", func(t *testing.T) {
		cli, srv := openPair(t)
		answerAfterGoaways(t, srv, []*message.Goaway{{}}, func(r *session.Request) {
			if _, err := r.AcceptSubscribe(&message.SubscribeOK{TrackAlias: 1}); err == nil {
				_ = message.Marshal(r.Stream, &message.Goaway{})
			}
		})
		sub, err := cli.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		_ = sub.Broker().Serve(ctx, nil)
		requireClosedCode(t, cli, moqt.SessionProtocolViolation)
	})
	t.Run("New Session URI to the server", func(t *testing.T) {
		cli, srv := openPair(t)
		answerAfterGoaways(t, cli, []*message.Goaway{migrate}, func(r *session.Request) {
			_, _ = r.AcceptPublish()
		})
		if _, err := srv.Publish(t.Context(), &message.Publish{Name: []byte("t"), TrackAlias: 1}); err == nil {
			t.Fatal("Publish succeeded after a GOAWAY with a New Session URI to the server")
		}
		requireClosedCode(t, srv, moqt.SessionProtocolViolation)
	})
}
