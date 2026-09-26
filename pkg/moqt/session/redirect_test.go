package session_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// redirectError is a REDIRECT REQUEST_ERROR carrying rd.
func redirectError(rd message.Redirect) *message.RequestError {
	return &message.RequestError{ErrorCode: moqt.RequestRedirect, ErrorReason: "elsewhere", Redirect: &rd}
}

// answerRaw has sess accept the next request and write resp on its stream as
// is, bypassing the checks [session.Request.Reject] applies.
func answerRaw(t *testing.T, sess *session.Session, resp message.Message) {
	t.Helper()
	go func() {
		r, err := sess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = message.Marshal(r.Stream, resp)
	}()
}

// TestRedirectFollowable: a REDIRECT's Redirect reaches the requester on its
// RequestRejectedError (§10.6.1), sent with Reject, and a Connect URI to the
// client or a Track Name for a SUBSCRIBE keeps the session open.
func TestRedirectFollowable(t *testing.T) {
	cli, srv := openPair(t)
	want := &message.Redirect{
		ConnectURI: []byte("https://other.example/moq"),
		Namespace:  wire.TrackNamespace{[]byte("elsewhere")},
		TrackName:  []byte("t2"),
	}
	rejected := make(chan error, 1)
	go func() {
		r, err := srv.AcceptRequest(t.Context())
		if err != nil {
			rejected <- err
			return
		}
		rejected <- r.Reject(&session.RequestRejectedError{Code: moqt.RequestRedirect, Reason: "moved", Redirect: want})
	}()
	_, err := cli.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err := <-rejected; err != nil {
		t.Fatalf("Reject: %v", err)
	}
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.Code != moqt.RequestRedirect {
		t.Fatalf("Subscribe = %v, want a REDIRECT", err)
	}
	if !reflect.DeepEqual(rej.Redirect, want) {
		t.Fatalf("Redirect = %+v, want %+v", rej.Redirect, want)
	}
	requireStaysOpen(t, cli, 50*time.Millisecond)
}

// TestReceivedRedirectViolationCloses: a Redirect with a Connect URI received
// by a server, or with a Track Name for a namespace-scoped request, closes the
// session with PROTOCOL_VIOLATION (§10.6.1), on every path a REQUEST_ERROR is
// read.
func TestReceivedRedirectViolationCloses(t *testing.T) {
	uri := message.Redirect{ConnectURI: []byte("https://other.example/moq")}
	name := message.Redirect{Namespace: wire.TrackNamespace{[]byte("ns")}, TrackName: []byte("t")}
	ns := wire.TrackNamespace{[]byte("ns")}
	for _, tc := range []struct {
		name string
		// toServer: the server is the requester and receives the REDIRECT.
		toServer bool
		rd       message.Redirect
		open     func(t *testing.T, requester *session.Session) error
	}{
		{"Connect URI at the server, SUBSCRIBE", true, uri, func(t *testing.T, s *session.Session) error {
			_, err := s.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
			return err
		}},
		{"Connect URI at the server, AwaitPublishOK", true, uri, func(t *testing.T, s *session.Session) error {
			stream, err := s.OpenPublish(&message.Publish{Name: []byte("t"), TrackAlias: 1})
			if err != nil {
				return err
			}
			_, err = s.AwaitPublishOK(t.Context(), stream)
			return err
		}},
		{"Track Name for SUBSCRIBE_NAMESPACE", false, name, func(t *testing.T, s *session.Session) error {
			_, err := s.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: ns})
			return err
		}},
		{"Track Name for PUBLISH_NAMESPACE", false, name, func(t *testing.T, s *session.Session) error {
			_, err := s.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns})
			return err
		}},
		{"Track Name for SUBSCRIBE_TRACKS", false, name, func(t *testing.T, s *session.Session) error {
			_, err := s.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			requester, responder := cli, srv
			if tc.toServer {
				requester, responder = srv, cli
			}
			answerRaw(t, responder, redirectError(tc.rd))
			_ = tc.open(t, requester)
			requireClosedCode(t, requester, moqt.SessionProtocolViolation)
		})
	}
}

// TestReceivedRedirectViolationClosesOnUpdate: a Redirect carrying a Connect
// URI on a server's request stream after the response closes the session
// (§10.6.1): answering a REQUEST_UPDATE read by Update itself or routed by the
// broker, or unsolicited, handed to Serve's callback.
func TestReceivedRedirectViolationClosesOnUpdate(t *testing.T) {
	for _, tc := range []struct {
		name          string
		served, apply bool
	}{
		{"read by Update", false, true},
		{"routed by the broker", true, true},
		{"unsolicited", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			sub, pub := subscribePair(t, srv, cli)
			go func() {
				if tc.apply {
					if _, err := message.Parse(pub); err != nil {
						return
					}
				}
				_ = message.Marshal(
					pub,
					redirectError(message.Redirect{ConnectURI: []byte("https://other.example/moq")}),
				)
			}()
			if tc.served {
				b := sub.Broker()
				go func() { _ = b.Serve(t.Context(), nil) }()
			}
			if tc.apply {
				_, _ = sub.Update(t.Context(), nil)
			}
			requireClosedCode(t, srv, moqt.SessionProtocolViolation)
		})
	}
}

// TestRejectRefusesSessionFatalRedirect: Reject does not send a Redirect its
// peer would have to close the session for (§10.6.1), so the request can still
// be refused another way.
func TestRejectRefusesSessionFatalRedirect(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("ns")}
	for _, tc := range []struct {
		name     string
		toServer bool
		rd       *message.Redirect
		open     func(t *testing.T, requester *session.Session) error
	}{
		{"Connect URI to the server", true, &message.Redirect{ConnectURI: []byte("https://other.example/moq")},
			func(t *testing.T, s *session.Session) error {
				_, err := s.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
				return err
			}},
		{"Track Name for SUBSCRIBE_NAMESPACE", false, &message.Redirect{Namespace: ns, TrackName: []byte("t")},
			func(t *testing.T, s *session.Session) error {
				_, err := s.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: ns})
				return err
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			requester, responder := cli, srv
			if tc.toServer {
				requester, responder = srv, cli
			}
			refused := make(chan error, 1)
			go func() {
				r, err := responder.AcceptRequest(t.Context())
				if err != nil {
					refused <- err
					return
				}
				refused <- r.Reject(&session.RequestRejectedError{Code: moqt.RequestRedirect, Redirect: tc.rd})
				_ = r.RejectError(moqt.RequestDoesNotExist, "no")
			}()
			err := tc.open(t, requester)
			if rej, ok := errors.AsType[*session.RequestRejectedError](
				err,
			); !ok ||
				rej.Code != moqt.RequestDoesNotExist {
				t.Fatalf("request = %v, want the DOES_NOT_EXIST sent after the refused REDIRECT", err)
			}
			if err := <-refused; err == nil {
				t.Fatal("Reject sent a Redirect the peer must close the session for")
			}
			requireStaysOpen(t, requester, 50*time.Millisecond)
		})
	}
}

// TestUpdateHandlerRedirectSentAsInternalError: REDIRECT cannot answer a
// REQUEST_UPDATE (§10.6.2), so an UpdateHandler's REDIRECT goes out as
// INTERNAL_ERROR rather than as a REDIRECT the peer cannot parse, and both
// sessions stay open.
func TestUpdateHandlerRedirectSentAsInternalError(t *testing.T) {
	client, server := openPair(t)
	sub, pub := subscribePair(t, client, server)
	b := pub.Broker()
	b.HandleUpdates(func(*message.RequestUpdate) (*message.RequestOK, error) {
		return nil, &session.RequestRejectedError{Code: moqt.RequestRedirect, Reason: "moved"}
	})
	go func() { _ = b.Serve(t.Context(), nil) }()

	_, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)})
	if rej, ok := errors.AsType[*session.RequestRejectedError](err); !ok || rej.Code != moqt.RequestInternalError {
		t.Fatalf("Update = %v, want INTERNAL_ERROR", err)
	}
	requireStaysOpen(t, client, 50*time.Millisecond)
}
