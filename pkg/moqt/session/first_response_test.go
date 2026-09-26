package session_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestNamespaceScopedFirstResponseCloses: a first response other than
// REQUEST_OK or REQUEST_ERROR to SUBSCRIBE_NAMESPACE or SUBSCRIBE_TRACKS closes
// the session with PROTOCOL_VIOLATION (§10.19, §10.20), even one that is legal
// later on the stream.
func TestNamespaceScopedFirstResponseCloses(t *testing.T) {
	prefix := wire.TrackNamespace{[]byte("ns")}
	requests := []struct {
		name string
		open func(t *testing.T, s *session.Session) error
	}{
		{"SUBSCRIBE_NAMESPACE", func(t *testing.T, s *session.Session) error {
			_, err := s.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: prefix})
			return err
		}},
		{"SUBSCRIBE_TRACKS", func(t *testing.T, s *session.Session) error {
			_, err := s.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: prefix})
			return err
		}},
	}
	responses := []struct {
		name string
		msg  message.Message
	}{
		{"NAMESPACE", &message.Namespace{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("a")}}},
		{"GOAWAY", &message.Goaway{}},
		{"SUBSCRIBE_OK", &message.SubscribeOK{TrackAlias: 1}},
	}
	for _, req := range requests {
		for _, resp := range responses {
			t.Run(req.name+" answered with "+resp.name, func(t *testing.T) {
				cli, srv := openPair(t)
				answerRaw(t, srv, resp.msg)
				if err := req.open(t, cli); err == nil {
					t.Fatalf("%s succeeded on a %s", req.name, resp.name)
				}
				requireClosedCode(t, cli, moqt.SessionProtocolViolation)
			})
		}
	}
}

// TestOtherFirstResponseKeepsSession: no MUST covers the first response to
// other requests (§5.1, §6.2 put theirs on the sender), so an unexpected one
// fails the request and leaves the session open.
func TestOtherFirstResponseKeepsSession(t *testing.T) {
	for _, tc := range []struct {
		name string
		open func(t *testing.T, s *session.Session) error
	}{
		{"SUBSCRIBE", func(t *testing.T, s *session.Session) error {
			_, err := s.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
			return err
		}},
		{"PUBLISH_NAMESPACE", func(t *testing.T, s *session.Session) error {
			_, err := s.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: wire.TrackNamespace{[]byte("ns")}})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			answerRaw(t, srv, &message.Namespace{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("a")}})
			if err := tc.open(t, cli); err == nil {
				t.Fatalf("%s succeeded on a NAMESPACE", tc.name)
			}
			requireStaysOpen(t, cli, 50*time.Millisecond)
		})
	}
}
