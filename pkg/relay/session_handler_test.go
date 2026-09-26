package relay_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestSessionHandler_AuthDenialMapsToRequestError: an Authorizer rejecting
// SUBSCRIBE produces REQUEST_ERROR with the policy's code.
func TestSessionHandler_AuthDenialMapsToRequestError(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{
		err: relay.Deny(moqt.RequestUnauthorized, "test denial"),
	}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)

	var rejected *session.RequestRejectedError
	if !errors.As(err, &rejected) {
		t.Fatalf("expected *RequestRejectedError, got %T", err)
	}
	if rejected.Reason != "test denial" {
		t.Errorf("Reason = %q, want %q", rejected.Reason, "test denial")
	}
	if got := auth.subscribeCalls.Load(); got != 1 {
		t.Errorf("AuthorizeSubscribe called %d times, want 1", got)
	}
}

// TestRelay_AuthDenialUsesPolicyCode: each request type consults its own
// Authorizer method once, before any track lookup, and a denial reaches the
// requester as REQUEST_ERROR with the policy's code (UNAUTHORIZED for a plain
// error).
func TestRelay_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		err   error
		send  func(t *testing.T, sess *session.Session) error
		calls func(a *denyAuthorizer) int32
	}{
		{
			"SUBSCRIBE", errors.New("token expired"),
			func(t *testing.T, sess *session.Session) error {
				_, err := sess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
				return err
			},
			func(a *denyAuthorizer) int32 { return a.subscribeCalls.Load() },
		},
		{
			"FETCH", errors.New("no fetch for you"),
			func(t *testing.T, sess *session.Session) error {
				_, err := sess.Fetch(t.Context(), &message.Fetch{Namespace: ns("video"), Name: []byte("cam1")})
				return err
			},
			func(a *denyAuthorizer) int32 { return a.fetchCalls.Load() },
		},
		{
			"TRACK_STATUS", errors.New("no status"),
			func(t *testing.T, sess *session.Session) error {
				_, err := sess.TrackStatus(t.Context(), &message.TrackStatus{Namespace: ns("video"), Name: []byte("cam1")})
				return err
			},
			func(a *denyAuthorizer) int32 { return a.trackStatusCalls.Load() },
		},
		{
			"PUBLISH_NAMESPACE", relay.Deny(moqt.RequestUnauthorized, "nope"),
			func(t *testing.T, sess *session.Session) error {
				_, err := sess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns("video")})
				return err
			},
			func(a *denyAuthorizer) int32 { return a.publishNamespaceCalls.Load() },
		},
		{
			"SUBSCRIBE_NAMESPACE", relay.Deny(moqt.RequestUnauthorized, "no subscribing"),
			func(t *testing.T, sess *session.Session) error {
				_, err := sess.SubscribeNamespace(t.Context(),
					&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")})
				return err
			},
			func(a *denyAuthorizer) int32 { return a.subscribeNamespaceCalls.Load() },
		},
		{
			"SUBSCRIBE_TRACKS", relay.Deny(moqt.RequestUnauthorized, "no tracks"),
			func(t *testing.T, sess *session.Session) error {
				_, err := sess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns("video")})
				return err
			},
			func(a *denyAuthorizer) int32 { return a.subscribeTracksCalls.Load() },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			auth := &denyAuthorizer{err: tc.err}
			clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
			defer teardown()

			requireRejectedWithCode(t, tc.send(t, clientSess), moqt.RequestUnauthorized)
			if got := tc.calls(auth); got != 1 {
				t.Errorf("%s Authorizer calls = %d, want 1", tc.name, got)
			}
		})
	}
}

// TestSessionHandler_TokenVerification: a TokenVerifier's denial of a USE_VALUE
// AUTHORIZATION_TOKEN is REQUEST_ERROR with its code, before any track lookup
// (§10.2.2); an accepted token lets the SUBSCRIBE reach its handler, which
// refuses the unknown track DOES_NOT_EXIST.
func TestSessionHandler_TokenVerification(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		token string
		want  moqt.RequestErrorCode
	}{
		{"denied", "expired", moqt.RequestExpiredAuthToken},
		{"allowed", "valid", moqt.RequestDoesNotExist},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			verifier := session.TokenVerifierFunc(
				func(_ context.Context, _ *session.Session, tok session.ResolvedToken) error {
					if string(tok.Value) == "expired" {
						return session.DenyToken(moqt.RequestExpiredAuthToken, "token expired")
					}
					return nil
				})
			clientSess, teardown := connectRelay(t, relay.Config{
				SessionOptions: []session.Option{session.WithTokenVerifier(verifier)},
			})
			defer teardown()

			_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
				Namespace: ns("video"),
				Name:      []byte("cam1"),
				Parameters: message.Parameters{
					message.AuthorizationTokenParam(message.Token{
						AliasType:  message.AliasTypeUseValue,
						TokenType:  1,
						TokenValue: []byte(tc.token),
					}),
				},
			})
			requireRejectedWithCode(t, err, tc.want)
		})
	}
}

// TestSessionHandler_DispatchSurvivesPerRequestRejection: three SUBSCRIBEs for
// unknown tracks on one session are each refused DOES_NOT_EXIST, and the
// dispatch loop survives them.
func TestSessionHandler_DispatchSurvivesPerRequestRejection(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	for range 3 {
		_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: ns("video"),
			Name:      []byte("cam1"),
		})
		requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
	}
}

// denyAuthorizer is a recording authorizer that returns err from every
// method. Used to prove the dispatch table routes each message type to the
// correct AuthorizeX call.
type denyAuthorizer struct {
	err                     error
	subscribeCalls          atomic.Int32
	publishCalls            atomic.Int32
	publishNamespaceCalls   atomic.Int32
	subscribeNamespaceCalls atomic.Int32
	subscribeTracksCalls    atomic.Int32
	fetchCalls              atomic.Int32
	trackStatusCalls        atomic.Int32
}

func (a *denyAuthorizer) AuthorizeSubscribe(context.Context, *session.Session, *message.Subscribe) error {
	a.subscribeCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	a.publishCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizePublishNamespace(context.Context, *session.Session, *message.PublishNamespace) error {
	a.publishNamespaceCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeFetch(context.Context, *session.Session, *message.Fetch) error {
	a.fetchCalls.Add(1)
	return a.err
}

func (a *denyAuthorizer) AuthorizeSubscribeNamespace(
	context.Context,
	*session.Session,
	*message.SubscribeNamespace,
) error {
	a.subscribeNamespaceCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeSubscribeTracks(context.Context, *session.Session, *message.SubscribeTracks) error {
	a.subscribeTracksCalls.Add(1)
	return a.err
}
func (a *denyAuthorizer) AuthorizeTrackStatus(context.Context, *session.Session, *message.TrackStatus) error {
	a.trackStatusCalls.Add(1)
	return a.err
}

// TestSessionHandler_TruncatedOpenerKeepsServing: a request stream that ends
// or is reset before its first message is complete fails that request only
// (§3.3.2, §3.3.3); the relay goes on serving the session.
func TestSessionHandler_TruncatedOpenerKeepsServing(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		end  func(session.Stream)
	}{
		{"FIN", func(s session.Stream) { _ = s.Close() }},
		{"reset", func(s session.Stream) { s.CancelWrite(0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			l := newPipeListener()
			_, teardown := connectRelayOn(t, relay.Config{}, l)
			defer teardown()
			peer, conn := dialRaw(t, l)
			stream, err := conn.OpenStream()
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			// Type SUBSCRIBE, Length 16, then only two body bytes.
			_, _ = stream.Write([]byte{byte(message.TypeSubscribe), 0x00, 0x10, 0x00, 0x00})
			tc.end(stream)

			// The pipe transport blocks the opener's write until the relay
			// reads it, which no context bounds, so wait here instead.
			errc := make(chan error, 1)
			go func() {
				_, err := peer.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
				errc <- err
			}()
			select {
			case err := <-errc:
				requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
			case <-time.After(2 * time.Second):
				t.Fatal("relay stopped reading requests after the truncated one")
			}
		})
	}
}

// TestSessionHandler_BadRequestIDClosesSession: a request opener with a
// wrong-parity Request ID closes the session (§10.1).
func TestSessionHandler_BadRequestIDClosesSession(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	_, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()
	peer, conn := dialRaw(t, l)
	stream, err := conn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	// A client's Request IDs are even.
	_ = message.Marshal(stream, &message.Subscribe{RequestID: 1, Namespace: ns("video"), Name: []byte("cam1")})
	requireSessionClosed(t, peer, "a SUBSCRIBE with an odd Request ID from a client")
}
