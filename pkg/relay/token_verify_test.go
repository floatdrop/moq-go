package relay_test

import (
	"context"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestSessionHandler_TokenDenialMapsToRequestError verifies the token-verifier
// wiring end to end: a SUBSCRIBE carrying a USE_VALUE AUTHORIZATION_TOKEN is
// rejected by a session-level TokenVerifier, and the relay turns that denial
// into a REQUEST_ERROR with the verifier's chosen code (§10.2.2) — before any
// track lookup runs, so the code is the token code, not RequestDoesNotExist.
func TestSessionHandler_TokenDenialMapsToRequestError(t *testing.T) {
	t.Parallel()
	verifier := session.TokenVerifierFunc(func(_ context.Context, _ *session.Session, tok session.ResolvedToken) error {
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
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.AuthorizationTokenParam(message.Token{
				AliasType:  message.AliasTypeUseValue,
				TokenType:  1,
				TokenValue: []byte("expired"),
			}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestExpiredAuthToken)
}

// TestSessionHandler_TokenAllowReachesHandler verifies that a token the
// verifier accepts does not short-circuit dispatch: the request reaches the
// SUBSCRIBE handler, which (with no upstream) rejects with RequestDoesNotExist.
// This proves the verifier authorizes rather than blanket-denies.
func TestSessionHandler_TokenAllowReachesHandler(t *testing.T) {
	t.Parallel()
	verifier := session.TokenVerifierFunc(func(_ context.Context, _ *session.Session, _ session.ResolvedToken) error {
		return nil // accept everything
	})
	clientSess, teardown := connectRelay(t, relay.Config{
		SessionOptions: []session.Option{session.WithTokenVerifier(verifier)},
	})
	defer teardown()

	_, err := clientSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.AuthorizationTokenParam(message.Token{
				AliasType:  message.AliasTypeUseValue,
				TokenType:  1,
				TokenValue: []byte("valid"),
			}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}
