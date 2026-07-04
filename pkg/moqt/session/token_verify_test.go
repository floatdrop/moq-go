package session_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// openTokenPair opens a client/server pair where the server is configured
// with the given options (typically WithMaxAuthTokenCacheSize and/or
// WithTokenVerifier). The client is plain. Both sessions are closed on
// cleanup.
func openTokenPair(t *testing.T, serverOpts ...session.Option) (client, server *session.Session) {
	t.Helper()
	ctx := t.Context()
	aConn, bConn := sessiontest.NewConnPair()

	var (
		wg         sync.WaitGroup
		aErr, bErr error
	)
	wg.Go(func() {
		client, aErr = session.Client(ctx, aConn,
			session.WithImplementation("mediamesh-test/client"),
		)
	})
	wg.Go(func() {
		server, bErr = session.Server(ctx, bConn,
			append([]session.Option{session.WithImplementation("mediamesh-test/server")}, serverOpts...)...,
		)
	})
	wg.Wait()
	if aErr != nil {
		t.Fatalf("client Open: %v", aErr)
	}
	if bErr != nil {
		t.Fatalf("server Open: %v", bErr)
	}
	t.Cleanup(func() {
		_ = client.Close(moqt.SessionNoError, "test cleanup")
		_ = server.Close(moqt.SessionNoError, "test cleanup")
	})
	return client, server
}

// sendSubscribeWithTokens opens a request stream from client carrying a
// SUBSCRIBE with the given tokens as AUTHORIZATION_TOKEN parameters. The open
// runs in a background goroutine because OpenRequest writes the SUBSCRIBE
// synchronously and the pipe-backed write blocks until the peer accepts the
// stream and reads it — the caller is expected to invoke AcceptRequest on the
// server side. The opened stream is cancelled on cleanup so neither side blocks
// after the test.
func sendSubscribeWithTokens(t *testing.T, client *session.Session, toks ...message.Token) {
	t.Helper()
	var ps message.Parameters
	for _, tok := range toks {
		ps = append(ps, message.AuthorizationTokenParam(tok))
	}
	sub := &message.Subscribe{
		RequestID:  client.AllocRequestID(),
		Name:       []byte("track"),
		Parameters: ps,
	}
	streamCh := make(chan session.Stream, 1)
	go func() {
		stream, err := session.OpenRequestForTest(client, sub)
		if err != nil {
			t.Errorf("client OpenRequest: %v", err)
			close(streamCh)
			return
		}
		streamCh <- stream
	}()
	t.Cleanup(func() {
		if stream, ok := <-streamCh; ok && stream != nil {
			stream.CancelRead(uint64(moqt.StreamResetCancelled))
			stream.CancelWrite(uint64(moqt.StreamResetCancelled))
		}
	})
}

// TestAcceptRequestResolvesRegisterToken verifies that a REGISTER token on an
// inbound request is committed to the cache and surfaced as a ResolvedToken,
// and that a later USE_ALIAS on a separate request resolves to the same
// (Type, Value).
func TestAcceptRequestResolvesRegisterToken(t *testing.T) {
	client, server := openTokenPair(t, session.WithMaxAuthTokenCacheSize(4096))

	// Request 1: REGISTER alias 7 → (type 9, "secret").
	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeRegister,
		TokenAlias: 7,
		TokenType:  9,
		TokenValue: []byte("secret"),
	})
	req1, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest 1: %v", err)
	}
	if len(req1.Tokens) != 1 {
		t.Fatalf("req1.Tokens len = %d, want 1", len(req1.Tokens))
	}
	if req1.Tokens[0].Type != 9 || string(req1.Tokens[0].Value) != "secret" {
		t.Errorf("req1 resolved token = %+v, want {9, secret}", req1.Tokens[0])
	}

	// Request 2: USE_ALIAS 7 → must resolve to the registered value.
	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseAlias,
		TokenAlias: 7,
	})
	req2, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest 2: %v", err)
	}
	if len(req2.Tokens) != 1 {
		t.Fatalf("req2.Tokens len = %d, want 1", len(req2.Tokens))
	}
	if req2.Tokens[0].Type != 9 || string(req2.Tokens[0].Value) != "secret" {
		t.Errorf("req2 resolved token = %+v, want {9, secret}", req2.Tokens[0])
	}
}

// TestAcceptRequestUseValueToken verifies that a USE_VALUE token is surfaced
// directly without touching the cache (so it works even with aliasing
// prohibited, maxSize=0).
func TestAcceptRequestUseValueToken(t *testing.T) {
	client, server := openTokenPair(t) // no cache budget: aliasing prohibited

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseValue,
		TokenType:  3,
		TokenValue: []byte("inline"),
	})
	req, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	if len(req.Tokens) != 1 {
		t.Fatalf("Tokens len = %d, want 1", len(req.Tokens))
	}
	if req.Tokens[0].Type != 3 || string(req.Tokens[0].Value) != "inline" {
		t.Errorf("resolved token = %+v, want {3, inline}", req.Tokens[0])
	}
}

// TestAcceptRequestDuplicateAliasIsSessionError verifies that registering the
// same alias twice is a session-level fault carrying
// SessionDuplicateAuthTokenAlias.
func TestAcceptRequestDuplicateAliasIsSessionError(t *testing.T) {
	client, server := openTokenPair(t, session.WithMaxAuthTokenCacheSize(4096))

	reg := message.Token{AliasType: message.AliasTypeRegister, TokenAlias: 1, TokenType: 1, TokenValue: []byte("a")}
	sendSubscribeWithTokens(t, client, reg)
	if _, err := server.AcceptRequest(t.Context()); err != nil {
		t.Fatalf("AcceptRequest 1: %v", err)
	}

	// Re-register alias 1 → duplicate.
	sendSubscribeWithTokens(t, client, reg)
	_, err := server.AcceptRequest(t.Context())
	if err == nil {
		t.Fatal("AcceptRequest 2: expected duplicate-alias error, got nil")
	}
	var tce *session.TokenCacheError
	if !errors.As(err, &tce) {
		t.Fatalf("error = %v, want *TokenCacheError", err)
	}
	if tce.Code != moqt.SessionDuplicateAuthTokenAlias {
		t.Errorf("Code = 0x%X, want SessionDuplicateAuthTokenAlias", uint64(tce.Code))
	}
}

// TestAcceptRequestUnknownAliasIsSessionError verifies that USE_ALIAS for an
// unregistered alias yields SessionUnknownAuthTokenAlias.
func TestAcceptRequestUnknownAliasIsSessionError(t *testing.T) {
	client, server := openTokenPair(t, session.WithMaxAuthTokenCacheSize(4096))

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseAlias,
		TokenAlias: 99,
	})
	_, err := server.AcceptRequest(t.Context())
	if err == nil {
		t.Fatal("expected unknown-alias error, got nil")
	}
	var tce *session.TokenCacheError
	if !errors.As(err, &tce) {
		t.Fatalf("error = %v, want *TokenCacheError", err)
	}
	if tce.Code != moqt.SessionUnknownAuthTokenAlias {
		t.Errorf("Code = 0x%X, want SessionUnknownAuthTokenAlias", uint64(tce.Code))
	}
}

// TestAcceptRequestRegisterPersistsWhenAliasingProhibited verifies that with
// no negotiated cache budget (maxSize=0) a REGISTER is an overflow fault per
// §10.3.1.3.
func TestAcceptRequestRegisterProhibitedIsOverflow(t *testing.T) {
	client, server := openTokenPair(t) // maxSize 0

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeRegister,
		TokenAlias: 1,
		TokenType:  1,
		TokenValue: []byte("x"),
	})
	_, err := server.AcceptRequest(t.Context())
	if err == nil {
		t.Fatal("expected overflow error, got nil")
	}
	var tce *session.TokenCacheError
	if !errors.As(err, &tce) {
		t.Fatalf("error = %v, want *TokenCacheError", err)
	}
	if tce.Code != moqt.SessionAuthTokenCacheOverflow {
		t.Errorf("Code = 0x%X, want SessionAuthTokenCacheOverflow", uint64(tce.Code))
	}
}

// TestVerifyRequestTokensAllow verifies that a verifier returning nil
// authorizes the request.
func TestVerifyRequestTokensAllow(t *testing.T) {
	var seen session.ResolvedToken
	verifier := session.TokenVerifierFunc(func(_ context.Context, _ *session.Session, tok session.ResolvedToken) error {
		seen = tok
		return nil
	})
	client, server := openTokenPair(t, session.WithTokenVerifier(verifier))

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseValue,
		TokenType:  5,
		TokenValue: []byte("ok"),
	})
	req, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	if err := server.VerifyRequestTokens(t.Context(), req); err != nil {
		t.Fatalf("VerifyRequestTokens: unexpected denial: %v", err)
	}
	if seen.Type != 5 || string(seen.Value) != "ok" {
		t.Errorf("verifier saw %+v, want {5, ok}", seen)
	}
}

// TestVerifyRequestTokensDeny verifies that a verifier-supplied DenyToken
// surfaces as a *TokenDeniedError carrying the chosen request-error code.
func TestVerifyRequestTokensDeny(t *testing.T) {
	verifier := session.TokenVerifierFunc(func(_ context.Context, _ *session.Session, _ session.ResolvedToken) error {
		return session.DenyToken(moqt.RequestExpiredAuthToken, "token expired")
	})
	client, server := openTokenPair(t, session.WithTokenVerifier(verifier))

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseValue,
		TokenType:  1,
		TokenValue: []byte("stale"),
	})
	req, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	err = server.VerifyRequestTokens(t.Context(), req)
	if err == nil {
		t.Fatal("VerifyRequestTokens: expected denial, got nil")
	}
	var denied *session.TokenDeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("error = %v, want *TokenDeniedError", err)
	}
	if denied.RequestErrorCode() != moqt.RequestExpiredAuthToken {
		t.Errorf("RequestErrorCode = 0x%X, want RequestExpiredAuthToken", uint64(denied.RequestErrorCode()))
	}
}

// TestVerifyRequestTokensWrapsPlainError verifies that a plain (non-DenyToken)
// verifier error is normalised to RequestUnauthorized.
func TestVerifyRequestTokensWrapsPlainError(t *testing.T) {
	verifier := session.TokenVerifierFunc(func(_ context.Context, _ *session.Session, _ session.ResolvedToken) error {
		return errors.New("bad signature")
	})
	client, server := openTokenPair(t, session.WithTokenVerifier(verifier))

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseValue,
		TokenType:  1,
		TokenValue: []byte("v"),
	})
	req, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	err = server.VerifyRequestTokens(t.Context(), req)
	var denied *session.TokenDeniedError
	if !errors.As(err, &denied) {
		t.Fatalf("error = %v, want *TokenDeniedError", err)
	}
	if denied.RequestErrorCode() != moqt.RequestUnauthorized {
		t.Errorf("RequestErrorCode = 0x%X, want RequestUnauthorized", uint64(denied.RequestErrorCode()))
	}
}

// TestVerifyRequestTokensNoVerifier verifies that without a configured
// verifier, VerifyRequestTokens is a no-op (nil) even for token-bearing
// requests.
func TestVerifyRequestTokensNoVerifier(t *testing.T) {
	client, server := openTokenPair(t) // no verifier

	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseValue,
		TokenType:  1,
		TokenValue: []byte("v"),
	})
	req, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	if err := server.VerifyRequestTokens(t.Context(), req); err != nil {
		t.Errorf("VerifyRequestTokens with no verifier = %v, want nil", err)
	}
}

// TestTokenCacheAccessor verifies that the session exposes a non-nil cache
// sized from WithMaxAuthTokenCacheSize.
func TestTokenCacheAccessor(t *testing.T) {
	_, server := openTokenPair(t, session.WithMaxAuthTokenCacheSize(2048))
	cache := server.TokenCache()
	if cache == nil {
		t.Fatal("TokenCache() = nil")
	}
	if cache.MaxSize() != 2048 {
		t.Errorf("MaxSize() = %d, want 2048", cache.MaxSize())
	}
}

// TestProcessFollowupTokensRegistersAlias pins the §10.2.2 REQUEST_UPDATE
// token path: an alias REGISTERed on a follow-up REQUEST_UPDATE (routed
// through ProcessFollowupTokens by whoever reads follow-ups directly) must
// enter the cache exactly like a first-message REGISTER, so a later
// USE_ALIAS on a fresh request resolves instead of killing the session with
// UNKNOWN_AUTH_TOKEN_ALIAS.
func TestProcessFollowupTokensRegistersAlias(t *testing.T) {
	client, server := openTokenPair(t, session.WithMaxAuthTokenCacheSize(4096))

	// Request 1: a plain SUBSCRIBE establishing the stream the update rides.
	// Opened inline (not via sendSubscribeWithTokens) because the client
	// stream is needed below to send the REQUEST_UPDATE.
	sub := &message.Subscribe{RequestID: client.AllocRequestID(), Name: []byte("track")}
	streamCh := make(chan session.Stream, 1)
	go func() {
		stream, err := session.OpenRequestForTest(client, sub)
		if err != nil {
			t.Errorf("client OpenRequest: %v", err)
			close(streamCh)
			return
		}
		streamCh <- stream
	}()
	req1, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest 1: %v", err)
	}
	clientStream, okStream := <-streamCh
	if !okStream || clientStream == nil {
		t.Fatal("client stream not opened")
	}
	t.Cleanup(func() {
		clientStream.CancelRead(uint64(moqt.StreamResetCancelled))
		clientStream.CancelWrite(uint64(moqt.StreamResetCancelled))
	})

	// Client sends REQUEST_UPDATE carrying a REGISTER token; the server
	// reads the follow-up directly and routes it through
	// ProcessFollowupTokens, then acks per §10.9.
	updDone := make(chan error, 1)
	go func() {
		_, err := client.UpdateRequest(t.Context(), clientStream, message.Parameters{
			message.AuthorizationTokenParam(message.Token{
				AliasType:  message.AliasTypeRegister,
				TokenAlias: 7,
				TokenType:  9,
				TokenValue: []byte("secret"),
			}),
		})
		updDone <- err
	}()

	m, err := message.Parse(req1.Stream)
	if err != nil {
		t.Fatalf("server Parse follow-up: %v", err)
	}
	upd, ok := m.(*message.RequestUpdate)
	if !ok {
		t.Fatalf("follow-up = %T, want *message.RequestUpdate", m)
	}
	resolved, err := server.ProcessFollowupTokens(upd)
	if err != nil {
		t.Fatalf("ProcessFollowupTokens: %v", err)
	}
	if len(resolved) != 1 || resolved[0].Type != 9 || string(resolved[0].Value) != "secret" {
		t.Fatalf("resolved = %+v, want one {9, secret}", resolved)
	}
	if err := req1.Reply(&message.RequestOK{}); err != nil {
		t.Fatalf("Reply REQUEST_OK: %v", err)
	}
	if err := <-updDone; err != nil {
		t.Fatalf("UpdateRequest: %v", err)
	}

	// Request 2: USE_ALIAS must resolve to the value registered above.
	sendSubscribeWithTokens(t, client, message.Token{
		AliasType:  message.AliasTypeUseAlias,
		TokenAlias: 7,
	})
	req2, err := server.AcceptRequest(t.Context())
	if err != nil {
		t.Fatalf("AcceptRequest 2 (alias registered via REQUEST_UPDATE not honored?): %v", err)
	}
	if len(req2.Tokens) != 1 || req2.Tokens[0].Type != 9 || string(req2.Tokens[0].Value) != "secret" {
		t.Fatalf("req2 resolved token = %+v, want {9, secret}", req2.Tokens)
	}
}
