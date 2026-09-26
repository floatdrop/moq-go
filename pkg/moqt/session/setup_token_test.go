package session_test

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// AUTHORIZATION TOKEN setup option (§10.3.1.4), which carries tokens like the
// message parameter of the same name (§10.2.2).

// tokenOption encodes tok as an AUTHORIZATION TOKEN setup option.
func tokenOption(tok message.Token) wire.KVPair {
	return wire.KVPair{Type: uint64(message.SetupOptionAuthorizationToken), ByteVal: tok.Bytes()}
}

// openWithClientOptions opens a pair whose client sends extra raw SETUP options, returning the
// server's open error and the code its conn closed with, if any.
func openWithClientOptions(
	t *testing.T,
	serverOpts []session.Option,
	extra ...wire.KVPair,
) (cli, srv *session.Session, srvClose <-chan uint64, srvErr error) {
	t.Helper()
	cliConn, rawSrv := sessiontest.NewConnPair()
	srvConn := newCloseRecorder(rawSrv)
	cliOpts := make([]session.Option, 0, len(extra))
	for _, kv := range extra {
		cliOpts = append(cliOpts, session.WithSetupOptionForTest(kv))
	}
	var (
		wg   sync.WaitGroup
		cErr error
	)
	wg.Go(func() { cli, cErr = session.Client(t.Context(), cliConn, cliOpts...) })
	wg.Go(func() { srv, srvErr = session.Server(t.Context(), srvConn, serverOpts...) })
	wg.Wait()
	if cErr != nil {
		t.Fatalf("client open: %v", cErr)
	}
	t.Cleanup(func() {
		_ = cli.Close(moqt.SessionNoError, "cleanup")
		if srv != nil {
			_ = srv.Close(moqt.SessionNoError, "cleanup")
		}
	})
	return cli, srv, srvConn.code, srvErr
}

func TestSetupTokens(t *testing.T) {
	t.Parallel()
	const tokType = 7
	value := []byte("session-grant")

	t.Run("USE_VALUE is exposed", func(t *testing.T) {
		t.Parallel()
		_, srv, _, err := openWithClientOptions(t, nil,
			tokenOption(message.Token{AliasType: message.AliasTypeUseValue, TokenType: tokType, TokenValue: value}))
		if err != nil {
			t.Fatalf("server open: %v", err)
		}
		got := srv.SetupTokens()
		if len(got) != 1 || got[0].Type != tokType || string(got[0].Value) != string(value) {
			t.Fatalf("SetupTokens = %+v, want one {%d %q}", got, tokType, value)
		}
	})

	t.Run("REGISTER is cached for later requests", func(t *testing.T) {
		t.Parallel()
		cli, srv, _, err := openWithClientOptions(
			t,
			[]session.Option{session.WithMaxAuthTokenCacheSize(1024)},
			tokenOption(message.Token{
				AliasType: message.AliasTypeRegister, TokenAlias: 3, TokenType: tokType, TokenValue: value,
			}),
		)
		if err != nil {
			t.Fatalf("server open: %v", err)
		}
		if got := srv.SetupTokens(); len(got) != 1 || string(got[0].Value) != string(value) {
			t.Fatalf("SetupTokens = %+v, want the registered token", got)
		}
		// A later request refers to the token by the alias SETUP registered.
		go func() {
			_, _ = cli.Subscribe(t.Context(), &message.Subscribe{
				Namespace: wire.TrackNamespace{[]byte("ns")}, Name: []byte("t"),
				Parameters: message.Parameters{message.AuthorizationTokenParam(
					message.Token{AliasType: message.AliasTypeUseAlias, TokenAlias: 3})},
			})
		}()
		req, err := srv.AcceptRequest(t.Context())
		if err != nil {
			t.Fatalf("AcceptRequest with USE_ALIAS 3: %v", err)
		}
		if len(req.Tokens) != 1 || req.Tokens[0].Type != tokType || string(req.Tokens[0].Value) != string(value) {
			t.Fatalf("request tokens = %+v, want the one SETUP registered", req.Tokens)
		}
	})

	t.Run("REGISTER over the cache size is used as a value", func(t *testing.T) {
		t.Parallel()
		// No MAX_AUTH_TOKEN_CACHE_SIZE: the default 0 prohibits aliases, so
		// the option is treated as USE_VALUE, not a cache overflow (§10.3.1.4).
		_, srv, _, err := openWithClientOptions(
			t,
			nil,
			tokenOption(message.Token{
				AliasType: message.AliasTypeRegister, TokenAlias: 3, TokenType: tokType, TokenValue: value,
			}),
		)
		if err != nil {
			t.Fatalf("server open: %v", err)
		}
		if got := srv.SetupTokens(); len(got) != 1 || string(got[0].Value) != string(value) {
			t.Fatalf("SetupTokens = %+v, want the token as a value", got)
		}
		if _, _, err := srv.TokenCache().Resolve(3); err == nil {
			t.Fatal("an alias the cache could not hold was registered anyway")
		}
	})
}

// TestSetupTokenViolations: tokens in a received SETUP that close the session.
func TestSetupTokenViolations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts []wire.KVPair
		want moqt.SessionErrorCode
	}{
		// §10.2.2: DELETE or USE_ALIAS in SETUP is a PROTOCOL_VIOLATION.
		{"DELETE", []wire.KVPair{tokenOption(message.Token{AliasType: message.AliasTypeDelete, TokenAlias: 1})},
			moqt.SessionProtocolViolation},
		{"USE_ALIAS", []wire.KVPair{tokenOption(message.Token{AliasType: message.AliasTypeUseAlias, TokenAlias: 1})},
			moqt.SessionProtocolViolation},
		// §10.2.2: an undecodable Token is a KEY_VALUE_FORMATTING_ERROR.
		{"undecodable", []wire.KVPair{{Type: uint64(message.SetupOptionAuthorizationToken), ByteVal: []byte{0x09}}},
			moqt.SessionKeyValueFormattingError},
		// §10.2.2: registering an Alias twice is DUPLICATE_AUTH_TOKEN_ALIAS.
		{"duplicate REGISTER", []wire.KVPair{tokenOption(register(1, "a")), tokenOption(register(1, "b"))},
			moqt.SessionDuplicateAuthTokenAlias},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, closed, err := openWithClientOptions(t,
				[]session.Option{session.WithMaxAuthTokenCacheSize(1024)}, tc.opts...)
			if err == nil {
				t.Fatal("server opened a session whose SETUP carried an invalid token")
			}
			requireClosedWith(t, closed, tc.want)
		})
	}
}

// ---------------------------------------------------------------------------
// Send side: a REGISTER the peer's MAX_AUTH_TOKEN_CACHE_SIZE cannot hold is
// used as a value, so the sender purges it (§10.3.1.4).
// ---------------------------------------------------------------------------

// register builds a REGISTER token of Token Type 1.
func register(alias uint64, value string) message.Token {
	return message.Token{
		AliasType:  message.AliasTypeRegister,
		TokenAlias: alias,
		TokenType:  1,
		TokenValue: []byte(value),
	}
}

// TestSetupTokensSent: the tokens reach the peer, and the sender learns which
// REGISTERs the peer holds — those that fit its cache, in SETUP order.
func TestSetupTokensSent(t *testing.T) {
	small := register(1, "0123456789")             // 16+10 = 26 bytes
	large := register(2, strings.Repeat("x", 100)) // 116 bytes: does not fit
	tiny := register(3, "ab")                      // 18 bytes: fits after 1
	value := message.Token{AliasType: message.AliasTypeUseValue, TokenType: 1, TokenValue: []byte("v")}
	client, server := openPairWithOpts(t,
		[]session.Option{
			session.WithSetupToken(small), session.WithSetupToken(large),
			session.WithSetupToken(tiny), session.WithSetupToken(value),
		},
		[]session.Option{session.WithMaxAuthTokenCacheSize(26 + 18)},
	)

	if got := client.SetupTokenAliases(); !slices.Equal(got, []uint64{1, 3}) {
		t.Fatalf("SetupTokenAliases() = %v, want [1 3]: alias 2 did not fit the peer's cache", got)
	}
	if got := len(server.SetupTokens()); got != 4 {
		t.Fatalf("server received %d setup tokens, want 4", got)
	}
	for alias, want := range map[uint64]bool{1: true, 2: false, 3: true} {
		_, _, err := server.TokenCache().Resolve(alias)
		if (err == nil) != want {
			t.Errorf("server cache holds alias %d: %v, want %v", alias, err == nil, want)
		}
	}
}

// TestSetupTokensDefaultCacheIsZero: a peer that advertises no cache size has
// the default 0, so no REGISTER is held.
func TestSetupTokensDefaultCacheIsZero(t *testing.T) {
	client, _ := openPairWithOpts(t, []session.Option{session.WithSetupToken(register(1, "v"))}, nil)
	if got := client.SetupTokenAliases(); len(got) != 0 {
		t.Fatalf("SetupTokenAliases() = %v, want none: the peer's cache size defaults to 0", got)
	}
}

// TestSetupTokenRefused: the client refuses to send a SETUP token the server
// must close on (DELETE, USE_ALIAS, a repeated alias; §10.2.2).
func TestSetupTokenRefused(t *testing.T) {
	for name, toks := range map[string][]message.Token{
		"DELETE":          {{AliasType: message.AliasTypeDelete, TokenAlias: 1}},
		"USE_ALIAS":       {{AliasType: message.AliasTypeUseAlias, TokenAlias: 1}},
		"duplicate alias": {register(1, "a"), register(1, "b")},
	} {
		t.Run(name, func(t *testing.T) {
			var opts []session.Option
			for _, tok := range toks {
				opts = append(opts, session.WithSetupToken(tok))
			}
			requireRefusedOpen(t, "AUTHORIZATION TOKEN", func(ctx context.Context) (*session.Session, error) {
				clientConn, _ := sessiontest.NewConnPair()
				return session.Client(ctx, clientConn, opts...)
			})
		})
	}
}
