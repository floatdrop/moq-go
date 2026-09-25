package session_test

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §10.3.1.4: the AUTHORIZATION TOKEN setup option is "functionally equivalent
// to the AUTHORIZATION TOKEN message parameter" (§10.2.2), carrying tokens
// "that the peer can use to authorize MOQT session establishment".

// tokenOption encodes tok as an AUTHORIZATION TOKEN setup option.
func tokenOption(tok message.Token) wire.KVPair {
	return wire.KVPair{Type: uint64(message.SetupOptionAuthorizationToken), ByteVal: tok.Bytes()}
}

// closeRecorder records the code its session closes the connection with,
// which sessiontest does not pass to the peer.
type closeRecorder struct {
	session.Conn

	code chan uint64
}

func (c *closeRecorder) CloseWithError(code uint64, reason string) error {
	select {
	case c.code <- code:
	default:
	}
	return c.Conn.CloseWithError(code, reason)
}

// openWithClientOptions opens a pair whose client sends extra SETUP options,
// and returns the server's open error alongside the sessions and the code the
// server closed its connection with, if it did.
func openWithClientOptions(
	t *testing.T,
	serverOpts []session.Option,
	extra ...wire.KVPair,
) (cli, srv *session.Session, srvClose <-chan uint64, srvErr error) {
	t.Helper()
	cliConn, rawSrv := sessiontest.NewConnPair()
	srvConn := &closeRecorder{Conn: rawSrv, code: make(chan uint64, 1)}
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

// requireClosedWith checks the code a connection was closed with.
func requireClosedWith(t *testing.T, closed <-chan uint64, want moqt.SessionErrorCode) {
	t.Helper()
	select {
	case code := <-closed:
		if code != uint64(want) {
			t.Fatalf("closed with code %#x, want %#x", code, uint64(want))
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("connection stayed open; want close with %#x", uint64(want))
	}
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
			tokenOption(
				message.Token{
					AliasType:  message.AliasTypeRegister,
					TokenAlias: 3,
					TokenType:  tokType,
					TokenValue: value,
				},
			),
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
		// No MAX_AUTH_TOKEN_CACHE_SIZE: the default 0 prohibits aliases.
		// §10.3.1.4: the receiver "MUST NOT fail the session with
		// AUTH_TOKEN_CACHE_OVERFLOW. Instead, it MUST treat the option as
		// Alias Type USE_VALUE."
		_, srv, _, err := openWithClientOptions(
			t,
			nil,
			tokenOption(
				message.Token{
					AliasType:  message.AliasTypeRegister,
					TokenAlias: 3,
					TokenType:  tokType,
					TokenValue: value,
				},
			),
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

// TestSetupTokenViolations: tokens in SETUP that close the session.
func TestSetupTokenViolations(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		opts []wire.KVPair
		want moqt.SessionErrorCode
	}{
		// §10.2.2: "If a server receives Alias Type DELETE (0x0) or USE_ALIAS
		// (0x2) in a SETUP message, it MUST close the session with a
		// PROTOCOL_VIOLATION."
		{"DELETE", []wire.KVPair{tokenOption(message.Token{AliasType: message.AliasTypeDelete, TokenAlias: 1})},
			moqt.SessionProtocolViolation},
		{"USE_ALIAS", []wire.KVPair{tokenOption(message.Token{AliasType: message.AliasTypeUseAlias, TokenAlias: 1})},
			moqt.SessionProtocolViolation},
		// "If the Token structure cannot be decoded, the receiver MUST close
		// the Session with KEY_VALUE_FORMATTING_ERROR."
		{"undecodable", []wire.KVPair{{Type: uint64(message.SetupOptionAuthorizationToken), ByteVal: []byte{0x09}}},
			moqt.SessionKeyValueFormattingError},
		// "The receiver of a message attempting to register an Alias which is
		// already registered MUST close the Session with
		// DUPLICATE_AUTH_TOKEN_ALIAS."
		{
			"duplicate REGISTER",
			[]wire.KVPair{
				tokenOption(
					message.Token{
						AliasType:  message.AliasTypeRegister,
						TokenAlias: 1,
						TokenType:  1,
						TokenValue: []byte("a"),
					},
				),
				tokenOption(
					message.Token{
						AliasType:  message.AliasTypeRegister,
						TokenAlias: 1,
						TokenType:  1,
						TokenValue: []byte("b"),
					},
				),
			},
			moqt.SessionDuplicateAuthTokenAlias,
		},
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
