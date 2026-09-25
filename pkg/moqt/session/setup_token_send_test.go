package session_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// §10.3.1.4, the send side: "The endpoint can specify one or more tokens in
// SETUP that the peer can use to authorize MOQT session establishment." A
// REGISTER the peer's MAX_AUTH_TOKEN_CACHE_SIZE cannot hold is treated by the
// peer as USE_VALUE, and "the sender MUST handle registration failures of this
// kind by purging any Token Aliases that failed to register based on the
// peer's MAX_AUTH_TOKEN_CACHE_SIZE option in SETUP (or the default value of
// 0)."

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

// TestSetupTokenRefused: DELETE and USE_ALIAS have no meaning in SETUP (a
// server that receives one "MUST close the session with a
// PROTOCOL_VIOLATION", §10.2.2), and a repeated REGISTER alias would close it
// with DUPLICATE_AUTH_TOKEN_ALIAS. The open fails before anything is sent.
func TestSetupTokenRefused(t *testing.T) {
	for name, toks := range map[string][]message.Token{
		"DELETE":          {{AliasType: message.AliasTypeDelete, TokenAlias: 1}},
		"USE_ALIAS":       {{AliasType: message.AliasTypeUseAlias, TokenAlias: 1}},
		"duplicate alias": {register(1, "a"), register(1, "b")},
	} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			var opts []session.Option
			for _, tok := range toks {
				opts = append(opts, session.WithSetupToken(tok))
			}
			clientConn, _ := sessiontest.NewConnPair()
			sess, err := session.Client(ctx, clientConn, opts...)
			if err == nil {
				_ = sess.Close(moqt.SessionNoError, "test cleanup")
				t.Fatal("the client opened with a setup token the peer must refuse")
			}
			if !strings.Contains(err.Error(), "AUTHORIZATION TOKEN") {
				t.Errorf("error %q does not name the AUTHORIZATION TOKEN option", err)
			}
		})
	}
}
