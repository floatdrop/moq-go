package relay_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// setupTokenRecorder records the SETUP tokens (§9.1.4) of every session that
// SUBSCRIBEs through it.
type setupTokenRecorder struct {
	relay.AllowAllAuthorizer

	mu   sync.Mutex
	seen [][]session.ResolvedToken
}

func (a *setupTokenRecorder) AuthorizeSubscribe(_ context.Context, sess *session.Session, _ *message.Subscribe) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = append(a.seen, sess.SetupTokens())
	return nil
}

// TestCrossRelay_UpstreamSessionOptions: what Config.UpstreamSessionOptions
// returns for a peer reaches the session the relay dials to it and no session
// it accepts, so a SETUP AUTHORIZATION TOKEN placed there identifies the relay
// to its peers without being handed to its clients.
func TestCrossRelay_UpstreamSessionOptions(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()

	const tokType, tokValue = 7, "relay-A-credential"
	rec := &setupTokenRecorder{}
	var dialed atomic.Value
	relayB := startTestRelay(t.Context(), relay.Config{
		Discovery: store, RelayAddr: "relay-B", Authorizer: rec,
	})
	relayA := startTestRelay(t.Context(), relay.Config{
		Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(nil, relayB),
		UpstreamSessionOptions: func(relayAddr string) []session.Option {
			dialed.Store(relayAddr)
			return []session.Option{session.WithSetupToken(message.Token{
				AliasType: message.AliasTypeUseValue, TokenType: tokType, TokenValue: []byte(tokValue),
			})}
		},
	})

	pubSess, _ := publishOnRelay(t, relayB, "cam1", 7)
	subSess := dialClient(t, relayA)
	subscribeCam1(t, subSess)

	rec.mu.Lock()
	seen := rec.seen
	rec.mu.Unlock()
	if len(seen) != 1 {
		t.Fatalf("relay-B authorized %d SUBSCRIBEs, want 1 (relay-A's upstream)", len(seen))
	}
	if toks := seen[0]; len(toks) != 1 || toks[0].Type != tokType || string(toks[0].Value) != tokValue {
		t.Fatalf("relay-A's upstream session SETUP tokens = %+v, want one {%d %q}", toks, tokType, tokValue)
	}
	// The option is for the dialing side only: a client relay-A accepted got
	// no token in its SETUP.
	if toks := subSess.SetupTokens(); len(toks) != 0 {
		t.Fatalf("client received SETUP tokens %+v from relay-A, want none", toks)
	}
	if got := dialed.Load(); got != "relay-B" {
		t.Fatalf("UpstreamSessionOptions called with %v, want the dialed relay's address", got)
	}

	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}
