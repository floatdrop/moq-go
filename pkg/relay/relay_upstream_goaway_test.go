package relay

import (
	"context"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// TestResolveUpstreamsSkipsGoingAwayRelay: a pooled session to a remote relay
// that sent GOAWAY takes no new requests (§10.4), so it must not take one of
// the UpstreamFanIn slots either; resolution falls through to the next-ranked
// relay while the draining one is still listed in Discovery.
func TestResolveUpstreamsSkipsGoingAwayRelay(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	ns := wire.TrackNamespace{[]byte("video")}
	store := discovery.NewMemoryStore()
	defer store.Close()
	addrs := []string{"relay-B", "relay-C"}
	for _, a := range addrs {
		if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{Prefix: ns, RelayAddr: a}); err != nil {
			t.Fatalf("PublishNamespace %s: %v", a, err)
		}
	}

	// Each remote relay is a bare server session the test drives.
	var (
		mu      sync.Mutex
		remotes = map[string]*session.Session{}
	)
	dialer := func(_ context.Context, addr string) (session.Conn, error) {
		cli, srv := sessiontest.NewConnPair()
		go func() {
			// Not the dial's context: the pool cancels it once the dial
			// returns, which can be before this side of the handshake ends.
			s, err := session.Server(t.Context(), srv)
			if err != nil {
				return
			}
			mu.Lock()
			remotes[addr] = s
			mu.Unlock()
		}()
		return cli, nil
	}
	p := newUpstreamPool(upstreamPoolConfig{
		dialer: dialer, discovery: store, relayAddr: "relay-A", log: slog.Default(), fanIn: 1,
		serveSession: func(*session.Session, func()) {},
	})
	defer p.close()

	first := p.resolveUpstreams(ctx, ns)
	if len(first) != 1 {
		t.Fatalf("resolveUpstreams = %d sessions, want 1 (fan-in 1)", len(first))
	}
	top := rankedAddrs(ns, addrs)[0]
	var remote *session.Session
	for deadline := time.Now().Add(2 * time.Second); remote == nil; time.Sleep(5 * time.Millisecond) {
		// The server side of the handshake can finish after the client's.
		mu.Lock()
		remote = remotes[top]
		mu.Unlock()
		if remote == nil && time.Now().After(deadline) {
			t.Fatalf("no server session for the top-ranked %s", top)
		}
	}
	if err := remote.SendGoaway(10*time.Second, ""); err != nil {
		t.Fatalf("SendGoaway: %v", err)
	}
	select {
	case <-first[0].GoawayReceived():
	case <-time.After(2 * time.Second):
		t.Fatal("the pooled session never saw the GOAWAY")
	}

	second := p.resolveUpstreams(ctx, ns)
	if len(second) != 1 || second[0] == first[0] {
		t.Fatalf("resolveUpstreams after GOAWAY returned the draining %s again; want the next-ranked relay", top)
	}
	_ = remote.Close(moqt.SessionNoError, "done")
}
