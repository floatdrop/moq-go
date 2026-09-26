package relay

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// rankedAddrs runs rankByAffinity over addrs (as NamespaceInfo candidates) and
// returns the resulting RelayAddr order — the reduction every relay applies to
// pick its top-fanIn upstreams for ns.
func rankedAddrs(ns wire.TrackNamespace, addrs []string) []string {
	infos := make([]discovery.NamespaceInfo, len(addrs))
	for i, a := range addrs {
		infos[i] = discovery.NamespaceInfo{Prefix: ns, RelayAddr: a}
	}
	rankByAffinity(ns, infos)
	out := make([]string, len(infos))
	for i, info := range infos {
		out[i] = info.RelayAddr
	}
	return out
}

// TestRankByAffinityConverges: the ranking depends only on the namespace and
// candidate set, so relays agree on it whatever order they learned it in.
func TestRankByAffinityConverges(t *testing.T) {
	t.Parallel()

	ns := wire.Namespace("sports", "game7")
	want := rankedAddrs(ns, []string{"relay-a", "relay-b", "relay-c", "relay-d", "relay-e"})

	// Every candidate must survive the sort exactly once (no drops/dupes).
	if len(want) != 5 {
		t.Fatalf("ranked %d candidates, want 5: %v", len(want), want)
	}

	// Feeding the same set in any order must yield the identical ranking.
	perms := [][]string{
		{"relay-e", "relay-d", "relay-c", "relay-b", "relay-a"},
		{"relay-c", "relay-a", "relay-e", "relay-b", "relay-d"},
		{"relay-b", "relay-e", "relay-a", "relay-d", "relay-c"},
	}
	for _, p := range perms {
		if got := rankedAddrs(ns, p); !slices.Equal(got, want) {
			t.Errorf("input order %v ranked as %v; want %v (ranking must be order-independent)", p, got, want)
		}
	}
}

// TestRankByAffinitySpreads: distinct namespaces land on more than one top
// relay.
func TestRankByAffinitySpreads(t *testing.T) {
	t.Parallel()

	addrs := []string{"relay-a", "relay-b", "relay-c", "relay-d", "relay-e"}
	tops := map[string]int{}
	for i := range 50 {
		ns := wire.Namespace("broadcast", fmt.Sprintf("ch-%d", i))
		tops[rankedAddrs(ns, addrs)[0]]++
	}
	if len(tops) < 2 {
		t.Fatalf("all 50 namespaces mapped to a single top relay %v; weight is not mixing the address", tops)
	}
}

// TestRankByAffinitySubsetOrderStable: removing a candidate does not reorder
// the rest.
func TestRankByAffinitySubsetOrderStable(t *testing.T) {
	t.Parallel()

	ns := wire.Namespace("live", "cam1")
	full := rankedAddrs(ns, []string{"relay-a", "relay-b", "relay-c", "relay-d"})

	// Drop the current top relay; the remaining three must keep their order.
	withoutTop := rankedAddrs(ns, []string{"relay-b", "relay-c", "relay-d", "relay-a"})
	withoutTop = slices.DeleteFunc(withoutTop, func(a string) bool { return a == full[0] })
	if want := full[1:]; !slices.Equal(withoutTop, want) {
		t.Errorf("subset ranked %v; want %v (dropping a relay must not reorder the rest)", withoutTop, want)
	}
}

// TestNewUpstreamPoolFanInPassthrough: UpstreamFanIn is kept verbatim; zero or
// negative means unbounded, the full fan-in of §9.5.
func TestNewUpstreamPoolFanInPassthrough(t *testing.T) {
	t.Parallel()

	for _, in := range []int{0, -1, 1, 2, 5} {
		p := newUpstreamPool(upstreamPoolConfig{log: slog.Default(), fanIn: in})
		if p.fanIn != in {
			t.Errorf("newUpstreamPool stored fanIn %d; want %d (verbatim)", p.fanIn, in)
		}
		p.close()
	}
}

// TestResolveUpstreamsSkipsGoingAwayRelay: a pooled session whose relay sent
// GOAWAY (§10.4) takes no UpstreamFanIn slot; resolution falls through to the
// next-ranked relay.
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
