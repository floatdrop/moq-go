package relay

import (
	"fmt"
	"log/slog"
	"slices"
	"testing"

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

// TestRankByAffinityConverges is the property the whole scheme rests on: the
// ranking is a pure function of (namespace, candidate set), so relays that
// receive the advertisements in different orders still compute the same order —
// and therefore agree on the top-fanIn upstreams.
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

// TestRankByAffinitySpreads guards against a degenerate hash: if the address
// were left out of the weight, every namespace would tie and fall back to the
// same alphabetically-first relay. Distinct namespaces must land on more than
// one top relay.
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

// TestRankByAffinitySubsetOrderStable pins the subset-order-preservation that
// keeps two leaves convergent even when their candidate sets differ by an
// unreachable entry: removing any relay must not reorder the rest, because each
// weight is independent of the others present.
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

// TestNewUpstreamPoolFanInPassthrough pins that UpstreamFanIn is carried
// verbatim: the pool applies no default, because zero (and any negative) already
// means "unbounded" — the §9.5 full fan-in — which resolveUpstreams enforces via
// its `fanIn > 0` guard rather than by normalizing the value here.
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
