package relay_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// Cross-relay routing: which remote relays a SUBSCRIBE with no local upstream
// is sent to, found through Discovery and reached through the Dialer.

// dialLog counts a Dialer's dials per address.
type dialLog struct {
	mu sync.Mutex
	n  map[string]int
}

func (d *dialLog) record(addr string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.n == nil {
		d.n = map[string]int{}
	}
	d.n[addr]++
}

func (d *dialLog) counts() map[string]int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return maps.Clone(d.n)
}

// TestCrossRelay_OnDemandSubscribe: a subscriber on relay A receives Objects
// published to relay B, which A finds through Discovery and dials.
func TestCrossRelay_OnDemandSubscribe(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)

	pubSess, _ := publishOnRelay(t, relayB, "cam1", 7)
	// Subscribe returns only once A's upstream to B, and B's to the publisher,
	// are established, so the whole chain is live before anything is written.
	subSess := dialClient(t, relayA)
	subReq := subscribeCam1(t, subSess)

	reads := readNextSubgroup(t, subSess)
	const sgCount = 5
	publishObjects(t, pubSess, 7, 0, sgCount)
	r := awaitSubgroupRead(t, reads)
	if len(r.ids) != sgCount {
		t.Fatalf("subscriber received %d objects, want %d", len(r.ids), sgCount)
	}
	if r.header.TrackAlias != subReq.OK.TrackAlias {
		t.Errorf("subgroup TrackAlias = %d, want %d (subscriber's outbound alias)",
			r.header.TrackAlias, subReq.OK.TrackAlias)
	}

	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}

// TestCrossRelay_LocalPublisherFailureFallsBackToDiscovery: when the local
// publisher's upstream SUBSCRIBE fails, the relay still falls back to a remote
// relay found through Discovery.
func TestCrossRelay_LocalPublisherFailureFallsBackToDiscovery(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)

	pubB, _ := publishOnRelay(t, relayB, "cam1", 9)

	// A local publisher of the same namespace on A refuses every upstream
	// SUBSCRIBE, so A must try it, fail, and fall back to B.
	pLocal := dialClient(t, relayA)
	publishNS(t, pLocal, "video")
	rejectDone := make(chan struct{})
	go func() {
		defer close(rejectDone)
		for {
			req, err := pLocal.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			_ = req.RejectError(moqt.RequestDoesNotExist, "local publisher declines")
		}
	}()

	subSess := dialClient(t, relayA)
	subscribeCam1(t, subSess)

	reads := readNextSubgroup(t, subSess)
	const sgCount = 3
	publishObjects(t, pubB, 9, 0, sgCount)
	if r := awaitSubgroupRead(t, reads); len(r.ids) != sgCount {
		t.Fatalf("received %d objects via Discovery fallback, want %d", len(r.ids), sgCount)
	}

	_ = subSess.Close(0, "done")
	_ = pLocal.Close(0, "done")
	_ = pubB.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
	<-rejectDone
}

// TestCrossRelay_MultiRemoteFanIn: with two remote relays advertising a
// namespace, relay A subscribes to both (§9.5) and delivers each Object once (§9.3,
// §2.1).
func TestCrossRelay_MultiRemoteFanIn(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayC := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-C"})
	var dials dialLog
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(dials.record, relayB, relayC),
	})

	// A redundant publisher of the same track on each of B and C.
	pubBSess, _ := publishOnRelay(t, relayB, "cam1", 7)
	pubCSess, _ := publishOnRelay(t, relayC, "cam1", 7)

	// Subscribe returns only once A has established both upstreams.
	subSess := dialClient(t, relayA)
	subscribeCam1(t, subSess)
	if got, want := dials.counts(), map[string]int{"relay-B": 1, "relay-C": 1}; !maps.Equal(got, want) {
		t.Errorf("Dialer calls %v, want %v (dial-all)", got, want)
	}

	events := make(chan objEvent, 64)
	go readSubgroups(ctx, subSess, events)

	// Both remotes push the same Objects 0,1,2 on the same (group, subgroup).
	publishObjects(t, pubBSess, 7, 0, 3)
	publishObjects(t, pubCSess, 7, 0, 3)

	// Each of 0,1,2 must arrive exactly once across however many outbound
	// streams the merge produced; collect until 500ms of quiet.
	seen := map[uint64]int{}
	hard := time.After(3 * time.Second)
collect:
	for {
		select {
		case ev := <-events:
			if ev.err == nil {
				seen[ev.absID]++
			}
		case <-time.After(500 * time.Millisecond):
			break collect
		case <-hard:
			break collect
		}
	}
	if want := map[uint64]int{0: 1, 1: 1, 2: 1}; !maps.Equal(seen, want) {
		t.Fatalf("delivered Object counts %v across two remotes, want each of 0,1,2 exactly once (dedup)", seen)
	}

	_ = subSess.Close(0, "done")
	_ = pubBSess.Close(0, "done")
	_ = pubCSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
	relayC.stop(t)
}

// TestCrossRelay_SelfExclusion: a Discovery entry naming this relay's own
// RelayAddr is never dialled; the subscriber is refused.
func TestCrossRelay_SelfExclusion(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()

	var dials atomic.Int64
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store,
		RelayAddr: "relay-A",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			dials.Add(1)
			return nil, fmt.Errorf("unexpected dial to %q", addr)
		},
	})
	if err := store.PublishNamespace(
		ctx,
		discovery.NamespaceInfo{Prefix: ns("video"), RelayAddr: "relay-A"},
	); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}

	subSess := dialClient(t, relayA)
	if _, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")}); err == nil {
		t.Fatal("Subscribe succeeded; want rejection (no remote relay, self excluded)")
	}
	if got := dials.Load(); got != 0 {
		t.Errorf("Dialer fired %d times; want 0 (self must not be dialled)", got)
	}

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_PoolReuse: SUBSCRIBEs to two tracks on the same remote relay
// share one dialled session.
func TestCrossRelay_PoolReuse(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	var dials dialLog
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(dials.record, relayB),
	})

	pubSess, _ := publishOnRelay(t, relayB, "cam1", 1)
	publishVideoTrack(t, pubSess, "cam2", 2)

	subSess := dialClient(t, relayA)
	subscribeCam1(t, subSess)
	if _, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: ns("video"), Name: []byte("cam2")}); err != nil {
		t.Fatalf("Subscribe cam2: %v", err)
	}
	if got, want := dials.counts(), map[string]int{"relay-B": 1}; !maps.Equal(got, want) {
		t.Errorf("Dialer calls %v for two tracks on one relay, want %v (pool reuse)", got, want)
	}

	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}

// TestCrossRelay_DialerWithoutRelayAddrWarns: New warns when a Dialer is set
// without a RelayAddr, and not when one is set.
func TestCrossRelay_DialerWithoutRelayAddrWarns(t *testing.T) {
	t.Parallel()
	dialer := func(_ context.Context, _ string) (session.Conn, error) {
		return nil, errors.New("unused")
	}
	newWith := func(relayAddr string) string {
		var buf bytes.Buffer
		logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelWarn}))
		store := discovery.NewMemoryStore()
		defer store.Close()
		// New logs synchronously and starts no goroutines.
		_ = relay.New(newPipeListener(), relay.Config{
			Discovery: store, RelayAddr: relayAddr, Dialer: dialer, Logger: logger,
		})
		return buf.String()
	}

	if out := newWith(""); !strings.Contains(out, "RelayAddr") {
		t.Errorf("empty RelayAddr + Dialer should warn about RelayAddr; got %q", out)
	}
	if out := newWith("relay-A"); strings.Contains(out, "RelayAddr") {
		t.Errorf("RelayAddr set should not warn; got %q", out)
	}
}

// TestCrossRelay_NoDialerNoop: with Discovery but no Dialer, a SUBSCRIBE with
// no local publisher is refused exactly as on a single-instance relay.
func TestCrossRelay_NoDialerNoop(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()

	// A remote namespace the relay must not try to use without a Dialer.
	if err := store.PublishNamespace(
		ctx,
		discovery.NamespaceInfo{Prefix: ns("video"), RelayAddr: "relay-B"},
	); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	_, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
	if err == nil {
		t.Fatal("Subscribe succeeded; want rejection (no Dialer, no local publisher)")
	}
	if _, ok := errors.AsType[*session.RequestRejectedError](err); !ok {
		t.Logf("Subscribe error (non-RequestRejectedError is acceptable): %v", err)
	}

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_UpstreamFanInCapConverges: with UpstreamFanIn 1 and three
// remotes, a leaf relay subscribes to exactly one, and two leaves pick the same
// one.
func TestCrossRelay_UpstreamFanInCapConverges(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()

	var remotes []*testRelay
	var pubSessions []*session.Session
	for _, addr := range []string{"relay-B", "relay-C", "relay-D"} {
		tr := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: addr})
		remotes = append(remotes, tr)
		// Every remote hosts cam1, so whichever the leaves converge on can
		// establish the upstream.
		ps, _ := publishOnRelay(t, tr, "cam1", 7)
		pubSessions = append(pubSessions, ps)
	}

	// Each leaf's log holds only dials into known remotes: resolveUpstreams
	// skips an unreachable candidate (such as the other leaf) and falls
	// through, so it is exactly the leaf's established upstreams.
	var logA1, logA2 dialLog
	relayA1 := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A1", UpstreamFanIn: 1, Dialer: dialerTo(logA1.record, remotes...),
	})
	relayA2 := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A2", UpstreamFanIn: 1, Dialer: dialerTo(logA2.record, remotes...),
	})

	// Subscribe blocks until the upstream is established, so the logs are
	// settled when it returns.
	sub1 := dialClient(t, relayA1)
	subscribeCam1(t, sub1)
	sub2 := dialClient(t, relayA2)
	subscribeCam1(t, sub2)

	a1, a2 := logA1.counts(), logA2.counts()
	for leaf, log := range map[string]map[string]int{"A1": a1, "A2": a2} {
		if len(log) != 1 || slices.Max(slices.Collect(maps.Values(log))) != 1 {
			t.Errorf("%s dialed %v; want exactly one upstream, once (UpstreamFanIn=1)", leaf, log)
		}
	}
	if len(a1) == 1 && len(a2) == 1 && !maps.Equal(a1, a2) {
		t.Errorf("leaves diverged: A1 dialed %v, A2 dialed %v; rendezvous ranking must converge", a1, a2)
	}

	_ = sub1.Close(0, "done")
	_ = sub2.Close(0, "done")
	for _, ps := range pubSessions {
		_ = ps.Close(0, "done")
	}
	relayA1.stop(t)
	relayA2.stop(t)
	for _, tr := range remotes {
		tr.stop(t)
	}
}
