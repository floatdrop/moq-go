package relay_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// testRelay is a relay started on its own in-process pipeListener, used by the
// cross-relay tests that need direct control over two relay instances and a
// Dialer wiring one to the other.
type testRelay struct {
	r        *relay.Relay
	l        *pipeListener
	startErr chan error
}

func startTestRelay(ctx context.Context, cfg relay.Config) *testRelay {
	if cfg.GoawayTimeout == 0 {
		cfg.GoawayTimeout = 50 * time.Millisecond
	}
	l := newPipeListener()
	r := relay.New(l, cfg)
	se := make(chan error, 1)
	go func() { se <- r.Start(ctx) }()
	return &testRelay{r: r, l: l, startErr: se}
}

func (tr *testRelay) stop(t *testing.T) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tr.r.Stop(ctx)
	select {
	case err := <-tr.startErr:
		if err != nil {
			t.Errorf("Start returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start did not return after Stop")
	}
}

// dialClient connects a fresh client session into tr's listener.
func dialClient(t *testing.T, tr *testRelay) *session.Session {
	t.Helper()
	conn, err := tr.l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sess, err := session.Client(t.Context(), conn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	return sess
}

func videoNS() wire.TrackNamespace { return wire.TrackNamespace{[]byte("video")} }

// TestCrossRelay_OnDemandSubscribe is the end-to-end happy path: a subscriber
// on relay A receives objects published to relay B, routed across the boundary
// purely through Discovery + the Dialer. B advertises the "video" namespace;
// A has no local publisher, follows FindNamespace to B, dials it, and
// subscribes upstream. Objects flow publisher → B → A → subscriber.
func TestCrossRelay_OnDemandSubscribe(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store,
		RelayAddr: "relay-A",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr == "relay-B" {
				return relayB.l.Dial()
			}
			return nil, fmt.Errorf("no relay at %q", addr)
		},
	})

	// Publisher connects to B, advertises the namespace (so FindNamespace can
	// route here) and PUBLISHes the track (so B has an established upstream).
	pubSess := dialClient(t, relayB)
	pns, err := pubSess.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	const pubAlias = uint64(7)
	pubReq, err := pubSess.Publish(ctx, &message.Publish{
		Namespace:  videoNS(),
		Name:       []byte("cam1"),
		TrackAlias: pubAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Subscriber connects to A and subscribes. Subscribe returns only after A
	// has established its upstream to B (which established B's upstream to the
	// publisher), so the full chain is live by the time we push objects.
	subSess := dialClient(t, relayA)
	subReq, err := subSess.Subscribe(ctx, &message.Subscribe{
		Namespace: videoNS(),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("cross-relay Subscribe: %v", err)
	}

	type subgroupResult struct {
		header  message.SubgroupHeader
		objects []*message.SubgroupObject
	}
	subgroupCh := make(chan subgroupResult, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		var objs []*message.SubgroupObject
		for {
			obj, err := sg.ReadObject()
			if err != nil {
				subgroupCh <- subgroupResult{header: sg.Header, objects: objs}
				return
			}
			objs = append(objs, obj)
		}
	}()

	pubSg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     pubAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	const sgCount = 5
	for i := range sgCount {
		if err := pubSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := pubSg.Close(); err != nil {
		t.Fatalf("pubSg.Close: %v", err)
	}

	select {
	case res := <-subgroupCh:
		if len(res.objects) != sgCount {
			t.Fatalf("subscriber received %d objects, want %d", len(res.objects), sgCount)
		}
		if res.header.TrackAlias != subReq.OK.TrackAlias {
			t.Errorf("subgroup TrackAlias = %d, want %d (subscriber's outbound alias)",
				res.header.TrackAlias, subReq.OK.TrackAlias)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("objects did not cross the relay boundary within deadline")
	}

	// Teardown: close clients, then stop A (tears down its upstream to B),
	// then B.
	_ = subReq.Close()
	_ = pubSg.Close()
	_ = pubReq.Close()
	_ = pns.Close()
	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}

// TestCrossRelay_LocalPublisherFailureFallsBackToDiscovery pins that a local
// publisher whose upstream SUBSCRIBE fails does not abort the search: the relay
// still falls back to a remote relay via Discovery. Without that, a transiently
// failing local publisher would reject the downstream SUBSCRIBE even though a
// healthy remote serves the track.
func TestCrossRelay_LocalPublisherFailureFallsBackToDiscovery(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store,
		RelayAddr: "relay-A",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr == "relay-B" {
				return relayB.l.Dial()
			}
			return nil, fmt.Errorf("no relay at %q", addr)
		},
	})

	// Healthy publisher on B serves video/cam1.
	pubB := dialClient(t, relayB)
	pnsB, err := pubB.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()})
	if err != nil {
		t.Fatalf("B PublishNamespace: %v", err)
	}
	const pubAlias = uint64(9)
	pubReqB, err := pubB.Publish(
		ctx,
		&message.Publish{Namespace: videoNS(), Name: []byte("cam1"), TrackAlias: pubAlias},
	)
	if err != nil {
		t.Fatalf("B Publish: %v", err)
	}

	// A local publisher on A advertises the same namespace but REJECTS every
	// upstream SUBSCRIBE — the relay must try it, fail, then fall back to B.
	pLocal := dialClient(t, relayA)
	pnsLocal, err := pLocal.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()})
	if err != nil {
		t.Fatalf("local PublishNamespace: %v", err)
	}
	rejectDone := make(chan struct{})
	go func() {
		defer close(rejectDone)
		for {
			req, err := pLocal.AcceptRequest(ctx)
			if err != nil {
				return
			}
			_ = req.RejectError(moqt.RequestDoesNotExist, "local publisher declines")
		}
	}()

	// Subscriber on A: the local publisher rejects, so A must reach B.
	subSess := dialClient(t, relayA)
	subReq, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("Subscribe should have fallen back to Discovery, got: %v", err)
	}

	objects := make(chan int, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		n := 0
		for {
			if _, err := sg.ReadObject(); err != nil {
				objects <- n
				return
			}
			n++
		}
	}()

	sgB, err := pubB.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     pubAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	const sgCount = 3
	for i := range sgCount {
		if err := sgB.WriteObject(&message.SubgroupObject{Payload: []byte{byte('A' + i)}}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	_ = sgB.Close()

	select {
	case n := <-objects:
		if n != sgCount {
			t.Fatalf("received %d objects via Discovery fallback, want %d", n, sgCount)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("no objects after the local publisher failed and Discovery fallback should have served")
	}

	_ = subReq.Close()
	_ = pnsLocal.Close()
	_ = pnsB.Close()
	_ = pubReqB.Close()
	_ = subSess.Close(0, "done")
	_ = pLocal.Close(0, "done")
	_ = pubB.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
	<-rejectDone
}

// TestCrossRelay_MultiRemoteFanIn pins §9.5 cross-relay fault tolerance: when
// two remote relays both advertise a namespace, relay A subscribes to BOTH (not
// just the first) and fans them into one track. The Dialer must fire for each
// remote, and the subscriber must receive each object exactly once even though
// both remotes push the same {GroupID, ObjectID} stream (the §2.1 dedup gate
// drops the redundant copy).
func TestCrossRelay_MultiRemoteFanIn(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayC := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-C"})

	var dialsB, dialsC atomic.Int64
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store,
		RelayAddr: "relay-A",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			switch addr {
			case "relay-B":
				dialsB.Add(1)
				return relayB.l.Dial()
			case "relay-C":
				dialsC.Add(1)
				return relayC.l.Dial()
			default:
				return nil, fmt.Errorf("no relay at %q", addr)
			}
		},
	})

	// A redundant publisher on each of B and C: same track, same namespace.
	startPub := func(tr *testRelay) (*session.Session, *session.Publication) {
		ps := dialClient(t, tr)
		if _, err := ps.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()}); err != nil {
			t.Fatalf("PublishNamespace: %v", err)
		}
		p, err := ps.Publish(ctx, &message.Publish{Namespace: videoNS(), Name: []byte("cam1"), TrackAlias: 7})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		return ps, p
	}
	pubBSess, pubB := startPub(relayB)
	pubCSess, pubC := startPub(relayC)

	// Subscriber on A. Subscribe returns only after A has established BOTH
	// upstreams (to B and C), so both Dialer calls have happened by here.
	subSess := dialClient(t, relayA)
	subReq, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("cross-relay Subscribe: %v", err)
	}

	if got := dialsB.Load(); got != 1 {
		t.Errorf("Dialer fired %d times for relay-B; want 1 (dial-all)", got)
	}
	if got := dialsC.Load(); got != 1 {
		t.Errorf("Dialer fired %d times for relay-C; want 1 (dial-all)", got)
	}

	events := make(chan objEvent, 64)
	go readSubgroups(ctx, subSess, events)

	// Both remotes push the same objects 0,1,2 on the same (group, subgroup).
	push := func(p *session.Publication) {
		sg, err := p.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: 7, GroupID: 0, SubgroupID: 0,
		})
		if err != nil {
			t.Errorf("OpenSubgroup: %v", err)
			return
		}
		for i := range 3 {
			if err := sg.WriteObject(&message.SubgroupObject{
				ObjectIDDelta: 0,
				Payload:       []byte{byte('A' + i)},
			}); err != nil {
				t.Errorf("WriteObject #%d: %v", i, err)
				return
			}
		}
		_ = sg.Close()
	}
	push(pubB)
	push(pubC)

	// Collect with a quiet-period idle timeout: each of 0,1,2 must arrive exactly
	// once across however many outbound streams the merge produced.
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
	for _, id := range []uint64{0, 1, 2} {
		if seen[id] != 1 {
			t.Fatalf("object %d delivered %d times across two remotes, want exactly 1 (dedup): %v", id, seen[id], seen)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("delivered set = %v, want {0,1,2}", seen)
	}

	_ = subReq.Close()
	_ = pubB.Close()
	_ = pubC.Close()
	_ = subSess.Close(0, "done")
	_ = pubBSess.Close(0, "done")
	_ = pubCSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
	relayC.stop(t)
}

// TestCrossRelay_SelfExclusion pins the loop guard: a FindNamespace result that
// names this relay's own RelayAddr must never trigger a dial or a self-loop
// SUBSCRIBE. The store is seeded with a namespace owned by "relay-A" itself;
// A's subscriber must be rejected (no other relay serves it) and the Dialer
// must never fire.
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

	// Seed the store with a namespace advertised by relay-A itself.
	if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    videoNS(),
		RelayAddr: "relay-A",
	}); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}

	subSess := dialClient(t, relayA)
	_, err := subSess.Subscribe(ctx, &message.Subscribe{
		Namespace: videoNS(),
		Name:      []byte("cam1"),
	})
	if err == nil {
		t.Fatal("Subscribe succeeded; want rejection (no remote relay, self excluded)")
	}

	if got := dials.Load(); got != 0 {
		t.Errorf("Dialer fired %d times; want 0 (self must not be dialled)", got)
	}

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_PoolReuse pins that two cross-relay SUBSCRIBEs to the same
// remote relay share a single dialled session: subscribing to two distinct
// tracks in the same remote namespace dials once and reuses the pooled session.
func TestCrossRelay_PoolReuse(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})

	var dials atomic.Int64
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store,
		RelayAddr: "relay-A",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr != "relay-B" {
				return nil, fmt.Errorf("no relay at %q", addr)
			}
			dials.Add(1)
			return relayB.l.Dial()
		},
	})

	// Publisher on B advertises the namespace and PUBLISHes two tracks.
	pubSess := dialClient(t, relayB)
	pns, err := pubSess.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	pub1, err := pubSess.Publish(ctx, &message.Publish{Namespace: videoNS(), Name: []byte("cam1"), TrackAlias: 1})
	if err != nil {
		t.Fatalf("Publish cam1: %v", err)
	}
	pub2, err := pubSess.Publish(ctx, &message.Publish{Namespace: videoNS(), Name: []byte("cam2"), TrackAlias: 2})
	if err != nil {
		t.Fatalf("Publish cam2: %v", err)
	}

	subSess := dialClient(t, relayA)
	sub1, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("Subscribe cam1: %v", err)
	}
	sub2, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam2")})
	if err != nil {
		t.Fatalf("Subscribe cam2: %v", err)
	}

	if got := dials.Load(); got != 1 {
		t.Errorf("Dialer fired %d times for two tracks on one relay; want 1 (pool reuse)", got)
	}

	_ = sub1.Close()
	_ = sub2.Close()
	_ = pub1.Close()
	_ = pub2.Close()
	_ = pns.Close()
	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}

// TestCrossRelay_WatchNamespacesForward pins the consume side of
// WatchNamespaces: a namespace advertised by a *remote* relay is reflected to a
// local SUBSCRIBE_NAMESPACE holder as a NAMESPACE message.
func TestCrossRelay_WatchNamespacesForward(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	nsReq, err := subSess.SubscribeNamespace(ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: videoNS(),
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}

	// A remote relay advertises ["video","cam1"] into the shared store. A's
	// WatchNamespaces consumer should forward it to our SUBSCRIBE_NAMESPACE
	// holder as a NAMESPACE carrying the suffix ["cam1"].
	//
	// runNamespaceWatch only sees events emitted AFTER it registered its
	// watcher (MemoryStore does not replay history to new watchers), and that
	// registration happens asynchronously in Start. So re-advertise on a ticker
	// until the subscriber observes it — PublishNamespace re-emits OpPublish on
	// every call. The injector is stopped once we've read the NAMESPACE.
	stopInject := make(chan struct{})
	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			_ = store.PublishNamespace(ctx, discovery.NamespaceInfo{
				Prefix:    wire.TrackNamespace{[]byte("video"), []byte("cam1")},
				RelayAddr: "relay-C",
			})
			select {
			case <-stopInject:
				return
			case <-ticker.C:
			}
		}
	}()
	defer close(stopInject)

	got := relaytest.ReadNextMessage(t, nsReq, time.After(2*time.Second))
	nsMsg, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("got %T, want *message.Namespace", got)
	}
	if len(nsMsg.TrackNamespaceSuffix) != 1 || string(nsMsg.TrackNamespaceSuffix[0]) != "cam1" {
		t.Fatalf("NAMESPACE suffix = %v, want [cam1]", nsMsg.TrackNamespaceSuffix)
	}

	_ = nsReq.Close()
	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_SubscribeNamespaceSeedsRemote pins the seed side of
// cross-relay namespace discovery: a SUBSCRIBE_NAMESPACE holder is told about a
// namespace a *remote* relay advertised BEFORE the subscriber (and before this
// relay) existed. Unlike the WatchNamespaces path, the seed reads
// FindNamespacesUnder at subscribe time, so one pre-advertise suffices — no
// re-advertise ticker needed.
func TestCrossRelay_SubscribeNamespaceSeedsRemote(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	// Remote advertisement exists before the subscriber (and before relay A).
	if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    wire.TrackNamespace{[]byte("video"), []byte("cam1")},
		RelayAddr: "relay-C",
	}); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}

	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	nsReq, err := subSess.SubscribeNamespace(ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: videoNS(),
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}

	got := relaytest.ReadNextMessage(t, nsReq, time.After(2*time.Second))
	nsMsg, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("got %T, want *message.Namespace", got)
	}
	if len(nsMsg.TrackNamespaceSuffix) != 1 || string(nsMsg.TrackNamespaceSuffix[0]) != "cam1" {
		t.Fatalf("seeded NAMESPACE suffix = %v, want [cam1]", nsMsg.TrackNamespaceSuffix)
	}

	_ = nsReq.Close()
	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_ConcurrentSubscriberWrites drives two independent writers at
// one SUBSCRIBE_NAMESPACE holder's stream: a local publisher's PUBLISH_NAMESPACE
// forwards (on a session-handler goroutine) and the relay-level WatchNamespaces
// consumer forwarding remote advertisements. The two write the same stream from
// different goroutines, so this must stay clean under -race (it is the race
// SubscriberEntry.WriteMessage's mutex closes).
func TestCrossRelay_ConcurrentSubscriberWrites(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})

	// Subscriber S subscribes to a prefix and continuously drains its stream so
	// writes never block.
	subSess := dialClient(t, relayA)
	nsReq, err := subSess.SubscribeNamespace(ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("room")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			if _, err := message.Parse(nsReq); err != nil {
				return
			}
		}
	}()

	const rounds = 50
	var wg sync.WaitGroup

	// Writer 1: a local publisher repeatedly advertises namespaces under "room";
	// each PUBLISH_NAMESPACE forwards a NAMESPACE to S (and its Close a
	// NAMESPACE_DONE) from relayA's publisher-handler goroutine.
	pubSess := dialClient(t, relayA)
	wg.Go(func() {
		for i := range rounds {
			pns, err := pubSess.PublishNamespace(ctx, &message.PublishNamespace{
				Namespace: wire.TrackNamespace{[]byte("room"), fmt.Appendf(nil, "local%d", i)},
			})
			if err != nil {
				return
			}
			_ = pns.Close()
		}
	})

	// Writer 2: remote advertisements injected into the shared store; each fires
	// the relay-level watch goroutine to forward a NAMESPACE to the same S.
	wg.Go(func() {
		for i := range rounds {
			_ = store.PublishNamespace(ctx, discovery.NamespaceInfo{
				Prefix:    wire.TrackNamespace{[]byte("room"), fmt.Appendf(nil, "remote%d", i)},
				RelayAddr: "relay-C",
			})
		}
	})

	wg.Wait()

	_ = nsReq.Close()
	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	<-drained
}

// TestCrossRelay_DialerWithoutRelayAddrWarns pins the misconfiguration
// diagnostic for #4: a Dialer set with an empty RelayAddr disables cross-relay
// routing silently (self/remote Discovery entries become indistinguishable), so
// New must emit a warning. A RelayAddr-set relay must NOT warn.
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
		// New logs synchronously and starts no goroutines, so the buffer is
		// fully written by the time New returns.
		_ = relay.New(newPipeListener(), relay.Config{
			Discovery: store,
			RelayAddr: relayAddr,
			Dialer:    dialer,
			Logger:    logger,
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

// TestCrossRelay_NoDialerNoop pins back-compat: with Discovery but no Dialer, a
// SUBSCRIBE with no local publisher is cleanly rejected (no cross-relay
// routing), exactly as a single-instance relay behaves.
func TestCrossRelay_NoDialerNoop(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	// Seed a remote namespace; without a Dialer the relay must not try to use
	// it.
	if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    videoNS(),
		RelayAddr: "relay-B",
	}); err != nil {
		t.Fatalf("seed PublishNamespace: %v", err)
	}

	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})

	subSess := dialClient(t, relayA)
	_, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	if err == nil {
		t.Fatal("Subscribe succeeded; want rejection (no Dialer, no local publisher)")
	}
	// Sanity: the rejection is the protocol-level REQUEST_ERROR, not a
	// transport error.
	if _, ok := errors.AsType[*session.RequestRejectedError](err); !ok {
		t.Logf("Subscribe error (non-RequestRejectedError is acceptable): %v", err)
	}

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}

// TestCrossRelay_UpstreamFanInCapConverges pins Phase-1 affinity routing: when
// three remote relays advertise a namespace but UpstreamFanIn is 1, a leaf relay
// subscribes to exactly one of them (the cap), and two independent leaf relays
// pick the *same* one (rendezvous convergence). That is what turns a full
// O(n²) relay-to-relay mesh into a tree rooted at one relay per namespace.
func TestCrossRelay_UpstreamFanInCapConverges(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayC := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-C"})
	relayD := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-D"})
	remotes := map[string]*testRelay{"relay-B": relayB, "relay-C": relayC, "relay-D": relayD}

	// A publisher on every remote so whichever the leaves converge on can serve
	// cam1 (establishment, and thus the dial, only completes against a relay that
	// actually hosts the track).
	var pubSessions []*session.Session
	for addr, tr := range remotes {
		ps := dialClient(t, tr)
		if _, err := ps.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()}); err != nil {
			t.Fatalf("%s PublishNamespace: %v", addr, err)
		}
		if _, err := ps.Publish(
			ctx,
			&message.Publish{Namespace: videoNS(), Name: []byte("cam1"), TrackAlias: 7},
		); err != nil {
			t.Fatalf("%s Publish: %v", addr, err)
		}
		pubSessions = append(pubSessions, ps)
	}

	// A per-leaf dialer that records only successful dials into known remotes.
	// A candidate a leaf cannot reach (e.g. the other leaf, should it re-advertise
	// the namespace) misses the lookup and is not counted — resolveUpstreams
	// treats it as skip-and-fall-through, so the log holds exactly the leaf's
	// established upstreams.
	makeDialer := func(mu *sync.Mutex, log map[string]int) func(context.Context, string) (session.Conn, error) {
		return func(_ context.Context, addr string) (session.Conn, error) {
			tr, ok := remotes[addr]
			if !ok {
				return nil, fmt.Errorf("no relay at %q", addr)
			}
			mu.Lock()
			log[addr]++
			mu.Unlock()
			return tr.l.Dial()
		}
	}
	dialed := func(mu *sync.Mutex, log map[string]int) (addrs []string, total int) {
		mu.Lock()
		defer mu.Unlock()
		for a, n := range log {
			if n > 0 {
				addrs = append(addrs, a)
				total += n
			}
		}
		return addrs, total
	}

	var muA1 sync.Mutex
	logA1 := map[string]int{}
	relayA1 := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A1", UpstreamFanIn: 1,
		Dialer: makeDialer(&muA1, logA1),
	})
	var muA2 sync.Mutex
	logA2 := map[string]int{}
	relayA2 := startTestRelay(ctx, relay.Config{
		Discovery: store, RelayAddr: "relay-A2", UpstreamFanIn: 1,
		Dialer: makeDialer(&muA2, logA2),
	})

	// Subscribe blocks until the (single) upstream is established, so the dial
	// logs are settled by the time each call returns.
	sub1 := dialClient(t, relayA1)
	req1, err := sub1.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("A1 Subscribe: %v", err)
	}
	sub2 := dialClient(t, relayA2)
	req2, err := sub2.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("A2 Subscribe: %v", err)
	}

	a1Addrs, a1Total := dialed(&muA1, logA1)
	a2Addrs, a2Total := dialed(&muA2, logA2)

	if len(a1Addrs) != 1 || a1Total != 1 {
		t.Errorf("A1 dialed %v (total %d); want exactly one upstream (UpstreamFanIn=1)", a1Addrs, a1Total)
	}
	if len(a2Addrs) != 1 || a2Total != 1 {
		t.Errorf("A2 dialed %v (total %d); want exactly one upstream (UpstreamFanIn=1)", a2Addrs, a2Total)
	}
	if len(a1Addrs) == 1 && len(a2Addrs) == 1 && a1Addrs[0] != a2Addrs[0] {
		t.Errorf("leaves diverged: A1 chose %q, A2 chose %q; rendezvous ranking must converge", a1Addrs[0], a2Addrs[0])
	}

	_ = req1.Close()
	_ = req2.Close()
	_ = sub1.Close(0, "done")
	_ = sub2.Close(0, "done")
	for _, ps := range pubSessions {
		_ = ps.Close(0, "done")
	}
	relayA1.stop(t)
	relayA2.stop(t)
	relayB.stop(t)
	relayC.stop(t)
	relayD.stop(t)
}

// TestCrossRelay_GoawayPrecedesUpstreamTeardown pins the §3.6 shutdown ordering:
// "When the server is a subscriber, it SHOULD send a GOAWAY message to
// downstream subscribers prior to unsubscribing from upstream publishers."
//
// The relay is a subscriber on every session its upstream pool dialled, so
// cancelling the pool is the "unsubscribe from upstream" step and must not run
// before the GOAWAY broadcast. It used to run first, at the same point as the
// listener close, which raced the upstream session out of Stop's snapshot — so
// the upstream peer could be dropped having never been told the relay was going
// away.
//
// The test owns the upstream peer's session directly (the Dialer hands the relay
// one end of a pipe and the test serves the other), so it can observe what that
// peer receives.
//
// Detection profile, measured against the old ordering: 5/5 failures at
// GOMAXPROCS=1, 0/5 on a multi-core run. Cancelling the pool does not itself
// close the session — it unwinds the handler, and only if that wins the race to
// deregister does the session miss Stop's snapshot and lose its GOAWAY. So this
// is a strict regression guard single-threaded and a correctness assertion
// everywhere else.
func TestCrossRelay_GoawayPrecedesUpstreamTeardown(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	// Stand in for a peer relay: advertise a namespace at "peer" so the relay
	// resolves it as an upstream, and serve the far end of the dialled pipe here.
	const peerAddr = "peer:4433"
	if err := store.PublishNamespace(ctx,
		discovery.NamespaceInfo{Prefix: videoNS(), RelayAddr: peerAddr}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	peerSessions := make(chan *session.Session, 1)
	r := startTestRelay(ctx, relay.Config{
		GoawayTimeout: 2 * time.Second, // long enough that the drain is observable
		Discovery:     store,
		RelayAddr:     "relay-under-test:4433",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr != peerAddr {
				return nil, fmt.Errorf("no relay at %q", addr)
			}
			relaySide, peerSide := sessiontest.NewConnPair()
			go func() {
				// The relay dials as a client, so this end completes SETUP as
				// the server.
				sess, err := session.Server(context.Background(), peerSide)
				if err != nil {
					close(peerSessions)
					return
				}
				peerSessions <- sess
			}()
			return relaySide, nil
		},
	})

	// A downstream SUBSCRIBE with no local publisher drives the upstream dial.
	// Issued in the background: the relay does not answer it until the upstream
	// does, and this peer deliberately never replies — the dial is all we need.
	subSess := dialClient(t, r)
	go func() {
		_, _ = subSess.Subscribe(ctx, &message.Subscribe{Namespace: videoNS(), Name: []byte("cam1")})
	}()

	var peer *session.Session
	select {
	case peer = <-peerSessions:
		if peer == nil {
			t.Fatal("upstream peer SETUP failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("relay never dialled the advertised upstream")
	}

	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = r.r.Stop(stopCtx)
	}()

	// The upstream peer must be told the relay is going away before its session
	// is torn down.
	select {
	case <-peer.GoawayReceived():
	case <-peer.Done():
		t.Fatal("upstream session was torn down without ever receiving a GOAWAY;" +
			" §3.6 requires the GOAWAY first")
	case <-time.After(5 * time.Second):
		t.Fatal("upstream peer received no GOAWAY")
	}

	<-stopDone
	select {
	case err := <-r.startErr:
		if err != nil {
			t.Errorf("Start returned: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Error("Start did not return after Stop")
	}
}

// TestCrossRelay_JoiningFetchBackfillsPublishOnceTrack pins §5.1: a relay MUST
// save the Largest Location an upstream communicated in SUBSCRIBE_OK, because
// that value is the Joining Location its own downstream subscribers need.
//
// The track here is published *once* and then goes quiet, which is what makes
// the omission fatal rather than merely late. A live subscription carries only
// future objects, so the sole route to content published before the subscriber
// arrived is the §10.12.2 Joining FETCH — and that is refused with INVALID_RANGE
// when the relay has no Joining Location to compute a range from. Before the
// fix, relay A learned no watermark from B's SUBSCRIBE_OK, omitted
// LARGEST_OBJECT from its own SUBSCRIBE_OK (violating §10.2.16), and rejected
// the backfill; the subscriber never saw the track's contents at all.
//
// An MSF catalog is exactly this shape — published on join, republished only
// when a participant's tracks change — so in a conference across two relays the
// participant behind that catalog stayed invisible for the whole call.
func TestCrossRelay_JoiningFetchBackfillsPublishOnceTrack(t *testing.T) {
	t.Parallel()

	store := discovery.NewMemoryStore()
	defer store.Close()

	ctx := t.Context()

	relayB := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-B"})
	relayA := startTestRelay(ctx, relay.Config{
		Discovery: store,
		RelayAddr: "relay-A",
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr == "relay-B" {
				return relayB.l.Dial()
			}
			return nil, fmt.Errorf("no relay at %q", addr)
		},
	})

	// Publisher on B publishes the whole track, then stops. Nothing is written
	// after the subscriber joins, so live delivery cannot cover any of it.
	pubSess := dialClient(t, relayB)
	pns, err := pubSess.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	const pubAlias = uint64(7)
	pubReq, err := pubSess.Publish(ctx, &message.Publish{
		Namespace:  videoNS(),
		Name:       []byte("catalog"),
		TrackAlias: pubAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	pubSg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     pubAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	const sgCount = 3
	for i := range sgCount {
		if err := pubSg.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 0,
			Payload:       []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("WriteObject #%d: %v", i, err)
		}
	}
	if err := pubSg.Close(); err != nil {
		t.Fatalf("pubSg.Close: %v", err)
	}

	// Let B's fanout observe the objects, so B has a watermark to report in its
	// SUBSCRIBE_OK. Without this the test could pass for the wrong reason: B
	// omitting LARGEST_OBJECT because it genuinely knows nothing yet is not the
	// bug under test.
	time.Sleep(100 * time.Millisecond)

	// Subscriber joins on A, which has no local publisher and follows Discovery
	// to B. Bind the message so its assigned Request ID can anchor the Joining
	// FETCH below (Subscribe mutates RequestID via AllocRequestID).
	subSess := dialClient(t, relayA)
	subMsg := &message.Subscribe{Namespace: videoNS(), Name: []byte("catalog")}
	subReq, err := subSess.Subscribe(ctx, subMsg)
	if err != nil {
		t.Fatalf("cross-relay Subscribe: %v", err)
	}

	// §10.2.16: "If Objects have been published on this Track the Publisher MUST
	// include this parameter." A is the publisher for this subscriber, and B has
	// told it objects exist, so its SUBSCRIBE_OK has to carry the watermark.
	// This is the assertion the fix turns green on the wire.
	if _, ok := subReq.OK.Parameters.Find(message.ParamLargestObject); !ok {
		t.Fatalf("A's SUBSCRIBE_OK omitted LARGEST_OBJECT; it learned no Joining "+
			"Location from B's SUBSCRIBE_OK (params=%v)", subReq.OK.Parameters)
	}

	// The Joining FETCH is the only way this subscriber can reach content
	// published before it arrived. A's own cache is empty — its upstream uses the
	// §9.4 Largest Object filter — so answering means stitching from B (§9.4).
	fetchReq, err := subSess.Fetch(ctx, &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining: &message.JoiningFetch{
			JoiningRequestID: subMsg.RequestID,
			JoiningStart:     0, // the largest group itself
		},
		Parameters: message.Parameters{message.GroupOrderParam(message.GroupOrderAscending)},
	})
	if err != nil {
		t.Fatalf("joining FETCH rejected, so the backfill is unreachable: %v", err)
	}
	defer fetchReq.Close()

	type fetchResult struct {
		n   int
		err error
	}
	done := make(chan fetchResult, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(ctx)
		if err != nil {
			done <- fetchResult{err: err}
			return
		}
		fs, ok := ds.(*session.IncomingFetchStream)
		if !ok {
			done <- fetchResult{err: fmt.Errorf("got %T, want *session.IncomingFetchStream", ds)}
			return
		}
		var n int
		for {
			if _, err := fs.ReadObject(); err != nil {
				done <- fetchResult{n: n}
				return
			}
			n++
		}
	}()

	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("reading the joining FETCH response: %v", res.err)
		}
		if res.n != sgCount {
			t.Errorf("joining FETCH returned %d objects, want %d — the backfill "+
				"did not cover the group published before the subscriber joined",
				res.n, sgCount)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("joining FETCH response did not arrive within deadline")
	}

	_ = subReq.Close()
	_ = pubReq.Close()
	_ = pns.Close()
	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
}
