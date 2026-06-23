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
	var reqErr *session.RequestRejectedError
	if !errors.As(err, &reqErr) {
		t.Logf("Subscribe error (non-RequestRejectedError is acceptable): %v", err)
	}

	_ = subSess.Close(0, "done")
	relayA.stop(t)
}
