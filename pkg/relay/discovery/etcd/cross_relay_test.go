package etcd_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	etcdstore "github.com/floatdrop/moq-go/pkg/relay/discovery/etcd"
)

// TestCrossRelayEtcd_OnDemandSubscribe is the end-to-end proof that the etcd
// backend actually routes across relays: two relays run with *separate* etcd
// Stores (separate clients, separate leases) that share one embedded etcd
// cluster under a common key prefix — the real multi-process topology. A
// publisher on relay B advertises the "video" namespace; that advertisement is
// written to etcd by B's Store. Relay A has no local publisher, so on a
// downstream SUBSCRIBE it reads the advertisement back out of etcd via its own
// Store, dials relay B, and subscribes upstream. Objects flow
// publisher → B → A → subscriber, entirely through etcd discovery + the Dialer.
func TestCrossRelayEtcd_OnDemandSubscribe(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)
	ctx := t.Context()

	// Quiet the relays' and stores' debug/warn chatter; a lost-lease warning is
	// expected during teardown when a Store closes under the running relay.
	logger := slog.New(slog.DiscardHandler)

	// One Store per relay, both scoped to the same prefix so each sees the
	// other's advertisements — exactly as two relay processes sharing a cluster.
	const prefix = "/xrelay/"
	storeB := openStore(t, endpoints, prefix, logger)
	storeA := openStore(t, endpoints, prefix, logger)

	relayB := startEtcdTestRelay(ctx, relay.Config{
		Discovery: storeB,
		RelayAddr: "relay-B",
		Logger:    logger,
	})
	relayA := startEtcdTestRelay(ctx, relay.Config{
		Discovery: storeA,
		RelayAddr: "relay-A",
		Logger:    logger,
		Dialer: func(_ context.Context, addr string) (session.Conn, error) {
			if addr == "relay-B" {
				return relayB.l.Dial()
			}
			return nil, fmt.Errorf("no relay at %q", addr)
		},
	})

	// Publisher connects to B: advertise the namespace (so A's FindNamespace can
	// route here through etcd) and PUBLISH the track (so B has a live upstream).
	pubSess := dialEtcdClient(t, relayB)
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

	// PublishNamespace returns once relay B has the advertisement; B writes it
	// to etcd asynchronously, and A can only route once its own Store can read
	// it back. Waiting for that is the difference between a deterministic test
	// and one that fails on a loaded runner with "no publisher for namespace" —
	// which is exactly how this test flaked in CI.
	waitForNamespace(t, storeA, videoNS())

	// Subscriber connects to A and subscribes. Subscribe returns only after A
	// has resolved the track through etcd and established its upstream to B, so
	// the full chain is live by the time we push objects.
	subSess := dialEtcdClient(t, relayA)
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
	case <-time.After(5 * time.Second):
		t.Fatal("objects did not cross the relay boundary within deadline")
	}

	// Teardown: clients, then A (tears down its upstream to B), then B. Stores
	// close last, after the relays that use them have stopped.
	_ = subReq.Close()
	_ = pubReq.Close()
	_ = pns.Close()
	_ = subSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
	_ = storeA.Close()
	_ = storeB.Close()
}

func videoNS() wire.TrackNamespace { return wire.TrackNamespace{[]byte("video")} }

// openStore dials a dedicated etcd client and wraps it in a Store scoped to
// prefix. The client is torn down with the test; the Store is closed by the
// caller after its relay stops.
func openStore(t *testing.T, endpoints []string, prefix string, logger *slog.Logger) *etcdstore.Store {
	t.Helper()
	cli, err := clientv3.New(clientv3.Config{Endpoints: endpoints, DialTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("dial etcd: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return etcdstore.New(cli, etcdstore.WithPrefix(prefix), etcdstore.WithLogger(logger))
}

// --- in-process relay harness (mirrors pkg/relay's cross-relay test rig) -----

// pipeListener feeds in-process sessiontest conn pairs to a relay's accept loop;
// Dial returns the client end of a fresh pair.
type pipeListener struct {
	conns chan session.Conn
	done  chan struct{}
}

func newPipeListener() *pipeListener {
	return &pipeListener{conns: make(chan session.Conn, 4), done: make(chan struct{})}
}

func (l *pipeListener) Dial() (session.Conn, error) {
	clientConn, serverConn := sessiontest.NewConnPair()
	select {
	case l.conns <- serverConn:
		return clientConn, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Accept(ctx context.Context) (session.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		return nil, net.ErrClosed
	}
}

func (l *pipeListener) Addr() net.Addr { return nil }

func (l *pipeListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}

type etcdTestRelay struct {
	r        *relay.Relay
	l        *pipeListener
	startErr chan error
}

func startEtcdTestRelay(ctx context.Context, cfg relay.Config) *etcdTestRelay {
	if cfg.GoawayTimeout == 0 {
		cfg.GoawayTimeout = 50 * time.Millisecond
	}
	l := newPipeListener()
	r := relay.New(l, cfg)
	se := make(chan error, 1)
	go func() { se <- r.Start(ctx) }()
	return &etcdTestRelay{r: r, l: l, startErr: se}
}

func (tr *etcdTestRelay) stop(t *testing.T) {
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

func dialEtcdClient(t *testing.T, tr *etcdTestRelay) *session.Session {
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

// waitForNamespace blocks until store can read ns back out of etcd, which is
// the point at which a relay using that Store can route to it.
//
// The write is asynchronous: PublishNamespace returns as soon as the relay it
// was sent to has the advertisement, but that relay's Store then writes to etcd
// and the *other* relay's Store has to observe it. Subscribing before that
// resolves is refused with "no publisher for namespace" — a real failure mode,
// just not the one any of these tests are about.
func waitForNamespace(t *testing.T, store *etcdstore.Store, ns wire.TrackNamespace) {
	t.Helper()
	ctx := t.Context()
	deadline := time.Now().Add(10 * time.Second)
	for {
		infos, err := store.FindNamespace(ctx, ns)
		if err == nil && len(infos) > 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("namespace %v never became visible through etcd (last err: %v)", ns, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
