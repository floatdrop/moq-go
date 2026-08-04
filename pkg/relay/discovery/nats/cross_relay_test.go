package nats_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	natsstore "github.com/floatdrop/moq-go/pkg/relay/discovery/nats"
)

// TestCrossRelayNATS_OnDemandSubscribe is the end-to-end proof that the NATS
// backend actually routes across relays: two relays run with *separate* Stores
// (separate connections) that share one embedded nats-server and one bucket —
// the real multi-process topology. A publisher on relay B advertises the "video"
// namespace; that advertisement is written to NATS by B's Store. Relay A has no
// local publisher, so on a downstream SUBSCRIBE it reads the advertisement back
// out of NATS via its own Store, dials relay B, and subscribes upstream. Objects
// flow publisher → B → A → subscriber, entirely through NATS discovery + the
// Dialer.
func TestCrossRelayNATS_OnDemandSubscribe(t *testing.T) {
	url := startEmbeddedNATS(t)
	ctx := t.Context()

	// Quiet the relays' and stores' debug/warn chatter; a heartbeat-failed warning
	// is possible during teardown when a Store closes under the running relay.
	logger := slog.New(slog.DiscardHandler)

	// One Store per relay, both on the same bucket so each sees the other's
	// advertisements — exactly as two relay processes sharing a NATS system.
	const bucket = "xrelay"
	storeB := openStore(t, url, bucket, logger)
	storeA := openStore(t, url, bucket, logger)

	relayB := startNATSTestRelay(ctx, relay.Config{
		Discovery: storeB,
		RelayAddr: "relay-B",
		Logger:    logger,
	})
	relayA := startNATSTestRelay(ctx, relay.Config{
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
	// route here through NATS) and PUBLISH the track (so B has a live upstream).
	pubSess := dialNATSClient(t, relayB)
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

	// Subscriber connects to A and subscribes. Subscribe returns only after A has
	// resolved the track through NATS and established its upstream to B, so the
	// full chain is live by the time we push objects.
	subSess := dialNATSClient(t, relayA)
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

// openStore dials a dedicated NATS connection and wraps it in a Store scoped to
// bucket. The connection is torn down with the test; the Store is closed by the
// caller after its relay stops.
func openStore(t *testing.T, url, bucket string, logger *slog.Logger) *natsstore.Store {
	t.Helper()
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("connect nats: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	s, err := natsstore.New(t.Context(), js, natsstore.WithBucket(bucket), natsstore.WithLogger(logger))
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return s
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

type natsTestRelay struct {
	r        *relay.Relay
	l        *pipeListener
	startErr chan error
}

func startNATSTestRelay(ctx context.Context, cfg relay.Config) *natsTestRelay {
	if cfg.GoawayTimeout == 0 {
		cfg.GoawayTimeout = 50 * time.Millisecond
	}
	l := newPipeListener()
	r := relay.New(l, cfg)
	se := make(chan error, 1)
	go func() { se <- r.Start(ctx) }()
	return &natsTestRelay{r: r, l: l, startErr: se}
}

func (tr *natsTestRelay) stop(t *testing.T) {
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

func dialNATSClient(t *testing.T, tr *natsTestRelay) *session.Session {
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
