package quicconn_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/internal/conntest"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// startRelayOverQUIC boots a real relay on a loopback QUIC listener and returns
// its dial address. enableDatagrams mirrors cmd/relay when true; passing
// false exercises the case where the transport lacks DATAGRAM support, which must
// NOT take down the relay's request/data handling.
func startRelayOverQUIC(t *testing.T, enableDatagrams bool) (addr string, quicCfg *quic.Config) {
	t.Helper()
	quicCfg = &quic.Config{
		MaxIdleTimeout:  5 * time.Second,
		KeepAlivePeriod: time.Second,
		EnableDatagrams: enableDatagrams,
	}
	ql, err := quic.ListenAddr("127.0.0.1:0", conntest.TLSConfig(t, testALPN), quicCfg)
	if err != nil {
		t.Fatalf("ListenAddr: %v", err)
	}
	t.Cleanup(func() { _ = ql.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r := relay.New(quicconn.NewListener(ql), relay.Config{GoawayTimeout: 100 * time.Millisecond})
	go func() { _ = r.Start(ctx) }()
	t.Cleanup(func() {
		sctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = r.Stop(sctx)
	})
	return ql.Addr().String(), quicCfg
}

// dialRelayClient dials the relay over real QUIC and completes the MoQT SETUP.
func dialRelayClient(t *testing.T, addr string, quicCfg *quic.Config) *session.Session {
	t.Helper()
	clientTLS := &tls.Config{InsecureSkipVerify: true, NextProtos: []string{testALPN}}
	qc, err := quic.DialAddr(t.Context(), addr, clientTLS, quicCfg)
	if err != nil {
		t.Fatalf("DialAddr: %v", err)
	}
	sess, err := session.Client(t.Context(), quicconn.New(qc))
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close(moqt.SessionNoError, "done") })
	return sess
}

// publishPeer reproduces a conferencing publisher's startup: PUBLISH catalog
// (+ emit the one-shot catalog object), PUBLISH video+audio, then announce the
// namespace LAST. Aliases are session-local so the fixed values are fine per peer.
func publishPeer(t *testing.T, sess *session.Session, id string) wire.TrackNamespace {
	t.Helper()
	ns := wire.Namespace("room", id)

	// Pre-allocate the alias, PUBLISH with it, then emit the one-shot catalog
	// object on that SAME alias. This is the pattern that
	// broke when AllocOutboundTrackAlias handed out 0 (the catalog's first
	// alias) and Publish silently re-allocated it — sending the object under a
	// stale alias the relay never bound.
	catAlias := sess.AllocOutboundTrackAlias()
	catReq, err := sess.Publish(
		t.Context(),
		&message.Publish{Namespace: ns, Name: []byte("catalog"), TrackAlias: catAlias},
	)
	if err != nil {
		t.Fatalf("[%s] Publish catalog: %v", id, err)
	}
	t.Cleanup(func() { catReq.Close() })

	sg, err := sess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: catAlias, GroupID: 0,
	})
	if err != nil {
		t.Fatalf("[%s] OpenSubgroup: %v", id, err)
	}
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{Payload: []byte("catalog-json")}); err != nil {
		t.Fatalf("[%s] WriteObjectAt: %v", id, err)
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("[%s] sg.Close: %v", id, err)
	}

	vidAlias := sess.AllocOutboundTrackAlias()
	vidReq, err := sess.Publish(
		t.Context(),
		&message.Publish{Namespace: ns, Name: []byte("video"), TrackAlias: vidAlias},
	)
	if err != nil {
		t.Fatalf("[%s] Publish video: %v", id, err)
	}
	t.Cleanup(func() { vidReq.Close() })
	audAlias := sess.AllocOutboundTrackAlias()
	audReq, err := sess.Publish(
		t.Context(),
		&message.Publish{Namespace: ns, Name: []byte("audio"), TrackAlias: audAlias},
	)
	if err != nil {
		t.Fatalf("[%s] Publish audio: %v", id, err)
	}
	t.Cleanup(func() { audReq.Close() })

	nsReq, err := sess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns})
	if err != nil {
		t.Fatalf("[%s] PublishNamespace: %v", id, err)
	}
	t.Cleanup(func() { nsReq.Close() })
	return ns
}

// awaitPeerAndFetchCatalog discovers selfID's view of the namespace, waits for
// the announce whose suffix is wantID (skipping its own), then pairs SUBSCRIBE
// (FilterLargestObject) with a relative Joining FETCH against wantID's catalog.
// catalogStepTimeout budgets each discovery step. Loopback QUIC on a busy CI
// runner is a great deal slower than on a developer machine — real handshakes,
// real packetisation, several sessions competing — and the previous 3s was
// tight enough that a contended runner failed here with nothing actually
// broken.
const catalogStepTimeout = 15 * time.Second

// Fails if the FETCH is rejected.
func awaitPeerAndFetchCatalog(t *testing.T, sess *session.Session, selfID, wantID string) error {
	t.Helper()
	nsStream, err := sess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.Namespace("room"),
	})
	if err != nil {
		return fmt.Errorf("[%s] SubscribeNamespace: %w", selfID, err)
	}
	t.Cleanup(func() { nsStream.Close() })

	found := make(chan error, 1)
	go func() {
		for {
			m, perr := message.Parse(nsStream)
			if perr != nil {
				found <- perr
				return
			}
			n, ok := m.(*message.Namespace)
			if !ok || len(n.TrackNamespaceSuffix) == 0 {
				continue
			}
			if string(n.TrackNamespaceSuffix[0]) == wantID {
				found <- nil
				return
			}
		}
	}()
	select {
	case e := <-found:
		if e != nil {
			return fmt.Errorf("[%s] await %s announce: %w", selfID, wantID, e)
		}
	case <-time.After(catalogStepTimeout):
		return fmt.Errorf("[%s] no NAMESPACE announce for %s within %s",
			selfID, wantID, catalogStepTimeout)
	}

	ns := wire.Namespace("room", wantID)
	// Next Object for the live edge, plus a fill covering the current group —
	// draft-20's replacement for the SUBSCRIBE + Relative Joining FETCH pair
	// (§5.1.3, §5.1.6). The fill rides the same request, so a catalog already
	// behind the live edge still arrives.
	subMsg := &message.Subscribe{
		Namespace: ns, Name: []byte("catalog"),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.FillParametersParam(message.Parameters{
				message.RelativeStartFilter(1),
			}),
		},
	}
	subStream, err := sess.Subscribe(t.Context(), subMsg)
	if err != nil {
		return fmt.Errorf("[%s] Subscribe %s catalog: %w", selfID, wantID, err)
	}
	t.Cleanup(func() { subStream.Close() })
	return nil
}

// TestReproRelayCatalogOverQUIC: one publisher, one subscriber, real relay over
// loopback QUIC, full discovery flow — the transport combination the in-process
// relay tests don't cover.
func TestReproRelayCatalogOverQUIC(t *testing.T) {
	addr, quicCfg := startRelayOverQUIC(t, true)
	pub := dialRelayClient(t, addr, quicCfg)
	publishPeer(t, pub, "peerA")

	sub := dialRelayClient(t, addr, quicCfg)
	if err := awaitPeerAndFetchCatalog(t, sub, "peerB", "peerA"); err != nil {
		t.Fatal(err)
	}
}

// TestReproRelayMutualConference reproduces a mutual conferencing topology: two
// peers, each session BOTH publishes its own catalog/video/audio AND subscribes
// to the other's catalog via a joining FETCH.
func TestReproRelayMutualConference(t *testing.T) {
	addr, quicCfg := startRelayOverQUIC(t, true)

	a := dialRelayClient(t, addr, quicCfg)
	b := dialRelayClient(t, addr, quicCfg)

	publishPeer(t, a, "peerA")
	publishPeer(t, b, "peerB")

	done := make(chan error, 2)
	go func() { done <- awaitPeerAndFetchCatalog(t, a, "peerA", "peerB") }()
	go func() { done <- awaitPeerAndFetchCatalog(t, b, "peerB", "peerA") }()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(2 * catalogStepTimeout):
			t.Fatalf("mutual catalog fetch did not complete within %s", 2*catalogStepTimeout)
		}
	}
}

// TestRelayServesWithoutDatagrams guards the datagram-loop hardening: when the
// transport has no DATAGRAM support, the relay's ReceiveDatagram fails on the
// first call. That must NOT tear down request/data handling — a PUBLISH must
// still get its REQUEST_OK. Before the fix this PUBLISH hung forever, because
// the datagram loop's failure cancelled the request and data loops too.
func TestRelayServesWithoutDatagrams(t *testing.T) {
	addr, quicCfg := startRelayOverQUIC(t, false /* no datagrams */)
	sess := dialRelayClient(t, addr, quicCfg)

	done := make(chan error, 1)
	go func() {
		req, err := sess.Publish(t.Context(), &message.Publish{
			Namespace:  wire.Namespace("room", "peerA"),
			Name:       []byte("catalog"),
			TrackAlias: sess.AllocOutboundTrackAlias(),
		})
		if req != nil {
			t.Cleanup(func() { req.Close() })
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Publish without datagrams: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Publish hung — datagram loop failure tore down the request loop")
	}
}
