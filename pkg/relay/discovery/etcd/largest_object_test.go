package etcd_test

import (
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"

	"context"
)

// waitForLargest polls TRACK_STATUS on sess until the relay reports the given
// Largest Location for the track, and returns once it does.
//
// This replaces the "sleep and hope the fanout has caught up" that the
// equivalent in-process test uses. TRACK_STATUS_OK carries LARGEST_OBJECT
// (§10.2.16), so the relay's own watermark is directly observable over the
// protocol, and the test can wait for the exact precondition it needs instead
// of a duration that is either flaky or slow.
func waitForLargest(
	t *testing.T,
	sess *session.Session,
	name []byte,
	wantGroup, wantObject uint64,
) {
	t.Helper()
	ctx := t.Context()
	deadline := time.Now().Add(10 * time.Second)
	var last string
	for {
		req, err := sess.TrackStatus(ctx, &message.TrackStatus{
			Namespace: videoNS(),
			Name:      name,
		})
		if err == nil {
			p, ok := req.OK.Parameters.Find(message.ParamLargestObject)
			_ = req.Close()
			if ok && p.Group == wantGroup && p.Object == wantObject {
				return
			}
			last = fmt.Sprintf("largest=%v (present=%t)", p, ok)
		} else {
			last = err.Error()
		}
		if time.Now().After(deadline) {
			t.Fatalf("relay never reported largest {%d,%d}: %s", wantGroup, wantObject, last)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestCrossRelayEtcd_LargestObjectSurvivesTheRelayHop is the cross-relay
// LARGEST_OBJECT regression, run over real etcd discovery.
//
// §10.2.16 requires a relay to set LARGEST_OBJECT to the largest of any value
// an upstream reported and the largest Location it has itself observed, and
// §9.4 makes that binding on relays. fea80fc fixed a build where only the
// second half was implemented: every value an upstream volunteered in
// SUBSCRIBE_OK was discarded, so a relay that had watched no objects arrive
// reported nothing downstream.
//
// The value degrading across the hop is not a cosmetic loss. A live
// subscription carries only future objects, so the sole route to content
// published before a subscriber arrived is the §10.12.2 Joining FETCH — and
// that is refused INVALID_RANGE when the relay has no Joining Location to
// compute a range from. A track published once and then quiet, which is exactly
// the shape of an MSF catalog, becomes permanently unreachable through the
// second relay while every log stays silent.
//
// The in-process version of this test (pkg/relay) proves the relay logic. This
// one proves it survives the topology it actually fails in: two relays that
// found each other through etcd rather than a shared in-memory store, each with
// its own Store, client and lease.
//
// It asserts the exact Location rather than the parameter's presence, because
// the failure this guards against has two shapes. Dropping the upstream's value
// omits the parameter; folding it in wrongly — taking the minimum, or resetting
// to the relay's own empty watermark — reports a value that is present and
// wrong, which a presence check waves through and a subscriber then uses to
// fetch the wrong range.
func TestCrossRelayEtcd_LargestObjectSurvivesTheRelayHop(t *testing.T) {
	endpoints := startEmbeddedEtcd(t)
	ctx := t.Context()
	logger := slog.New(slog.DiscardHandler)

	const prefix = "/largest/"
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

	// The publisher writes the whole track to B and then goes quiet. Nothing is
	// written after the subscriber joins, so live delivery cannot cover any of
	// it and the backfill is the only route.
	const trackName = "catalog"
	pubSess := dialEtcdClient(t, relayB)
	pns, err := pubSess.PublishNamespace(ctx, &message.PublishNamespace{Namespace: videoNS()})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	const pubAlias = uint64(7)
	pubReq, err := pubSess.Publish(ctx, &message.Publish{
		Namespace:  videoNS(),
		Name:       []byte(trackName),
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
	const objectCount = 3
	for i := range objectCount {
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

	// Precondition, not an assertion about A: B must have a watermark to report
	// before A can be blamed for losing it. Without this the test could pass for
	// the wrong reason — B legitimately omitting LARGEST_OBJECT because it knows
	// nothing yet is not the bug under test.
	const wantGroup, wantObject = uint64(0), uint64(objectCount - 1)
	statusSess := dialEtcdClient(t, relayB)
	waitForLargest(t, statusSess, []byte(trackName), wantGroup, wantObject)

	waitForNamespace(t, storeA, videoNS())

	// The subscriber joins on A, which has no local publisher and follows etcd
	// to B.
	subSess := dialEtcdClient(t, relayA)
	subMsg := &message.Subscribe{Namespace: videoNS(), Name: []byte(trackName)}
	subReq, err := subSess.Subscribe(ctx, subMsg)
	if err != nil {
		t.Fatalf("cross-relay Subscribe: %v", err)
	}

	// The assertion the whole test exists for: A is the publisher for this
	// subscriber, B told it objects exist, so §10.2.16 obliges A's SUBSCRIBE_OK
	// to carry the watermark — and to carry B's value, not a degraded one.
	got, ok := subReq.OK.Parameters.Find(message.ParamLargestObject)
	if !ok {
		t.Fatalf("A's SUBSCRIBE_OK omitted LARGEST_OBJECT: it discarded the value "+
			"B reported upstream, so a joining subscriber has no Joining Location "+
			"(params=%v)", subReq.OK.Parameters)
	}
	if got.Group != wantGroup || got.Object != wantObject {
		t.Errorf("A reported largest {%d,%d}, want {%d,%d} — the value degraded "+
			"crossing the relay hop", got.Group, got.Object, wantGroup, wantObject)
	}

	// And the consequence that made it fatal: the Joining FETCH is the only way
	// this subscriber reaches content published before it arrived, and it can
	// only be answered from the Location above.
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
		if res.n != objectCount {
			t.Errorf("joining FETCH returned %d objects, want %d — the backfill did "+
				"not cover the group published before the subscriber joined",
				res.n, objectCount)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("joining FETCH response did not arrive within deadline")
	}

	_ = subReq.Close()
	_ = pubReq.Close()
	_ = pns.Close()
	_ = subSess.Close(0, "done")
	_ = statusSess.Close(0, "done")
	_ = pubSess.Close(0, "done")
	relayA.stop(t)
	relayB.stop(t)
	_ = storeA.Close()
	_ = storeB.Close()
}

var _ = wire.TrackNamespace{}
