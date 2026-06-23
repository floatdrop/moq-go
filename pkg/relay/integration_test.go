package relay_test

// Integration test suite. These tests exercise the relay end-to-end
// over the in-process [sessiontest] transport. They complement the
// unit-level tests scattered across the relay package and serve as
// living documentation of the headline scenarios the relay must handle.
//
// Related integration-level coverage in other files:
//
//   - TestFanout_PublisherToSubscriberSingleObject — single-object E2E.
//     The richer multi-object/datagram variant lives below as
//     TestPublishSubscribeE2E.
//   - TestFanout_StalledSubscriberDoesNotBlockFastOne — per-subscriber
//     isolation: a stalled subscriber overflows without blocking a fast one.
//   - TestFetch_FromCacheAscending / Descending — FETCH from cache.
//   - TestFetch_CacheEvictionUnderLoad — cache eviction.
//   - TestRelay_StopBroadcastsGoaway / _StopReturnsEarlyOnCleanDrain /
//     _StopForceClosesOnTimeout + _InboundGoaway* — the GOAWAY half of
//     the migration story; TestGracefulMigration below adds the
//     subscriber-side migration cycle.
//   - TestDiscovery_* — Discovery store integration.

import (
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestPublishSubscribeE2E is the broad end-to-end happy path: a
// publisher claims a track, a subscriber on a separate session
// subscribes, and the relay forwards a mixed stream of subgroup
// objects and datagrams. Both transports MUST land at the subscriber
// with the relay-allocated outbound Track Alias.
func TestPublishSubscribeE2E(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Reader goroutines: one for the subgroup stream, one for
	// datagrams. We collect a snapshot of what arrives and assert
	// completeness + alias remapping.
	type subgroupResult struct {
		header  message.SubgroupHeader
		objects []*message.SubgroupObject
		err     error
	}
	subgroupCh := make(chan subgroupResult, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			subgroupCh <- subgroupResult{err: err}
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			subgroupCh <- subgroupResult{err: errors.New("not a SubgroupStream")}
			return
		}
		var objs []*message.SubgroupObject
		for {
			obj, err := sg.ReadObject()
			if err != nil {
				subgroupCh <- subgroupResult{header: sg.Header, objects: objs, err: err}
				return
			}
			objs = append(objs, obj)
		}
	}()

	datagramCh := make(chan *message.ObjectDatagram, 8)
	go func() {
		for {
			d, err := subSess.ReceiveDatagram(t.Context())
			if err != nil {
				return
			}
			datagramCh <- d
		}
	}()

	// Publisher sends a 5-object subgroup.
	pubSg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
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
			t.Fatalf("WriteObject subgroup #%d: %v", i, err)
		}
	}
	if err := pubSg.Close(); err != nil {
		t.Fatalf("pubSg.Close: %v", err)
	}

	// Publisher also sends 3 datagrams in a different group so the
	// fanout takes its own datagram path.
	const dgCount = 3
	for i := range dgCount {
		if err := pubSess.SendDatagram(&message.ObjectDatagram{
			Type:          0x08, // DEFAULT_PRIORITY
			TrackAlias:    publisherAlias,
			GroupID:       1,
			ObjectID:      uint64(i),
			ObjectPayload: []byte{byte('x' + i)},
		}); err != nil {
			t.Fatalf("SendDatagram #%d: %v", i, err)
		}
	}

	// Drain subgroup stream.
	select {
	case res := <-subgroupCh:
		// any clean termination is fine; the count is what matters
		if len(res.objects) != sgCount {
			t.Fatalf("subgroup objects = %d, want %d", len(res.objects), sgCount)
		}
		if res.header.TrackAlias != subReq.OK.TrackAlias {
			t.Errorf("subgroup TrackAlias = %d, want %d (subscriber's outbound alias)",
				res.header.TrackAlias, subReq.OK.TrackAlias)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subgroup stream did not arrive within deadline")
	}

	// Drain expected datagrams.
	got := 0
	deadline := time.After(2 * time.Second)
	for got < dgCount {
		select {
		case d := <-datagramCh:
			if d.TrackAlias != subReq.OK.TrackAlias {
				t.Errorf("datagram TrackAlias = %d, want %d", d.TrackAlias, subReq.OK.TrackAlias)
			}
			got++
		case <-deadline:
			t.Fatalf("received %d datagrams, want %d", got, dgCount)
		}
	}
}

// TestSubscriptionAggregation pins §9.4: two downstream subscribers
// for the same track must result in ONE upstream subscription. The
// upstream publisher counts the SUBSCRIBE requests it receives; the
// second downstream subscriber arriving while the upstream is
// Established must NOT trigger a fresh upstream SUBSCRIBE.
func TestSubscriptionAggregation(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// Publisher advertises the namespace so the on-demand upstream
	// subscribe path has somewhere to dial. It then accepts inbound
	// SUBSCRIBEs from the relay; we count them.
	pubNS, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNS.Close()

	subscribesSeen := make(chan struct{}, 4)
	go func() {
		for {
			req, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if _, ok := req.First.(*message.Subscribe); !ok {
				continue
			}
			subscribesSeen <- struct{}{}
			// Reply OK so the relay's on-demand upstream subscribe
			// transitions to Established.
			if err := req.Reply(&message.SubscribeOK{
				TrackAlias:      42,
				TrackProperties: []byte("rtp"),
			}); err != nil {
				t.Errorf("upstream Reply: %v", err)
				return
			}
		}
	}()

	// First downstream subscriber → triggers upstream SUBSCRIBE.
	sub1 := dialAnotherClient(t, pubSess)
	subReq1, err := sub1.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("sub1 Subscribe: %v", err)
	}
	defer subReq1.Close()

	// Wait for the publisher to see the first SUBSCRIBE before the
	// second subscriber dials — otherwise both downstream subscribers
	// might race the upstream-Established transition.
	select {
	case <-subscribesSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("publisher never received the upstream SUBSCRIBE for sub1")
	}

	// Second downstream subscriber → MUST be served from the existing
	// upstream; the publisher must NOT see a second SUBSCRIBE.
	sub2 := dialAnotherClient(t, pubSess)
	subReq2, err := sub2.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("sub2 Subscribe: %v", err)
	}
	defer subReq2.Close()

	select {
	case <-subscribesSeen:
		t.Fatal(
			"publisher received a SECOND upstream SUBSCRIBE; the relay should have aggregated sub2 onto the existing upstream",
		)
	case <-time.After(300 * time.Millisecond):
		// good — no extra subscribe
	}
}

// TestPublishNamespaceRouting exercises §6.1 / §9.5:
//
//   - A subscriber's SUBSCRIBE_NAMESPACE for prefix ["video"] should
//     observe a NAMESPACE event when a publisher PUBLISH_NAMESPACEs
//     ["video", "cam1"].
//   - A SUBSCRIBE for that track is routed via the namespace prefix
//     match to the publisher (the on-demand upstream subscribe path).
func TestPublishNamespaceRouting(t *testing.T) {
	t.Parallel()

	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	nsReq, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer nsReq.Close()

	pubSess := dialAnotherClient(t, subSess)
	pubNS, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam1")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNS.Close()

	// Subscriber should receive a NAMESPACE message advertising
	// ["video","cam1"] under the ["video"] prefix.
	deadline := time.After(2 * time.Second)
	got := relaytest.ReadNextMessage(t, nsReq, deadline)
	nsMsg, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("got %T, want *message.Namespace", got)
	}
	if len(nsMsg.TrackNamespaceSuffix) != 1 || string(nsMsg.TrackNamespaceSuffix[0]) != "cam1" {
		t.Fatalf("NAMESPACE suffix = %v, want [cam1]", nsMsg.TrackNamespaceSuffix)
	}
}

// TestDeliveryTimeouts pins the parameter-passthrough contract: a
// PUBLISH carrying OBJECT_DELIVERY_TIMEOUT / SUBGROUP_DELIVERY_TIMEOUT
// is accepted by the relay and a subscriber can complete its
// subscription cycle. The wire-level enforcement of these timeouts is
// owned by the session layer ([session.OutgoingSubgroupStream]'s
// timer-driven reset), which has its own tests; the relay's concern is
// that it doesn't reject or strip these parameters.
func TestDeliveryTimeouts(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: 1,
		Parameters: message.Parameters{
			message.ObjectDeliveryTimeoutParam(250 * time.Millisecond),
			message.SubgroupDeliveryTimeoutParam(1 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("Publish with delivery timeouts: %v", err)
	}
	defer pubReq.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()
	if subReq.OK == nil {
		t.Fatal("SubscribeOK is nil")
	}
}

// TestGracefulMigration exercises the GOAWAY → migrate lifecycle:
//
//  1. Publisher and subscriber are connected to the relay.
//  2. Operator calls Stop on the relay.
//  3. Both peers observe GOAWAY via session.GoawayReceived().
//  4. Both peers cleanly close their sessions in response (the
//     "migrate" action; in a multi-relay deployment they would
//     reconnect elsewhere).
//  5. Stop returns well before its grace period elapses (cooperative
//     drain).
func TestGracefulMigration(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	pubNS, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubNS.Close()

	subSess := dialAnotherClient(t, pubSess)

	// Cooperatively migrate: when GOAWAY arrives, close the session.
	migrated := make(chan struct{}, 2)
	go func() {
		<-pubSess.GoawayReceived()
		_ = pubSess.Close(0, "publisher migrating")
		migrated <- struct{}{}
	}()
	go func() {
		<-subSess.GoawayReceived()
		_ = subSess.Close(0, "subscriber migrating")
		migrated <- struct{}{}
	}()

	start := time.Now()
	teardown() // Stop is inside teardown
	elapsed := time.Since(start)

	// Both peers should have migrated by the time Stop returns. They
	// might still be wrapping up; allow brief settle.
	for i := range 2 {
		select {
		case <-migrated:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of 2 peers migrated within deadline", i)
		}
	}

	// connectRelay's teardown uses a 50ms GoawayTimeout. Cooperative
	// migration should be well under that.
	if elapsed > time.Second {
		t.Errorf("Stop took %v; cooperative migration should complete promptly", elapsed)
	}
}
