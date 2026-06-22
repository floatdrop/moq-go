package relay_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestDatagram_PublisherToSubscriberSingleDatagram is the canonical
// E2E test: publisher sends one OBJECT_DATAGRAM, relay forwards it to a
// subscriber on a separate session with the Track Alias remapped to the
// subscriber's per-session outbound alias.
func TestDatagram_PublisherToSubscriberSingleDatagram(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	type received struct {
		d   *message.ObjectDatagram
		err error
	}
	resCh := make(chan received, 1)
	go func() {
		d, err := subSess.ReceiveDatagram(t.Context())
		resCh <- received{d: d, err: err}
	}()

	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type:              0x08, // DEFAULT_PRIORITY only — Object ID present, no Properties, no Status
		TrackAlias:        publisherAlias,
		GroupID:           3,
		ObjectID:          5,
		PublisherPriority: 0,
		ObjectPayload:     []byte("hello-6e"),
	}); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("subscriber ReceiveDatagram: %v", res.err)
		}
		if res.d.TrackAlias != subReq.OK.TrackAlias {
			t.Fatalf("forwarded TrackAlias = %d, want %d (subscriber's outbound alias)",
				res.d.TrackAlias, subReq.OK.TrackAlias)
		}
		if res.d.GroupID != 3 || res.d.ObjectID != 5 {
			t.Fatalf("forwarded Location = (%d, %d), want (3, 5)", res.d.GroupID, res.d.ObjectID)
		}
		if string(res.d.ObjectPayload) != "hello-6e" {
			t.Fatalf("payload = %q, want %q", res.d.ObjectPayload, "hello-6e")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not receive datagram within deadline")
	}
}

// TestDatagram_FilterDropsBelowStart pins the §5.1.2 filter behaviour on
// the datagram path: a subscriber with AbsoluteStart {Group: 0, Object: 2}
// only sees datagrams whose Location is >= {0, 2}. The relay does not
// re-encode anything on a datagram (each is self-contained), so this is a
// straight gate check.
func TestDatagram_FilterDropsBelowStart(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.SubscriptionFilterParam(&message.SubscriptionFilter{
				Type:          message.FilterAbsoluteStart,
				StartLocation: message.Location{Group: 0, Object: 2},
			}),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Collector: drain every datagram the subscriber receives in a
	// background loop and record its Object IDs.
	got := make(chan []uint64, 1)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go func() {
		var ids []uint64
		for {
			d, err := subSess.ReceiveDatagram(ctx)
			if err != nil {
				got <- ids
				return
			}
			ids = append(ids, d.ObjectID)
		}
	}()

	// Send 5 datagrams in group 0 with Object IDs 0..4. Filter passes
	// only IDs 2, 3, 4.
	for i := range uint64(5) {
		if err := pubSess.SendDatagram(&message.ObjectDatagram{
			Type:          0x08, // DEFAULT_PRIORITY
			TrackAlias:    publisherAlias,
			GroupID:       0,
			ObjectID:      i,
			ObjectPayload: []byte{byte('A' + i)},
		}); err != nil {
			t.Fatalf("SendDatagram #%d: %v", i, err)
		}
	}

	// Let the relay drain, then stop the collector.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case ids := <-got:
		want := []uint64{2, 3, 4}
		if !reflect.DeepEqual(ids, want) {
			t.Fatalf("subscriber saw datagram Object IDs %v, want %v", ids, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not finish within deadline")
	}
}

// TestDatagram_UnknownAliasDroppedSilently pins the §11.3 rule: an inbound
// datagram with a Track Alias the relay doesn't recognise is dropped
// silently — the session must NOT be closed, and the relay must keep
// processing further datagrams normally.
func TestDatagram_UnknownAliasDroppedSilently(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	const publisherAlias = uint64(7)
	pubReqStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReqStream.Close()

	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Bogus datagram with an alias the relay never registered.
	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type:          0x08,
		TrackAlias:    publisherAlias + 99,
		GroupID:       0,
		ObjectID:      0,
		ObjectPayload: []byte("bogus"),
	}); err != nil {
		t.Fatalf("SendDatagram bogus: %v", err)
	}

	// Good datagram on the legitimate alias — must arrive at the
	// subscriber, proving the relay didn't terminate the session.
	type received struct {
		d   *message.ObjectDatagram
		err error
	}
	resCh := make(chan received, 1)
	go func() {
		d, err := subSess.ReceiveDatagram(t.Context())
		resCh <- received{d: d, err: err}
	}()
	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type:          0x08,
		TrackAlias:    publisherAlias,
		GroupID:       1,
		ObjectID:      2,
		ObjectPayload: []byte("good"),
	}); err != nil {
		t.Fatalf("SendDatagram good: %v", err)
	}

	select {
	case res := <-resCh:
		if res.err != nil {
			t.Fatalf("subscriber ReceiveDatagram: %v", res.err)
		}
		if string(res.d.ObjectPayload) != "good" {
			t.Fatalf("payload = %q, want %q", res.d.ObjectPayload, "good")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber did not receive the follow-up datagram — session may have been closed by the bogus alias")
	}
}
