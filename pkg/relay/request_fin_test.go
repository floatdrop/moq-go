package relay_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// §3.3.2: "A FIN only indicates that an endpoint will send no further messages
// in that direction; it is not a request cancellation." "A requester, with the
// exception of the sender of PUBLISH, MAY FIN immediately after sending a
// message if it will not send a REQUEST_UPDATE." §3.3.3: "An endpoint that has
// already sent a FIN on its sending direction and subsequently wishes to cancel
// sends STOP_SENDING on the receiving direction."

// TestRelay_SubscriberFINKeepsSubscription: a subscriber that FINs its side of
// the SUBSCRIBE stream still receives objects, and its later STOP_SENDING is
// what ends the subscription.
func TestRelay_SubscriberFINKeepsSubscription(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := subReq.Stream.Close(); err != nil { // FIN, not a cancel
		t.Fatalf("FIN: %v", err)
	}

	publishSubgroupObject(t, pubSess, alias, 3, -1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("a FIN'd subscription stopped receiving objects")
	}

	subReq.Stream.CancelRead(uint64(moqt.StreamResetCancelled)) // STOP_SENDING: the cancel
	// The relay tears the subscription down; later objects are not forwarded.
	// Retry the probe: the cancel reaches the relay asynchronously.
	deadline := time.Now().Add(2 * time.Second)
	misses := 0
	for group := uint64(4); ; group++ {
		go func() {
			sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: group,
			})
			if err != nil {
				return
			}
			_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
			_ = sg.Close()
		}()
		// Three misses in a row, so a merely slow forward cannot pass.
		if !awaitSubgroupObject(t, subSess, 200*time.Millisecond) {
			if misses++; misses == 3 {
				return
			}
			continue
		}
		misses = 0
		if time.Now().After(deadline) {
			t.Fatal("objects still forwarded after the subscriber's STOP_SENDING")
		}
	}
}

// TestRelay_PublishNamespaceFINStaysAdvertised: a publisher that FINs its
// PUBLISH_NAMESPACE stream has not withdrawn it (§6.2: "withdrawn by
// cancelling the request"), so SUBSCRIBEs for the namespace still reach it.
func TestRelay_PublishNamespaceFINStaysAdvertised(t *testing.T) {
	t.Parallel()
	ns := wire.TrackNamespace{[]byte("video")}
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	np, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	if err := np.Stream.Close(); err != nil { // FIN, not a withdrawal
		t.Fatalf("FIN: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let a wrong withdrawal land first

	got := make(chan message.Message, 1)
	go func() {
		req, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		got <- req.First
		_ = req.RejectError(moqt.RequestDoesNotExist, "test")
	}()
	subSess := dialAnotherClient(t, pubSess)
	go func() {
		_, _ = subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: []byte("cam1")})
	}()
	select {
	case m := <-got:
		if _, ok := m.(*message.Subscribe); !ok {
			t.Fatalf("publisher received %T, want the relayed SUBSCRIBE", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SUBSCRIBE never reached the publisher: the FIN withdrew the namespace")
	}
}

// TestRelay_FetchRequesterFINCompletesRequest: a FETCH requester that FINs will
// send no REQUEST_UPDATE, and the relay has nothing more to send on the request
// stream, so the relay FINs back and the request completes.
func TestRelay_FetchRequesterFINCompletesRequest(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	liveSess := dialAnotherClient(t, pubSess)
	live, err := liveSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = live.Close() })
	publishSubgroupObject(t, pubSess, alias, 3, -1)
	if !awaitSubgroupObject(t, liveSess, 2*time.Second) {
		t.Fatal("object not forwarded")
	}

	fetchSess := dialAnotherClient(t, pubSess)
	loc := message.Location{Group: 3}
	fr, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		Parameters: message.Parameters{fetchRangeFilter(loc, loc)},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	ds, err := fetchSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	_ = decodeFetchStream(t, ds.(*session.IncomingFetchStream), message.GroupOrderAscending)

	if err := fr.Stream.Close(); err != nil { // requester FIN
		t.Fatalf("FIN: %v", err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := message.Parse(fr.Stream)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, io.EOF) {
			t.Fatalf("request stream ended with %v, want the relay's FIN", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay never FINned the FETCH request stream after the requester's FIN")
	}
}

// TestRelay_SubscribeNamespaceFINKeepsSubscription: a SUBSCRIBE_NAMESPACE
// ends only by "resetting or sending STOP_SENDING on the stream" (§6.1), so a
// subscriber that FINs still hears about a publisher that arrives later.
func TestRelay_SubscribeNamespaceFINKeepsSubscription(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	if err := nsSub.Stream.Close(); err != nil { // FIN, not an unsubscribe
		t.Fatalf("FIN: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let a wrong unsubscribe land first

	pubSess := dialAnotherClient(t, subSess)
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam1")},
	}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	if got := relaytest.ReadNextMessage(t, nsSub, time.After(2*time.Second)); !isNamespace(got) {
		t.Fatalf("FIN'd SUBSCRIBE_NAMESPACE got %T, want NAMESPACE", got)
	}
}

// TestRelay_SubscribeTracksFINKeepsSubscription: the same for SUBSCRIBE_TRACKS
// (§6.1) — a FIN'd subscriber still gets PUBLISH for a later track.
func TestRelay_SubscribeTracksFINKeepsSubscription(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	ts, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	if err := ts.Stream.Close(); err != nil { // FIN, not an unsubscribe
		t.Fatalf("FIN: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	pubSess := dialAnotherClient(t, subSess)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam7")},
		Name:      []byte("rtp"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { _ = pubReq.Close() })

	got := make(chan message.Message, 1)
	go func() {
		req, err := subSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		got <- req.First
	}()
	select {
	case m := <-got:
		if _, ok := m.(*message.Publish); !ok {
			t.Fatalf("got %T, want the forwarded PUBLISH", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FIN'd SUBSCRIBE_TRACKS got no PUBLISH")
	}
}

// TestRelay_PublishNamespaceStopSendingAfterFINWithdraws: after a FIN, the
// publisher's STOP_SENDING is the §3.3.3 cancel — the relay withdraws the
// namespace and tells the subscribers it notified with NAMESPACE_DONE.
func TestRelay_PublishNamespaceStopSendingAfterFINWithdraws(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}

	pubSess := dialAnotherClient(t, subSess)
	np, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam1")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	if got := relaytest.ReadNextMessage(t, nsSub, time.After(2*time.Second)); !isNamespace(got) {
		t.Fatalf("got %T, want NAMESPACE", got)
	}
	if err := np.Stream.Close(); err != nil { // FIN
		t.Fatalf("FIN: %v", err)
	}
	np.Stream.CancelRead(uint64(moqt.StreamResetCancelled)) // then the cancel
	if got := relaytest.ReadNextMessage(t, nsSub, time.After(2*time.Second)); !isNamespaceDone(got) {
		t.Fatalf("got %T after the publisher's STOP_SENDING, want NAMESPACE_DONE", got)
	}
}
