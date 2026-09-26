package relay_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// Request stream lifecycle at the relay. A FIN is not a cancellation (§3.3.2);
// after a FIN the cancel is STOP_SENDING (§3.3.3). Only the request's sender
// may send REQUEST_UPDATE (§10.9), and only the publisher PUBLISH_STATE_NOTIFY
// (§10.10).

// TestRelay_SubscriberFINKeepsSubscription: a subscriber that FINs its side of
// the SUBSCRIBE stream still receives objects, and its later STOP_SENDING is
// what ends the subscription.
func TestRelay_SubscriberFINKeepsSubscription(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if err := subReq.Stream.Close(); err != nil { // FIN, not a cancel
		t.Fatalf("FIN: %v", err)
	}

	publishObjects(t, pubSess, alias, 3, 1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("a FIN'd subscription stopped receiving objects")
	}

	subReq.Stream.CancelRead(uint64(moqt.StreamResetCancelled)) // STOP_SENDING: the cancel
	// The relay tears the subscription down; later objects are not forwarded.
	// Retry the probe: the cancel reaches the relay asynchronously.
	deadline := time.Now().Add(2 * time.Second)
	misses := 0
	for group := uint64(4); ; group++ {
		go sendObjects(pubSess, alias, group, 1)
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

// TestRelay_PublishNamespaceFINStaysAdvertised: a FIN'd PUBLISH_NAMESPACE is
// not withdrawn (§6.2), so SUBSCRIBEs still reach its publisher.
func TestRelay_PublishNamespaceFINStaysAdvertised(t *testing.T) {
	t.Parallel()
	video := ns("video")
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	np, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video})
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
		_, _ = subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
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

// TestRelay_FetchRequesterFINCompletesRequest: after the FETCH requester's FIN
// the relay FINs back and the request completes.
func TestRelay_FetchRequesterFINCompletesRequest(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	liveSess := newCam1Subscriber(t, pubSess)
	publishObjects(t, pubSess, alias, 3, 1)
	if !awaitSubgroupObject(t, liveSess, 2*time.Second) {
		t.Fatal("object not forwarded")
	}

	fetchSess := dialAnotherClient(t, pubSess)
	loc := message.Location{Group: 3}
	fr, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace:  ns("video"),
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

// TestRelay_SubscribeNamespaceFINKeepsSubscription: a FIN'd SUBSCRIBE_NAMESPACE
// still hears about a later publisher (§6.1).
func TestRelay_SubscribeNamespaceFINKeepsSubscription(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: ns("video"),
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
		Namespace: ns("video", "cam1"),
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
		TrackNamespacePrefix: ns("video"),
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
		Namespace: ns("video", "cam7"),
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

// TestRelay_PublishNamespaceStopSendingAfterFINWithdraws: STOP_SENDING after a
// FIN withdraws the namespace (§3.3.3); subscribers get NAMESPACE_DONE.
func TestRelay_PublishNamespaceStopSendingAfterFINWithdraws(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}

	pubSess := dialAnotherClient(t, subSess)
	np, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video", "cam1"),
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

// TestRelay_SubscriberPublishStateNotifyClosesSession: PUBLISH_STATE_NOTIFY
// from the subscriber closes the session (§10.10).
func TestRelay_SubscriberPublishStateNotifyClosesSession(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go func() { _ = message.Marshal(subReq.Stream, &message.PublishStateNotify{}) }()
	requireSessionClosed(t, subSess, "a subscriber's PUBLISH_STATE_NOTIFY")
}

// TestRelay_UpstreamRequestUpdateOnSubscribeClosesSession: on the relay's own
// upstream SUBSCRIBE the publisher is not the request's sender, so its
// REQUEST_UPDATE is a PROTOCOL_VIOLATION (§10.9).
func TestRelay_UpstreamRequestUpdateOnSubscribeClosesSession(t *testing.T) {
	t.Parallel()
	video := ns("video")
	upSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	go func() {
		r, err := upSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if _, err := r.AcceptSubscribe(nil); err != nil {
			return
		}
		_ = message.Marshal(r.Stream, &message.RequestUpdate{RequestID: upSess.AllocRequestID()})
	}()
	live := dialAnotherClient(t, upSess)
	go func() {
		_, _ = live.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
	}()
	requireSessionClosed(t, upSess, "a REQUEST_UPDATE on the relay's own SUBSCRIBE")
}

// TestRelay_PublisherRequestUpdateOnPublishIsAllowed: the publisher of an
// accepted PUBLISH may send REQUEST_UPDATE (§10.9); the relay declines it
// without closing the session.
func TestRelay_PublisherRequestUpdateOnPublishIsAllowed(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	pub := publish(t, pubSess, &message.Publish{Namespace: ns("video"), Name: []byte("cam2")})
	_, err := pub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
	if rej, ok := errors.AsType[*session.RequestRejectedError](err); !ok || rej.Code != moqt.RequestNotSupported {
		t.Fatalf("Update on an accepted PUBLISH = %v, want REQUEST_ERROR NOT_SUPPORTED", err)
	}
	select {
	case <-pubSess.Done():
		t.Fatalf("relay closed the session on a legal REQUEST_UPDATE: %v", pubSess.Err())
	case <-time.After(200 * time.Millisecond):
	}
}
