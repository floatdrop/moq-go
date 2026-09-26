package relay_test

import (
	"context"
	"errors"
	"io"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// §10.19: "The publisher MUST NOT send NAMESPACE_DONE for a namespace suffix
// before the corresponding NAMESPACE", and NAMESPACE_DONE means the relay
// stops "serving new subscriptions for tracks within the provided Track
// Namespace" (§10.18) — so it is per namespace, not per publisher. §10.9.2
// covers TRACK_NAMESPACE_PREFIX updates.

func requireQuiet(t *testing.T, msgs <-chan message.Message, what string) {
	t.Helper()
	select {
	case m := <-msgs:
		t.Fatalf("%s: unexpected %T %+v", what, m, m)
	case <-time.After(300 * time.Millisecond):
	}
}

func requireNamespace(t *testing.T, msgs <-chan message.Message, suffix ...string) {
	t.Helper()
	m := nextMessage(t, msgs)
	n, ok := m.(*message.Namespace)
	if !ok || relaytest.FormatNamespace(n.TrackNamespaceSuffix) != relaytest.FormatNamespace(ns(suffix...)) {
		t.Fatalf("got %T %+v, want NAMESPACE %v", m, m, suffix)
	}
}

func requireNamespaceDone(t *testing.T, msgs <-chan message.Message, suffix ...string) {
	t.Helper()
	m := nextMessage(t, msgs)
	d, ok := m.(*message.NamespaceDone)
	if !ok || relaytest.FormatNamespace(d.TrackNamespaceSuffix) != relaytest.FormatNamespace(ns(suffix...)) {
		t.Fatalf("got %T %+v, want NAMESPACE_DONE %v", m, m, suffix)
	}
}

func publishNS(t *testing.T, sess *session.Session, fields ...string) *session.NamespacePublication {
	t.Helper()
	p, err := sess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns(fields...)})
	if err != nil {
		t.Fatalf("PublishNamespace %v: %v", fields, err)
	}
	return p
}

func subscribeNS(
	t *testing.T,
	sess *session.Session,
	fields ...string,
) (*session.NamespaceSubscription, <-chan message.Message) {
	t.Helper()
	s, err := sess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: ns(fields...)})
	if err != nil {
		t.Fatalf("SubscribeNamespace %v: %v", fields, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, streamMessages(t, s.Stream)
}

// TestNamespace_SecondPublisherKeepsNamespaceAlive: two publishers of one
// namespace announce it once, and it is done only when both have withdrawn.
func TestNamespace_SecondPublisherKeepsNamespaceAlive(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	_, msgs := subscribeNS(t, subSess, "video")

	pub1 := publishNS(t, dialAnotherClient(t, subSess), "video", "cam")
	requireNamespace(t, msgs, "cam")
	pub2 := publishNS(t, dialAnotherClient(t, subSess), "video", "cam")
	requireQuiet(t, msgs, "second publisher of an announced namespace")
	_ = pub1.Close()
	requireQuiet(t, msgs, "one of two publishers withdrew")
	_ = pub2.Close()
	requireNamespaceDone(t, msgs, "cam")
}

// TestNamespace_ReplayedNamespaceGetsDone: a namespace announced to a
// subscriber from the relay's existing state (§6.1) gets its NAMESPACE_DONE
// when its publisher withdraws.
func TestNamespace_ReplayedNamespaceGetsDone(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishNS(t, pubSess, "video", "cam")
	_, msgs := subscribeNS(t, dialAnotherClient(t, pubSess), "video")
	requireNamespace(t, msgs, "cam")
	_ = pub.Close()
	requireNamespaceDone(t, msgs, "cam")
}

func sendPrefixUpdate(t *testing.T, sess *session.Session, stream session.Stream, fields ...string) {
	t.Helper()
	if err := message.Marshal(stream, &message.RequestUpdate{
		RequestID:  sess.AllocRequestID(),
		Parameters: message.Parameters{message.TrackNamespacePrefixParam(ns(fields...))},
	}); err != nil {
		t.Fatalf("write REQUEST_UPDATE: %v", err)
	}
}

// TestNamespace_PrefixUpdateReconciles: after a TRACK_NAMESPACE_PREFIX update
// the subscriber's announced set matches the new prefix. Namespaces the new
// prefix drops are done before the REQUEST_OK (their suffixes are relative to
// the old prefix); namespaces it adds are announced after, relative to the new
// one (§10.9.2), and later events use it too.
func TestNamespace_PrefixUpdateReconciles(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishNS(t, pubSess, "video", "a")
	audio := publishNS(t, dialAnotherClient(t, pubSess), "audio", "b")
	subSess := dialAnotherClient(t, pubSess)
	nsSub, msgs := subscribeNS(t, subSess, "video")
	requireNamespace(t, msgs, "a")

	sendPrefixUpdate(t, subSess, nsSub.Stream, "audio")
	requireNamespaceDone(t, msgs, "a")
	if m := nextMessage(t, msgs); m.Type() != message.TypeRequestOK {
		t.Fatalf("got %T, want the update's REQUEST_OK", m)
	}
	requireNamespace(t, msgs, "b")
	_ = audio.Close()
	requireNamespaceDone(t, msgs, "b")
}

// TestNamespace_PrefixUpdateOverlapRejected: an updated prefix that "would
// share a common prefix with another active subscription of the same type in
// the same session" gets REQUEST_ERROR PREFIX_OVERLAP (§10.2.20), and a failed
// update ends the request — "the responder MUST close the bidi stream"
// (§10.9.1). Once the requester FINs back (§3.3.2) its prefix is free for a
// new subscription.
func TestNamespace_PrefixUpdateOverlapRejected(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, msgs := subscribeNS(t, subSess, "video")
	subscribeNS(t, subSess, "audio")

	sendPrefixUpdate(t, subSess, nsSub.Stream, "audio", "x")
	m := nextMessage(t, msgs)
	rej, ok := m.(*message.RequestError)
	if !ok || rej.ErrorCode != moqt.RequestPrefixOverlap {
		t.Fatalf("got %T %+v, want REQUEST_ERROR PREFIX_OVERLAP", m, m)
	}
	requireStreamEnds(t, msgs)
	_ = nsSub.Stream.Close() // FIN back, as §3.3.2 asks
	requirePrefixReusable(t, subSess, "video")
}

// TestNamespace_StopSendingEndsSubscription: a subscriber may cancel with
// STOP_SENDING alone (§3.3.3). The relay's next write fails; that must end the
// subscription — release its prefix — not leave it queuing messages forever.
func TestNamespace_StopSendingEndsSubscription(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	nsSub, err := subSess.SubscribeNamespace(
		t.Context(),
		&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")},
	)
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	nsSub.Stream.CancelRead(uint64(moqt.StreamResetCancelled))
	publishNS(t, dialAnotherClient(t, subSess), "video", "cam") // makes the relay write
	requirePrefixReusable(t, subSess, "video")
}

func requireStreamEnds(t *testing.T, msgs <-chan message.Message) {
	t.Helper()
	select {
	case m, ok := <-msgs:
		if ok {
			t.Fatalf("got %T after the refusal, want the stream closed", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the stream stayed open after a failed update (§10.9.1)")
	}
}

// requirePrefixReusable waits until a new SUBSCRIBE_NAMESPACE for prefix on
// sess is accepted, showing the old one's reservation was released.
func requirePrefixReusable(t *testing.T, sess *session.Session, fields ...string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		s, err := sess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: ns(fields...)})
		if err == nil {
			t.Cleanup(func() { _ = s.Close() })
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("prefix %v never released: %v", fields, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSubscribeTracks_PrefixUpdateApplies: a SUBSCRIBE_TRACKS prefix update
// changes which later PUBLISHes are forwarded (§10.9.2).
func TestSubscribeTracks_PrefixUpdateApplies(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	ts, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns("video")})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	msgs := streamMessages(t, ts.Stream)
	sendPrefixUpdate(t, subSess, ts.Stream, "audio")
	if m := nextMessage(t, msgs); m.Type() != message.TypeRequestOK {
		t.Fatalf("got %T, want the update's REQUEST_OK", m)
	}

	forwarded := make(chan string, 4)
	go func() {
		for {
			r, err := subSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if p, ok := r.First.(*message.Publish); ok {
				forwarded <- relaytest.FormatNamespace(p.Namespace)
			}
		}
	}()
	pubSess := dialAnotherClient(t, subSess)
	for _, n := range []string{"video", "audio"} {
		p, err := pubSess.Publish(t.Context(), &message.Publish{Namespace: ns(n), Name: []byte("t")})
		if err != nil {
			t.Fatalf("Publish %s: %v", n, err)
		}
		t.Cleanup(func() { _ = p.Close() })
	}
	select {
	case got := <-forwarded:
		if got != "audio" {
			t.Fatalf("forwarded PUBLISH for %q, want only audio", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PUBLISH under the updated prefix was not forwarded")
	}
	select {
	case got := <-forwarded:
		t.Fatalf("also forwarded PUBLISH for %q", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestNamespace_RemoteAndLocalSourcesShareOneAnnouncement: a namespace both a
// remote relay (via Discovery) and a local publisher advertise is announced
// once, and done only when neither does.
func TestNamespace_RemoteAndLocalSourcesShareOneAnnouncement(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)

	subSess := dialClient(t, relayA)
	_, msgs := subscribeNS(t, subSess, "video")
	remote := discovery.NamespaceInfo{Prefix: ns("video", "cam"), RelayAddr: "relay-C"}
	// The watch starts asynchronously; re-advertise until it is seen.
	stop := make(chan struct{})
	go func() {
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			_ = store.PublishNamespace(ctx, remote)
			select {
			case <-stop:
				return
			case <-tick.C:
			}
		}
	}()
	requireNamespace(t, msgs, "cam")
	close(stop)
	// A subscriber arriving now is seeded from the remote state, and counts
	// it as a source too.
	_, late := subscribeNS(t, dialClient(t, relayA), "video")
	requireNamespace(t, late, "cam")

	local := publishNS(t, dialClient(t, relayA), "video", "cam")
	requireQuiet(t, msgs, "a local publisher of a remotely announced namespace")
	if err := store.UnpublishNamespace(ctx, remote.Prefix, remote.RelayAddr); err != nil {
		t.Fatalf("UnpublishNamespace: %v", err)
	}
	requireQuiet(t, msgs, "the remote relay withdrew while a local publisher remains")
	requireQuiet(t, late, "the remote relay withdrew while a local publisher remains")
	_ = local.Close()
	requireNamespaceDone(t, msgs, "cam")
	requireNamespaceDone(t, late, "cam")
}

// failingWatchStore fails its first WatchNamespaces calls.
type failingWatchStore struct {
	*discovery.MemoryStore

	failures atomic.Int32
}

func (s *failingWatchStore) WatchNamespaces(ctx context.Context) (<-chan discovery.NamespaceEvent, error) {
	if s.failures.Add(-1) >= 0 {
		return nil, errors.New("watch unavailable")
	}
	return s.MemoryStore.WatchNamespaces(ctx)
}

// TestNamespace_WatchRetriedAfterFailure: a Discovery watch that fails to
// start is retried, so remote namespaces still reach subscribers.
func TestNamespace_WatchRetriedAfterFailure(t *testing.T) {
	t.Parallel()
	store := &failingWatchStore{MemoryStore: discovery.NewMemoryStore()}
	store.failures.Store(2)
	defer store.Close()
	ctx := t.Context()
	if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix: ns("video", "cam"), RelayAddr: "relay-C",
	}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)
	_, msgs := subscribeNS(t, dialClient(t, relayA), "video")
	requireNamespace(t, msgs, "cam")
}

// cuttableWatchStore's first WatchNamespaces channel forwards the real watch
// until cut is closed, then closes; later calls watch normally.
type cuttableWatchStore struct {
	*discovery.MemoryStore

	cut     chan struct{}
	watches atomic.Int32
}

func (s *cuttableWatchStore) WatchNamespaces(ctx context.Context) (<-chan discovery.NamespaceEvent, error) {
	events, err := s.MemoryStore.WatchNamespaces(ctx)
	if err != nil || s.watches.Add(1) > 1 {
		return events, err
	}
	out := make(chan discovery.NamespaceEvent)
	go func() {
		defer close(out)
		for {
			select {
			case <-s.cut:
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				select {
				case out <- ev:
				case <-s.cut:
					return
				}
			}
		}
	}()
	return out, nil
}

// TestNamespace_RestartedWatchDropsStaleRemote: a namespace a remote relay
// withdrew while the watch was down is done once the restarted watch's
// snapshot no longer lists it.
func TestNamespace_RestartedWatchDropsStaleRemote(t *testing.T) {
	t.Parallel()
	store := &cuttableWatchStore{MemoryStore: discovery.NewMemoryStore(), cut: make(chan struct{})}
	defer store.Close()
	ctx := t.Context()
	remote := discovery.NamespaceInfo{Prefix: ns("video", "cam"), RelayAddr: "relay-C"}
	if err := store.PublishNamespace(ctx, remote); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)
	_, msgs := subscribeNS(t, dialClient(t, relayA), "video")
	requireNamespace(t, msgs, "cam")

	close(store.cut) // the watch goes down...
	if err := store.UnpublishNamespace(ctx, remote.Prefix, remote.RelayAddr); err != nil {
		t.Fatalf("UnpublishNamespace: %v", err)
	}
	requireNamespaceDone(t, msgs, "cam") // ...and its restart reconciles
}

// TestNamespace_RestartedWatchChangesOnlyWhatChanged: a restarted watch is
// reconciled against what the relay already knew. A namespace withdrawn while
// the watch was down is done, one added then is announced, and one still
// advertised causes nothing — no NAMESPACE_DONE followed by NAMESPACE again.
func TestNamespace_RestartedWatchChangesOnlyWhatChanged(t *testing.T) {
	t.Parallel()
	store := &cuttableWatchStore{MemoryStore: discovery.NewMemoryStore(), cut: make(chan struct{})}
	defer store.Close()
	ctx := t.Context()
	remote := func(field string) discovery.NamespaceInfo {
		return discovery.NamespaceInfo{Prefix: ns("video", field), RelayAddr: "relay-C"}
	}
	for _, f := range []string{"cam", "mic"} {
		if err := store.PublishNamespace(ctx, remote(f)); err != nil {
			t.Fatalf("PublishNamespace %s: %v", f, err)
		}
	}
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)
	_, msgs := subscribeNS(t, dialClient(t, relayA), "video")
	got := map[string]bool{}
	for range 2 {
		got[relaytest.FormatNamespace(nextMessage(t, msgs).(*message.Namespace).TrackNamespaceSuffix)] = true
	}
	if len(got) != 2 {
		t.Fatalf("initial NAMESPACEs %v, want cam and mic", got)
	}

	close(store.cut) // the watch goes down...
	if err := store.UnpublishNamespace(ctx, remote("mic").Prefix, "relay-C"); err != nil {
		t.Fatalf("UnpublishNamespace: %v", err)
	}
	if err := store.PublishNamespace(ctx, remote("screen")); err != nil {
		t.Fatalf("PublishNamespace screen: %v", err)
	}

	// ...and its restart reconciles: exactly these two, in either order.
	want := map[string]bool{"NAMESPACE_DONE mic": true, "NAMESPACE screen": true}
	for range 2 {
		var desc string
		switch m := nextMessage(t, msgs).(type) {
		case *message.Namespace:
			desc = "NAMESPACE " + relaytest.FormatNamespace(m.TrackNamespaceSuffix)
		case *message.NamespaceDone:
			desc = "NAMESPACE_DONE " + relaytest.FormatNamespace(m.TrackNamespaceSuffix)
		}
		if !want[desc] {
			t.Fatalf("after the restart got %q, want only NAMESPACE_DONE mic and NAMESPACE screen", desc)
		}
		delete(want, desc)
	}
	requireQuiet(t, msgs, "cam is still advertised")
}

// TestNamespace_LargeSeedNotReset: the bound is for a subscriber whose stream
// is blocked, not for a burst. One whose prefix covers more namespaces than
// the bound, all queued at once when it subscribes, receives them all.
func TestNamespace_LargeSeedNotReset(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	const advertised = 1500
	for i := range advertised {
		if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix: ns("video", strconv.Itoa(i)), RelayAddr: "relay-C",
		}); err != nil {
			t.Fatalf("PublishNamespace: %v", err)
		}
	}
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)
	var s *session.NamespaceSubscription
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(20 * time.Millisecond) {
		// Wait until the relay's watch has the whole snapshot.
		var msgs <-chan message.Message
		s, msgs = subscribeNS(t, dialClient(t, relayA), "video")
		n := 0
	count:
		for {
			select {
			case _, ok := <-msgs:
				if !ok {
					t.Fatalf("stream ended after %d NAMESPACEs; a reading subscriber must not be reset", n)
				}
				if n++; n == advertised {
					return
				}
			case <-time.After(500 * time.Millisecond):
				break count
			}
		}
		_ = s.Close()
		if time.Now().After(deadline) {
			t.Fatalf("got %d NAMESPACEs, want %d", n, advertised)
		}
	}
}

// TestNamespace_BlockedSubscriberReset: §10.19 "If the publisher is unable to
// send NAMESPACE or NAMESPACE_DONE messages in a timely manner because the
// SUBSCRIBE_NAMESPACE response stream is blocked by flow control, the
// publisher MAY reset the SUBSCRIBE_NAMESPACE response stream." A subscriber
// that stops reading has its stream reset once its queue reaches the bound,
// rather than the queue growing without one.
func TestNamespace_BlockedSubscriberReset(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)
	s, err := dialClient(
		t,
		relayA,
	).SubscribeNamespace(ctx, &message.SubscribeNamespace{TrackNamespacePrefix: ns("video")})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer s.Close()

	const advertised = 1500 // more than the relay queues for one subscriber
	for i := range advertised {
		if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix: ns("video", strconv.Itoa(i)), RelayAddr: "relay-C",
		}); err != nil {
			t.Fatalf("PublishNamespace: %v", err)
		}
	}
	// Blocked past the limit, the next event resets the stream.
	time.Sleep(1500 * time.Millisecond)
	if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix: ns("video", "late"), RelayAddr: "relay-C",
	}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	n := 0
	for {
		_, err := message.Parse(s.Stream)
		if err == nil {
			if n++; n >= advertised {
				t.Fatalf("all %d NAMESPACEs were delivered to a subscriber that was not reading", n)
			}
			continue
		}
		if errors.Is(err, io.EOF) {
			t.Fatalf("stream FINed after %d messages; want it reset", n)
		}
		return
	}
}

// TestNamespace_TricklingSubscriberReset: a subscriber that reads, but too
// slowly to keep up, is as blocked as one that stopped: every single write
// completes, yet the oldest unsent message waits longer and longer. Once
// enough are waiting long enough, the stream is reset.
func TestNamespace_TricklingSubscriberReset(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayA := startTestRelay(ctx, relay.Config{Discovery: store, RelayAddr: "relay-A"})
	defer relayA.stop(t)
	s, err := dialClient(
		t,
		relayA,
	).SubscribeNamespace(ctx, &message.SubscribeNamespace{TrackNamespacePrefix: ns("video")})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer s.Close()
	ended := make(chan error, 1)
	go func() {
		for {
			if _, err := message.Parse(s.Stream); err != nil {
				ended <- err
				return
			}
			time.Sleep(20 * time.Millisecond) // about 50 messages a second
		}
	}()

	for i := range 1500 {
		if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
			Prefix: ns("video", strconv.Itoa(i)), RelayAddr: "relay-C",
		}); err != nil {
			t.Fatalf("PublishNamespace: %v", err)
		}
	}
	time.Sleep(1500 * time.Millisecond)
	if err := store.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix: ns("video", "late"), RelayAddr: "relay-C",
	}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	select {
	case err := <-ended:
		if errors.Is(err, io.EOF) {
			t.Fatal("stream FINed; want it reset")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a subscriber trickling its reads was not reset")
	}
}
