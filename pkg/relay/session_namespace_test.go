package relay_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestPublishNamespace_AcceptedAndRegistered exercises the happy path: a
// PUBLISH_NAMESPACE arrives, the relay authorizes it, replies REQUEST_OK,
// and keeps the request stream alive until the publisher cancels.
func TestPublishNamespace_AcceptedAndRegistered(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	stream, err := clientSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam1")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	// Close the request stream; the relay's handler should observe the FIN
	// and unregister cleanly. The session itself remains alive.
	if err := stream.Close(); err != nil {
		t.Fatalf("stream.Close: %v", err)
	}
}

// TestSubscribeNamespace_AcceptedAndDeliversInitialNamespaces verifies the
// §6.1 catch-up rule: a subscriber that joins after publishers have already
// announced MUST receive a NAMESPACE for every matching publisher.
func TestSubscribeNamespace_AcceptedAndDeliversInitialNamespaces(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// First publisher: video/cam1.
	pubStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam1")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubStream.Close()

	// Open a separate session on the same in-process listener for the
	// subscriber. The first connectRelay registered its own pipeListener,
	// so we hand-roll a second session via session.Client over a fresh
	// conn pair.
	subSess := dialAnotherClient(t, pubSess)

	subStream, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer subStream.Close()

	// Expect exactly one NAMESPACE with suffix ("cam1",).
	deadline := time.After(2 * time.Second)
	got := relaytest.ReadNextMessage(t, subStream, deadline)
	ns, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("first message = %T, want *message.Namespace", got)
	}
	if len(ns.TrackNamespaceSuffix) != 1 || string(ns.TrackNamespaceSuffix[0]) != "cam1" {
		t.Fatalf("suffix = %v, want [cam1]", relaytest.FormatNamespace(ns.TrackNamespaceSuffix))
	}
}

// TestPublishNamespace_FanoutsToMatchingSubscriber drives the live forwarding
// path: a SUBSCRIBE_NAMESPACE is open when a PUBLISH_NAMESPACE arrives, the
// subscriber must receive a NAMESPACE message announcing the new publisher.
func TestPublishNamespace_FanoutsToMatchingSubscriber(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, subSess)

	pubStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam2")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	deadline := time.After(2 * time.Second)
	got := relaytest.ReadNextMessage(t, subStream, deadline)
	ns, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("got %T, want *message.Namespace", got)
	}
	if len(ns.TrackNamespaceSuffix) != 1 || string(ns.TrackNamespaceSuffix[0]) != "cam2" {
		t.Fatalf("suffix = %v, want [cam2]", relaytest.FormatNamespace(ns.TrackNamespaceSuffix))
	}

	// Closing the publisher's request stream must produce a NAMESPACE_DONE
	// on the subscriber's stream.
	if err := pubStream.Close(); err != nil {
		t.Fatalf("pubStream.Close: %v", err)
	}
	deadline = time.After(2 * time.Second)
	got = relaytest.ReadNextMessage(t, subStream, deadline)
	done, ok := got.(*message.NamespaceDone)
	if !ok {
		t.Fatalf("after pub close: got %T, want *message.NamespaceDone", got)
	}
	if len(done.TrackNamespaceSuffix) != 1 || string(done.TrackNamespaceSuffix[0]) != "cam2" {
		t.Fatalf("done suffix = %v, want [cam2]", relaytest.FormatNamespace(done.TrackNamespaceSuffix))
	}
}

// TestSubscribeTracks_AcceptedWithoutForwarding: SUBSCRIBE_TRACKS is
// registered and replied OK, but no PUBLISH messages flow yet (those
// arrive via handlePublish). The subscriber's stream stays open and
// silent until the subscriber cancels.
func TestSubscribeTracks_AcceptedWithoutForwarding(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}

	// Closing the subscriber stream must let the handler exit cleanly.
	if err := subStream.Close(); err != nil {
		t.Fatalf("subStream.Close: %v", err)
	}
}

// TestPublishNamespace_AuthDenialUsesPolicyCode pins the auth-precedence
// behaviour for the namespace dispatch arm: a custom policy that denies
// PUBLISH_NAMESPACE surfaces its own REQUEST_ERROR code, not REQUEST_OK.
func TestPublishNamespace_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: relay.Deny(moqt.RequestUnauthorized, "nope")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.publishNamespaceCalls.Load(); got != 1 {
		t.Errorf("publishNamespaceCalls = %d, want 1", got)
	}
}

// ----- shared helpers --------------------------------------------------

// dialAnotherClient opens a fresh client session on the same in-process
// relay that `existing` is connected to. The implementation reaches into
// the testing package's connectRelay scope via a side-channel: we keep a
// per-test cache of (relayInstance, listener) tuples in the test file.
//
// In practice the simplest approach is: a brand-new pipe listener and relay
// per call would defeat the purpose, so we instead store the listener on a
// global init in connectRelay. Refactor: we expose newListenerForExisting()
// via a package-level map keyed on the *session.Session of the first client.
//
// To keep the diff focused, the implementation uses a global mutex-guarded
// map updated by connectRelay below.
var (
	pipeListenerMu sync.Mutex
	pipeListenerOf = make(map[*session.Session]*pipeListener)
)

// dialAnotherClient accepts testing.TB so it serves both tests and
// benchmarks. The handshake uses context.Background() rather than a
// per-test context (testing.TB has no Context()); the returned session is
// registered on the relay's client tracker, so connectRelay's teardown
// closes it.
func dialAnotherClient(tb testing.TB, existing *session.Session) *session.Session {
	tb.Helper()
	pipeListenerMu.Lock()
	l, ok := pipeListenerOf[existing]
	pipeListenerMu.Unlock()
	if !ok {
		tb.Fatal("dialAnotherClient: no pipeListener registered for the existing session; was connectRelay used?")
	}
	conn, err := l.Dial()
	if err != nil {
		tb.Fatalf("listener.Dial: %v", err)
	}
	sess, err := session.Client(context.Background(), conn)
	if err != nil {
		tb.Fatalf("session.Client: %v", err)
	}
	// Register on the same client tracker as the primary session so
	// the teardown closes this dialled client too. See
	// connectRelay for why this matters across
	// -count=N runs.
	pipeListenerClientsMu.Lock()
	if tracker, ok := pipeListenerClients[existing]; ok {
		tracker.add(sess)
	}
	pipeListenerClientsMu.Unlock()
	return sess
}

// dialAnotherClientWithLimits is [dialAnotherClient] with explicit bidi-stream
// credit caps on the new connection. serverBidi bounds how many bidi streams
// the relay can open toward this client — set it low to force the relay's
// PUBLISH fan-out into the PUBLISH_SKIPPED (§10.20) path.
func dialAnotherClientWithLimits(t *testing.T, existing *session.Session, clientBidi, serverBidi int) *session.Session {
	t.Helper()
	pipeListenerMu.Lock()
	l, ok := pipeListenerOf[existing]
	pipeListenerMu.Unlock()
	if !ok {
		t.Fatal(
			"dialAnotherClientWithLimits: no pipeListener registered for the existing session; was connectRelay used?",
		)
	}
	conn, err := l.DialWithLimits(clientBidi, serverBidi)
	if err != nil {
		t.Fatalf("listener.DialWithLimits: %v", err)
	}
	sess, err := session.Client(t.Context(), conn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	pipeListenerClientsMu.Lock()
	if tracker, ok := pipeListenerClients[existing]; ok {
		tracker.add(sess)
	}
	pipeListenerClientsMu.Unlock()
	return sess
}

// TestNamespaceStreams_AnswerRequestUpdate pins §10.9 on the namespace
// request streams: the relay previously held them open with a drain that
// discarded follow-ups unparsed, so a peer's REQUEST_UPDATE was never
// answered (the peer blocked until its ctx expired) and its §10.1 Request ID
// was never accounted for. Both the PUBLISH_NAMESPACE and the
// SUBSCRIBE_NAMESPACE streams must now reply REQUEST_OK.
func TestNamespaceStreams_AnswerRequestUpdate(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	nsPub, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer nsPub.Close()

	subSess := dialAnotherClient(t, pubSess)
	nsSub, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer nsSub.Close()
	// Drain the initial NAMESPACE backlog announcement so UpdateRequest's
	// direct response read below cannot mistake it for the reply.
	if m, err := message.Parse(nsSub); err != nil {
		t.Fatalf("read initial NAMESPACE: %v", err)
	} else if _, ok := m.(*message.Namespace); !ok {
		t.Fatalf("initial message is %T, want *message.Namespace", m)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := pubSess.UpdateRequest(ctx, nsPub.Stream, nil); err != nil {
		t.Fatalf("PUBLISH_NAMESPACE REQUEST_UPDATE unanswered (§10.9): %v", err)
	}
	if _, err := subSess.UpdateRequest(ctx, nsSub.Stream, nil); err != nil {
		t.Fatalf("SUBSCRIBE_NAMESPACE REQUEST_UPDATE unanswered (§10.9): %v", err)
	}
}
