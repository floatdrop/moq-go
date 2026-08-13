package relay_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// requestStreamWriteFault fails every relay write on a client's first request
// stream, and closes fired the first time it does. Tests wait on fired rather
// than on the client call it breaks: these faults hit the REQUEST_OK, which
// [session.Request.Reply] writes with no teardown of its own, so the client is
// left hanging and the hook is the only precise signal that the branch under
// test has run.
func requestStreamWriteFault() (fault sessiontest.FaultFunc, fired <-chan struct{}, writes func() int) {
	var (
		mu      sync.Mutex
		n       int
		ch      = make(chan struct{})
		closeCh sync.Once
	)
	return func(f sessiontest.FaultOp) error {
			if f.Op != sessiontest.OpStreamWrite || f.Stream != firstRequestStream {
				return nil
			}
			mu.Lock()
			n++
			mu.Unlock()
			closeCh.Do(func() { close(ch) })
			return errRejectWrite
		}, ch, func() int {
			mu.Lock()
			defer mu.Unlock()
			return n
		}
}

// awaitFault fails the test if the fault hook has not fired, meaning the
// branch under test never ran and whatever the test asserts afterwards proves
// nothing.
func awaitFault(t *testing.T, fired <-chan struct{}) {
	t.Helper()
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never attempted the write this test faults")
	}
}

// TestPublishNamespace_FailedRequestOKIsNotAdvertised pins what
// handlePublishNamespace does when it cannot deliver the REQUEST_OK: it
// returns, which runs the deferred UnregisterPublisher and — crucially —
// skips the §6.2 NAMESPACE fanout below it.
//
// That ordering is the whole point. A publisher that never received its
// REQUEST_OK does not believe it is publishing, so advertising its namespace
// to subscribers would announce a namespace nobody is serving. Deleting the
// `return` leaves the suite green everywhere else while doing exactly that.
func TestPublishNamespace_FailedRequestOKIsNotAdvertised(t *testing.T) {
	t.Parallel()
	fault, fired, _ := requestStreamWriteFault()
	l := newPipeListener()
	// Conn 2 is the faulted publisher: connectRelay's subscriber is 1, and the
	// healthy publisher dialled below is 3.
	l.faultFor = faultConn(2, fault)

	subSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	subStream, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer subStream.Close()

	// The faulted publisher never gets its REQUEST_OK, so this call hangs
	// until its context expires; run it detached and sync on the hook instead.
	faultedPub := dialAnotherClient(t, subSess)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = faultedPub.PublishNamespace(ctx, &message.PublishNamespace{
			Namespace: wire.TrackNamespace{[]byte("video"), []byte("faulted")},
		})
	}()
	awaitFault(t, fired)

	// A healthy publisher on a third conn. Its NAMESPACE must be the FIRST
	// thing the subscriber sees: if the faulted publisher had been advertised
	// despite its failed OK, "faulted" would already be queued ahead of it.
	okPub := dialAnotherClient(t, subSess)
	if _, err := okPub.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("ok")},
	}); err != nil {
		t.Fatalf("healthy PublishNamespace: %v", err)
	}

	got := relaytest.ReadNextMessage(t, subStream, time.After(2*time.Second))
	ns, isNS := got.(*message.Namespace)
	if !isNS {
		t.Fatalf("got %T, want *message.Namespace", got)
	}
	if len(ns.TrackNamespaceSuffix) != 1 || string(ns.TrackNamespaceSuffix[0]) != "ok" {
		t.Fatalf("first NAMESPACE suffix = %v, want [ok] — the publisher whose "+
			"REQUEST_OK failed was advertised anyway",
			relaytest.FormatNamespace(ns.TrackNamespaceSuffix))
	}
}

// TestSubscribeNamespace_FailedRequestOKLeavesNoSubscriber pins the other
// half of the reply-before-register ordering handleSubscribeNamespace
// documents: because the REQUEST_OK is written BEFORE RegisterSubscriber, a
// failed write must leave no subscriber behind at all.
//
// The assertion is that the relay never writes to that stream again. A
// registered-anyway subscriber would be picked up by MatchSubscribers and get
// the NAMESPACE fanout, which the hook would see as a second write.
func TestSubscribeNamespace_FailedRequestOKLeavesNoSubscriber(t *testing.T) {
	t.Parallel()
	fault, fired, writes := requestStreamWriteFault()
	l := newPipeListener()
	l.faultFor = faultConn(1, fault) // the faulted subscriber is connectRelay's client

	faultedSub, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = faultedSub.SubscribeNamespace(ctx, &message.SubscribeNamespace{
			TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
		})
	}()
	awaitFault(t, fired)

	// A healthy subscriber, used purely as the synchronisation point: once it
	// has seen both fanouts, the loops that would have written to the faulted
	// subscriber have run to completion.
	goodSub := dialAnotherClient(t, faultedSub)
	goodStream, err := goodSub.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("healthy SubscribeNamespace: %v", err)
	}
	defer goodStream.Close()

	pub := dialAnotherClient(t, faultedSub)
	pubStream, err := pub.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video"), []byte("cam1")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	if got := relaytest.ReadNextMessage(t, goodStream, time.After(2*time.Second)); !isNamespace(got) {
		t.Fatalf("healthy subscriber got %T, want *message.Namespace", got)
	}

	// NAMESPACE_DONE is emitted by a loop that runs after the NAMESPACE one,
	// so its arrival means the NAMESPACE fanout finished for every subscriber
	// — including the faulted one, had it been registered.
	if err := pubStream.Close(); err != nil {
		t.Fatalf("pubStream.Close: %v", err)
	}
	if got := relaytest.ReadNextMessage(t, goodStream, time.After(2*time.Second)); !isNamespaceDone(got) {
		t.Fatalf("healthy subscriber got %T, want *message.NamespaceDone", got)
	}

	if n := writes(); n != 1 {
		t.Errorf("relay attempted %d writes on the faulted subscriber's stream, want 1 "+
			"(only the REQUEST_OK) — it was registered despite the failed ack", n)
	}
}

func isNamespace(m message.Message) bool {
	_, ok := m.(*message.Namespace)
	return ok
}

func isNamespaceDone(m message.Message) bool {
	_, ok := m.(*message.NamespaceDone)
	return ok
}
