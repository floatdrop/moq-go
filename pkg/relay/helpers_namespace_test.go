package relay_test

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// Helpers for namespace publishing, SUBSCRIBE_NAMESPACE and the messages on
// request streams.

// publishNS sends PUBLISH_NAMESPACE for the namespace fields from sess.
func publishNS(t *testing.T, sess *session.Session, fields ...string) *session.NamespacePublication {
	t.Helper()
	p, err := sess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns(fields...)})
	if err != nil {
		t.Fatalf("PublishNamespace %v: %v", fields, err)
	}
	return p
}

// subscribeNS sends SUBSCRIBE_NAMESPACE for the prefix fields and returns the
// subscription with the messages read from its stream.
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

// readvertise publishes info into store every 20ms until the returned stop,
// which waits for the last publish, is called (it also runs at cleanup). A
// relay's Discovery watch registers asynchronously in Start and MemoryStore
// does not replay history to new watchers, so one publish can go unseen.
func readvertise(t *testing.T, store *discovery.MemoryStore, info discovery.NamespaceInfo) (stop func()) {
	t.Helper()
	quit := make(chan struct{})
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		tick := time.NewTicker(20 * time.Millisecond)
		defer tick.Stop()
		for {
			_ = store.PublishNamespace(t.Context(), info)
			select {
			case <-quit:
				return
			case <-tick.C:
			}
		}
	}()
	stop = sync.OnceFunc(func() {
		close(quit)
		<-exited
	})
	t.Cleanup(stop)
	return stop
}

// Subscribing.

// streamMessages delivers the control messages read from stream until it
// ends, then closes the channel.
func streamMessages(t *testing.T, stream session.Stream) <-chan message.Message {
	t.Helper()
	out := make(chan message.Message, 16)
	go func() {
		defer close(out)
		for {
			m, err := message.Parse(stream)
			if err != nil {
				return
			}
			out <- m
		}
	}()
	return out
}

// nextMessage returns the next message from msgs, failing after 2s or if the
// stream ended.
func nextMessage(t *testing.T, msgs <-chan message.Message) message.Message {
	t.Helper()
	select {
	case m, ok := <-msgs:
		if !ok {
			t.Fatal("stream ended")
		}
		return m
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for a message")
	}
	return nil
}

// isNamespace reports whether m is a NAMESPACE.
func isNamespace(m message.Message) bool {
	_, ok := m.(*message.Namespace)
	return ok
}

// isNamespaceDone reports whether m is a NAMESPACE_DONE.
func isNamespaceDone(m message.Message) bool {
	_, ok := m.(*message.NamespaceDone)
	return ok
}

// requireQuiet fails if a message arrives on msgs within 300ms.
func requireQuiet(t *testing.T, msgs <-chan message.Message, what string) {
	t.Helper()
	select {
	case m := <-msgs:
		t.Fatalf("%s: unexpected %T %+v", what, m, m)
	case <-time.After(300 * time.Millisecond):
	}
}

// requireNamespace requires the next message to be NAMESPACE for suffix.
func requireNamespace(t *testing.T, msgs <-chan message.Message, suffix ...string) {
	t.Helper()
	m := nextMessage(t, msgs)
	n, ok := m.(*message.Namespace)
	if !ok || relaytest.FormatNamespace(n.TrackNamespaceSuffix) != relaytest.FormatNamespace(ns(suffix...)) {
		t.Fatalf("got %T %+v, want NAMESPACE %v", m, m, suffix)
	}
}

// requireNamespaceDone requires the next message to be NAMESPACE_DONE for
// suffix.
func requireNamespaceDone(t *testing.T, msgs <-chan message.Message, suffix ...string) {
	t.Helper()
	m := nextMessage(t, msgs)
	d, ok := m.(*message.NamespaceDone)
	if !ok || relaytest.FormatNamespace(d.TrackNamespaceSuffix) != relaytest.FormatNamespace(ns(suffix...)) {
		t.Fatalf("got %T %+v, want NAMESPACE_DONE %v", m, m, suffix)
	}
}
