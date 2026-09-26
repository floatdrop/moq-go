package relay_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// prioritySpyConn wraps a [session.Conn] so its outbound uni streams implement
// [session.PrioritizedSendStream] and record every SetSendPriority call.
type prioritySpyConn struct {
	session.Conn

	mu         sync.Mutex
	priorities []session.StreamPriority
}

func (c *prioritySpyConn) OpenUniStream() (session.SendStream, error) {
	inner, err := c.Conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return &prioritySpyStream{SendStream: inner, parent: c}, nil
}

func (c *prioritySpyConn) record(p session.StreamPriority) {
	c.mu.Lock()
	c.priorities = append(c.priorities, p)
	c.mu.Unlock()
}

func (c *prioritySpyConn) snapshot() []session.StreamPriority {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]session.StreamPriority, len(c.priorities))
	copy(out, c.priorities)
	return out
}

// prioritySpyStream records SetSendPriority on its parent prioritySpyConn.
type prioritySpyStream struct {
	session.SendStream

	parent *prioritySpyConn
}

func (s *prioritySpyStream) SetSendPriority(p session.StreamPriority) {
	s.parent.record(p)
}

// spyPipeListener is a pipeListener whose accepted (relay-side) conns are
// wrapped in a prioritySpyConn.
type spyPipeListener struct {
	inner *pipeListener
	mu    sync.Mutex
	// spies are the per-accepted-conn spies, in accept order. Tests use
	// LastSpy() to read the most recently accepted conn (= the
	// subscriber's server-side, since publisher is dialed first).
	spies []*prioritySpyConn
}

// newSpyPipeListener wraps a fresh pipeListener.
func newSpyPipeListener() *spyPipeListener { return &spyPipeListener{inner: newPipeListener()} }

func (l *spyPipeListener) Accept(ctx context.Context) (session.Conn, error) {
	raw, err := l.inner.Accept(ctx)
	if err != nil {
		return nil, err
	}
	spy := &prioritySpyConn{Conn: raw}
	l.mu.Lock()
	l.spies = append(l.spies, spy)
	l.mu.Unlock()
	return spy, nil
}
func (l *spyPipeListener) Addr() net.Addr { return l.inner.Addr() }
func (l *spyPipeListener) Close() error   { return l.inner.Close() }

func (l *spyPipeListener) Dial() (session.Conn, error) { return l.inner.Dial() }

// LastSpy returns the spy wrapping the most recently accepted server-side
// conn. In the test below the subscriber dials second, so this is the
// relay's view of the subscriber's session.
func (l *spyPipeListener) LastSpy() *prioritySpyConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.spies) == 0 {
		return nil
	}
	return l.spies[len(l.spies)-1]
}

// TestFanout_AppliesEffectivePriorityOnStreamOpen: the relay sets a downstream
// subgroup stream's send priority to the subscriber's SUBSCRIBER_PRIORITY
// before writing Objects.
func TestFanout_AppliesEffectivePriorityOnStreamOpen(t *testing.T) {
	t.Parallel()

	// Build a relay with a spy-capable listener.
	l := newSpyPipeListener()
	r := relay.New(l, relay.Config{GoawayTimeout: 50 * time.Millisecond})
	startErr := make(chan error, 1)
	go func() { startErr <- r.Start(t.Context()) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = r.Stop(ctx)
		<-startErr
	}()

	// Publisher: dial first.
	pubConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial publisher: %v", err)
	}
	pubSess, err := session.Client(t.Context(), pubConn)
	if err != nil {
		t.Fatalf("publisher session.Client: %v", err)
	}
	const publisherAlias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: publisherAlias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubReq.Close()

	// Subscriber: dial second. The listener's Accept side wraps the
	// server-side conn — that's the one the relay holds and from which
	// it opens downstream subgroup streams, so its SetSendPriority calls
	// land in the spy recorder.
	subConn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial subscriber: %v", err)
	}
	subSess, err := session.Client(t.Context(), subConn)
	if err != nil {
		t.Fatalf("subscriber session.Client: %v", err)
	}
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			message.SubscriberPriorityParam(42),
		},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer subReq.Close()

	// Drain the subscriber's accept-side in the background so the relay
	// can complete its OpenSubgroup synchronously.
	go drainAll(t.Context(), subSess)

	pubSubgroup, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     publisherAlias,
		GroupID:        0,
		SubgroupID:     0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := pubSubgroup.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 0,
		Payload:       []byte("p"),
	}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := pubSubgroup.Close(); err != nil {
		t.Fatalf("pubSubgroup.Close: %v", err)
	}

	// Give the relay's fanout a moment to open the downstream stream.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		spy := l.LastSpy()
		if spy == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if got := spy.snapshot(); len(got) >= 1 {
			if got[0].Subscriber != 42 {
				t.Fatalf("first SetSendPriority Subscriber = %d, want 42", got[0].Subscriber)
			}
			return // success
		}
		time.Sleep(10 * time.Millisecond)
	}
	var snap []session.StreamPriority
	if spy := l.LastSpy(); spy != nil {
		snap = spy.snapshot()
	}
	t.Fatalf("relay never called SetSendPriority on the downstream subgroup stream (snapshot=%v)", snap)
}
