package conference

import (
	"context"
	"net"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/memconn"
)

// InProcess is a relay.Listener whose connections are in-process
// [memconn.Pair]s, with [InProcess.Dial] as the participants' end. No QUIC,
// TLS or socket is involved, so a run over it puts only the relay and the
// session library on the CPU.
type InProcess struct {
	conns chan session.Conn
	done  chan struct{}
	once  sync.Once
}

// NewInProcess returns a listener with no connections yet.
func NewInProcess() *InProcess {
	return &InProcess{conns: make(chan session.Conn), done: make(chan struct{})}
}

// Dial connects a participant and completes its SETUP; it suits [Config.Dial].
func (l *InProcess) Dial(ctx context.Context) (*session.Session, error) {
	client, server := memconn.Pair(0)
	select {
	case l.conns <- server:
	case <-l.done:
		_ = client.CloseWithError(uint64(moqt.SessionNoError), "")
		return nil, net.ErrClosed
	case <-ctx.Done():
		_ = client.CloseWithError(uint64(moqt.SessionNoError), "")
		return nil, ctx.Err()
	}
	return session.Client(ctx, client)
}

// Accept implements relay.Listener.
func (l *InProcess) Accept(ctx context.Context) (session.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Addr implements relay.Listener; an in-process listener has none.
func (l *InProcess) Addr() net.Addr { return nil }

// Close implements relay.Listener.
func (l *InProcess) Close() error {
	l.once.Do(func() { close(l.done) })
	return nil
}
