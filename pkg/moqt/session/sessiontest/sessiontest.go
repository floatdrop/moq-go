// Package sessiontest provides in-process helpers for testing MoQT session
// code without a real QUIC transport. NewConnPair returns two session.Conn
// endpoints backed by io.Pipes; streams opened on one end are accepted on
// the other. NewSessionPair goes one step further and performs the full SETUP
// handshake, returning two ready *session.Session values.
//
// The implementation deliberately mirrors a real QUIC stream's semantics
// where it matters for tests:
//
//   - Opening a unidirectional stream is purely local; the peer doesn't see
//     it until at least one byte is written (matching quic-go).
//   - CancelRead / CancelWrite unblock any in-flight Read / Write with an
//     error.
//   - CloseWithError cancels the shared connection context, which unblocks
//     any pending Accept on either end.
package sessiontest

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// NewSessionPair performs the MoQT SETUP handshake over an in-process conn
// pair and returns two ready Sessions — client (even Request IDs) and server
// (odd Request IDs). Both sessions are closed via tb.Cleanup when the test
// or benchmark ends.
//
// The parameter is testing.TB rather than *testing.T so the helper serves
// both tests and benchmarks. Because testing.TB does not expose Context()
// (that method lives only on *testing.T / *testing.B), the handshake context
// is managed internally and cancelled via tb.Cleanup.
func NewSessionPair(tb testing.TB) (client, server *session.Session) {
	tb.Helper()
	connA, connB := NewConnPair()

	ctx, cancel := context.WithCancel(context.Background())
	tb.Cleanup(cancel)

	var (
		wg           sync.WaitGroup
		aSess, bSess *session.Session
		aErr, bErr   error
	)
	wg.Go(func() {
		aSess, aErr = session.Client(ctx, connA)
	})
	wg.Go(func() {
		bSess, bErr = session.Server(ctx, connB)
	})
	wg.Wait()
	if aErr != nil {
		tb.Fatalf("sessiontest.NewSessionPair client: %v", aErr)
	}
	if bErr != nil {
		tb.Fatalf("sessiontest.NewSessionPair server: %v", bErr)
	}
	tb.Cleanup(func() {
		_ = aSess.Close(0, "")
		_ = bSess.Close(0, "")
	})
	return aSess, bSess
}

// NewConnPair returns two session.Conn endpoints wired together in-process.
// Both endpoints have unlimited outbound bidirectional-stream credit; use
// [NewConnPairWithLimits] to cap one or both sides for PUBLISH_BLOCKED-style
// stream-exhaustion testing.
func NewConnPair() (a, b session.Conn) {
	return NewConnPairWithLimits(-1, -1)
}

// NewConnPairWithLimits is [NewConnPair] with an explicit cap on each
// endpoint's outbound bidirectional-stream credit, modelling the peer's QUIC
// MAX_STREAMS limit. aBidiLimit caps how many bidi streams endpoint a may
// open; bBidiLimit does the same for endpoint b. A negative limit means
// unlimited. Once an endpoint's credit is exhausted, [pipeConn.OpenStream]
// returns [session.ErrNoStreamCredit] immediately — mirroring real QUIC, where
// a new stream cannot be opened until the peer raises the MAX_STREAMS limit.
func NewConnPairWithLimits(aBidiLimit, bBidiLimit int) (a, b session.Conn) {
	return newConnPair(aBidiLimit, bBidiLimit, 0)
}

// NewConnPairBuffered is [NewConnPair] but with each stream backed by a
// buffered [bufPipe] of bufSize bytes instead of a synchronous io.Pipe. The
// writer can run ahead by up to bufSize bytes before blocking, which decouples
// the producer and consumer goroutines so a relay/session throughput benchmark
// measures forwarding work rather than per-object goroutine scheduling. Both
// endpoints have unlimited outbound bidi-stream credit.
func NewConnPairBuffered(bufSize int) (a, b session.Conn) {
	return newConnPair(-1, -1, bufSize)
}

func newConnPair(aBidiLimit, bBidiLimit, bufSize int) (a, b session.Conn) {
	aUniToB := make(chan *uniStream, 4)
	bUniToA := make(chan *uniStream, 4)
	aBidiToB := make(chan *bidiStream, 4)
	bBidiToA := make(chan *bidiStream, 4)
	// Datagram channels: what A sends, B receives, and vice-versa.
	aDatagramToB := make(chan []byte, 16)
	bDatagramToA := make(chan []byte, 16)
	aCtx, aCancel := context.WithCancel(context.Background())
	bCtx, bCancel := context.WithCancel(context.Background())
	return &pipeConn{
			uniOut: aUniToB, uniIn: bUniToA,
			bidiOut: aBidiToB, bidiIn: bBidiToA,
			datagramOut: aDatagramToB, datagramIn: bDatagramToA,
			ctx: aCtx, ctxCancel: aCancel,
			bidiCredit: aBidiLimit,
			bufSize:    bufSize,
		},
		&pipeConn{
			uniOut: bUniToA, uniIn: aUniToB,
			bidiOut: bBidiToA, bidiIn: aBidiToB,
			datagramOut: bDatagramToA, datagramIn: aDatagramToB,
			ctx: bCtx, ctxCancel: bCancel,
			bidiCredit: bBidiLimit,
			bufSize:    bufSize,
		}
}

var errCancelled = errors.New("sessiontest: stream cancelled")
var errConnClosed = errors.New("sessiontest: connection closed")

// uniStream is a unidirectional pipe. The opener writes via w; the acceptor
// reads via r. The same struct satisfies both SendStream and ReceiveStream;
// each side gets back the appropriate interface, which constrains which
// methods they can call.
//
// ctx / ctxCancel implement Context(): the context is cancelled when Close()
// or CancelWrite() is called, signalling "all data committed" (or reset).
// For in-process pipes, data is synchronously delivered, so Close() is
// equivalent to "all data acknowledged".
type uniStream struct {
	r         pipeReadCloser
	w         pipeWriteCloser
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func newUniStream(bufSize int) *uniStream {
	r, w := newPipe(bufSize)
	ctx, cancel := context.WithCancel(context.Background())
	return &uniStream{r: r, w: w, ctx: ctx, ctxCancel: cancel}
}

func (s *uniStream) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *uniStream) Close() error {
	err := s.w.Close()
	s.ctxCancel() // signal "all data committed"
	return err
}
func (s *uniStream) CancelWrite(uint64) {
	_ = s.w.CloseWithError(errCancelled)
	s.ctxCancel() // signal reset
}
func (s *uniStream) Read(p []byte) (int, error) { return s.r.Read(p) }
func (s *uniStream) CancelRead(uint64)          { _ = s.r.CloseWithError(errCancelled) }

// Context is cancelled when Close() or CancelWrite() has been called,
// indicating the send side is done (either cleanly or via reset).
func (s *uniStream) Context() context.Context { return s.ctx }

// bidiStream is two io.Pipes wired so each end reads what the other writes.
// ctx / ctxCancel implement Context() on the send side.
type bidiStream struct {
	r         pipeReadCloser
	w         pipeWriteCloser
	ctx       context.Context
	ctxCancel context.CancelFunc
}

func newBidiStreamPair(bufSize int) (a, b *bidiStream) {
	aR, aW := newPipe(bufSize) // a writes, b reads
	bR, bW := newPipe(bufSize) // b writes, a reads
	aCtx, aCancel := context.WithCancel(context.Background())
	bCtx, bCancel := context.WithCancel(context.Background())
	return &bidiStream{r: bR, w: aW, ctx: aCtx, ctxCancel: aCancel},
		&bidiStream{r: aR, w: bW, ctx: bCtx, ctxCancel: bCancel}
}

func (s *bidiStream) Read(p []byte) (int, error)  { return s.r.Read(p) }
func (s *bidiStream) Write(p []byte) (int, error) { return s.w.Write(p) }
func (s *bidiStream) Close() error {
	err := s.w.Close()
	s.ctxCancel() // signal "all data committed"
	return err
}
func (s *bidiStream) CancelRead(uint64) { _ = s.r.CloseWithError(errCancelled) }
func (s *bidiStream) CancelWrite(uint64) {
	_ = s.w.CloseWithError(errCancelled)
	s.ctxCancel() // signal reset
}

// Context is cancelled when Close() or CancelWrite() has been called.
func (s *bidiStream) Context() context.Context { return s.ctx }

// cancellable is satisfied by both uniStream and bidiStream — anything the
// pipeConn hands out and needs to forcibly tear down on connection close.
type cancellable interface {
	CancelRead(uint64)
	CancelWrite(uint64)
}

type pipeConn struct {
	uniOut, uniIn           chan *uniStream
	bidiOut, bidiIn         chan *bidiStream
	datagramOut, datagramIn chan []byte

	ctx       context.Context
	ctxCancel context.CancelFunc

	mu      sync.Mutex
	closed  bool
	tracked []cancellable

	// bidiCredit caps how many outbound bidirectional streams this endpoint
	// may open, modelling the peer's QUIC MAX_STREAMS limit. A negative value
	// means unlimited. bidiUsed counts streams already opened; both are
	// guarded by mu.
	bidiCredit int
	bidiUsed   int

	// bufSize selects the per-stream pipe backing: 0 = synchronous io.Pipe,
	// >0 = a buffered bufPipe of that capacity (see [newPipe]).
	bufSize int
}

// reserveBidiCredit accounts for one outbound bidi stream against the cap.
// Returns false when the cap is set (non-negative) and already exhausted.
func (c *pipeConn) reserveBidiCredit() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bidiCredit >= 0 && c.bidiUsed >= c.bidiCredit {
		return false
	}
	c.bidiUsed++
	return true
}

// track records s so CloseWithError can cancel it. Returns false if the conn
// has already been closed, in which case the caller should cancel s itself.
func (c *pipeConn) track(s cancellable) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.tracked = append(c.tracked, s)
	return true
}

func (c *pipeConn) OpenUniStream() (session.SendStream, error) {
	s := newUniStream(c.bufSize)
	select {
	case c.uniOut <- s:
		if !c.track(s) {
			s.CancelRead(0)
			s.CancelWrite(0)
			return nil, errConnClosed
		}
		return s, nil
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

func (c *pipeConn) AcceptUniStream(ctx context.Context) (session.ReceiveStream, error) {
	select {
	case s := <-c.uniIn:
		if !c.track(s) {
			s.CancelRead(0)
			s.CancelWrite(0)
			return nil, errConnClosed
		}
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

// OpenStream is the non-blocking bidi open. When the credit cap is exhausted
// it returns session.ErrNoStreamCredit instead of blocking, mirroring
// quic-go's Conn.OpenStream / StreamLimitReachedError.
func (c *pipeConn) OpenStream() (session.Stream, error) {
	if !c.reserveBidiCredit() {
		return nil, session.ErrNoStreamCredit
	}
	mine, peers := newBidiStreamPair(c.bufSize)
	select {
	case c.bidiOut <- peers:
		if !c.track(mine) {
			mine.CancelRead(0)
			mine.CancelWrite(0)
			return nil, errConnClosed
		}
		return mine, nil
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

func (c *pipeConn) AcceptStream(ctx context.Context) (session.Stream, error) {
	select {
	case s := <-c.bidiIn:
		if !c.track(s) {
			s.CancelRead(0)
			s.CancelWrite(0)
			return nil, errConnClosed
		}
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

// SendDatagram delivers payload to the peer's datagramIn channel. The payload
// is copied so the caller may reuse the slice immediately.
func (c *pipeConn) SendDatagram(payload []byte) error {
	cp := make([]byte, len(payload))
	copy(cp, payload)
	select {
	case c.datagramOut <- cp:
		return nil
	case <-c.ctx.Done():
		return errConnClosed
	}
}

// ReceiveDatagram blocks until a datagram arrives from the peer or ctx / the
// connection is cancelled.
func (c *pipeConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case p := <-c.datagramIn:
		return p, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

// CloseWithError mirrors quic-go: cancelling the connection also forcibly
// tears down every stream the conn has handed out, so any in-flight Read or
// Write on those streams unblocks with an error.
func (c *pipeConn) CloseWithError(uint64, string) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	tracked := c.tracked
	c.tracked = nil
	c.mu.Unlock()

	c.ctxCancel()
	for _, s := range tracked {
		s.CancelRead(0)
		s.CancelWrite(0)
	}
	return nil
}

func (c *pipeConn) Context() context.Context { return c.ctx }
