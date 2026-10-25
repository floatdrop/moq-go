// Package memconn is an in-process [session.Conn]: two endpoints wired
// together in memory, so that one program can run both ends of a MoQT
// session, as a relay serving a session of its own process does
// ([relay.Relay.Connect]). It keeps a real connection's semantics where a
// session relies on them:
//
//   - Opening a stream never blocks (the Conn contract): a full accept queue
//     is [session.ErrNoStreamCredit], as an exhausted stream limit is.
//   - A datagram the peer has no room for is dropped, as QUIC may drop one.
//   - Closing either end closes the connection for both, as a CONNECTION_CLOSE
//     reaches the peer, and tears down every stream still open.
//
// Unlike [sessiontest]'s pipes, which it grew out of, it forgets each stream
// once the stream has ended, so a connection may last as long as its program.
package memconn

import (
	"context"
	"errors"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

const (
	// DefaultBufferSize is each stream direction's buffer, in bytes, for
	// [Pair]: the writer runs ahead of the reader by up to as much.
	DefaultBufferSize = 64 << 10
	// acceptQueue is how many streams an endpoint may have opened and its
	// peer not yet accepted; datagramQueue how many datagrams sent and not
	// yet received.
	acceptQueue   = 256
	datagramQueue = 1024
)

var (
	errCancelled  = errors.New("memconn: stream cancelled")
	errConnClosed = errors.New("memconn: connection closed")
)

// Pair returns two connected endpoints, each stream direction buffered by
// bufSize bytes (DefaultBufferSize when bufSize <= 0).
func Pair(bufSize int) (a, b session.Conn) {
	if bufSize <= 0 {
		bufSize = DefaultBufferSize
	}
	ctx, cancel := context.WithCancel(context.Background())
	shared := &shared{ctx: ctx, cancel: cancel, streams: map[cancellable]struct{}{}}
	aUni, bUni := make(chan *uniStream, acceptQueue), make(chan *uniStream, acceptQueue)
	aBidi, bBidi := make(chan *bidiStream, acceptQueue), make(chan *bidiStream, acceptQueue)
	aDgram, bDgram := make(chan []byte, datagramQueue), make(chan []byte, datagramQueue)
	return &conn{
			shared:   shared,
			bufSize:  bufSize,
			uniOut:   aUni,
			uniIn:    bUni,
			bidiOut:  aBidi,
			bidiIn:   bBidi,
			dgramOut: aDgram,
			dgramIn:  bDgram,
		},
		&conn{
			shared:   shared,
			bufSize:  bufSize,
			uniOut:   bUni,
			uniIn:    aUni,
			bidiOut:  bBidi,
			bidiIn:   aBidi,
			dgramOut: bDgram,
			dgramIn:  aDgram,
		}
}

// cancellable is what CloseWithError tears down: a stream still open.
type cancellable interface {
	CancelRead(uint64)
	CancelWrite(uint64)
}

// shared is what both endpoints of a pair have in common: the connection's
// lifetime and the streams open on it.
type shared struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	closed  bool
	streams map[cancellable]struct{}
}

// track records s until done, when it has ended; false if the connection is
// closed already, s then the caller's to cancel.
func (s *shared) track(c cancellable, done <-chan struct{}) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	s.streams[c] = struct{}{}
	go func() {
		select {
		case <-done:
		case <-s.ctx.Done():
			return
		}
		s.mu.Lock()
		delete(s.streams, c)
		s.mu.Unlock()
	}()
	return true
}

func (s *shared) close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	streams := s.streams
	s.streams = nil
	s.mu.Unlock()
	s.cancel()
	for c := range streams {
		c.CancelRead(0)
		c.CancelWrite(0)
	}
}

type conn struct {
	*shared

	bufSize int

	uniOut, uniIn     chan *uniStream
	bidiOut, bidiIn   chan *bidiStream
	dgramOut, dgramIn chan []byte
}

func (c *conn) OpenUniStream() (session.SendStream, error) {
	if c.ctx.Err() != nil {
		return nil, errConnClosed
	}
	s := newUniStream(c.bufSize)
	select {
	case c.uniOut <- s:
	default:
		return nil, session.ErrNoStreamCredit
	}
	if !c.track(s, s.done) {
		s.CancelRead(0)
		s.CancelWrite(0)
		return nil, errConnClosed
	}
	return s, nil
}

func (c *conn) AcceptUniStream(ctx context.Context) (session.ReceiveStream, error) {
	select {
	case s := <-c.uniIn:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

func (c *conn) OpenStream() (session.Stream, error) {
	if c.ctx.Err() != nil {
		return nil, errConnClosed
	}
	mine, peers := newBidiStreamPair(c.bufSize)
	select {
	case c.bidiOut <- peers:
	default:
		return nil, session.ErrNoStreamCredit
	}
	if !c.track(mine, mine.done) || !c.track(peers, peers.done) {
		mine.CancelRead(0)
		mine.CancelWrite(0)
		return nil, errConnClosed
	}
	return mine, nil
}

func (c *conn) AcceptStream(ctx context.Context) (session.Stream, error) {
	select {
	case s := <-c.bidiIn:
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

// SendDatagram hands a copy of payload to the peer, or drops it when the
// peer has datagramQueue of them not yet received.
func (c *conn) SendDatagram(payload []byte) error {
	if c.ctx.Err() != nil {
		return errConnClosed
	}
	select {
	case c.dgramOut <- append([]byte(nil), payload...):
	default: // dropped, as a datagram may be
	}
	return nil
}

func (c *conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	select {
	case p := <-c.dgramIn:
		return p, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.ctx.Done():
		return nil, errConnClosed
	}
}

// CloseWithError closes the connection for both endpoints.
func (c *conn) CloseWithError(uint64, string) error {
	c.close()
	return nil
}

func (c *conn) Context() context.Context { return c.ctx }

// uniStream is one direction's pipe: its opener writes, its acceptor reads.
// Its context, the send side's, ends with Close, CancelWrite or the
// acceptor's CancelRead (STOP_SENDING); done closes once both sides have
// ended, the stream forgotten then.
type uniStream struct {
	r      pipeReadCloser
	w      pipeWriteCloser
	ctx    context.Context
	cancel context.CancelFunc

	once     sync.Once
	readDone chan struct{}
	done     chan struct{}
}

func newUniStream(bufSize int) *uniStream {
	r, w := newPipe(bufSize)
	ctx, cancel := context.WithCancel(context.Background())
	s := &uniStream{r: r, w: w, ctx: ctx, cancel: cancel, readDone: make(chan struct{}), done: make(chan struct{})}
	go func() {
		<-ctx.Done()
		<-s.readDone
		close(s.done)
	}()
	return s
}

func (s *uniStream) Write(p []byte) (int, error) { return s.w.Write(p) }

func (s *uniStream) Close() error {
	err := s.w.Close()
	s.cancel()
	return err
}

func (s *uniStream) CancelWrite(uint64) {
	_ = s.w.CloseWithError(errCancelled)
	s.cancel()
}

func (s *uniStream) Context() context.Context { return s.ctx }

func (s *uniStream) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil {
		s.endRead()
	}
	return n, err
}

// CancelRead is the acceptor's STOP_SENDING, which ends the send side too.
func (s *uniStream) CancelRead(uint64) {
	_ = s.r.CloseWithError(errCancelled)
	s.cancel()
	s.endRead()
}

func (s *uniStream) endRead() { s.once.Do(func() { close(s.readDone) }) }

// bidiStream is one end of two pipes, each end reading what the other
// writes. Its context is its send side's; done closes once it has stopped
// both writing and reading.
type bidiStream struct {
	r          pipeReadCloser
	w          pipeWriteCloser
	ctx        context.Context
	cancel     context.CancelFunc
	peerCancel context.CancelFunc

	once     sync.Once
	readDone chan struct{}
	done     chan struct{}
}

func newBidiStreamPair(bufSize int) (a, b *bidiStream) {
	aR, aW := newPipe(bufSize) // a writes, b reads
	bR, bW := newPipe(bufSize) // b writes, a reads
	aCtx, aCancel := context.WithCancel(context.Background())
	bCtx, bCancel := context.WithCancel(context.Background())
	a = &bidiStream{
		r:          bR,
		w:          aW,
		ctx:        aCtx,
		cancel:     aCancel,
		peerCancel: bCancel,
		readDone:   make(chan struct{}),
		done:       make(chan struct{}),
	}
	b = &bidiStream{
		r:          aR,
		w:          bW,
		ctx:        bCtx,
		cancel:     bCancel,
		peerCancel: aCancel,
		readDone:   make(chan struct{}),
		done:       make(chan struct{}),
	}
	for _, s := range []*bidiStream{a, b} {
		go func() {
			<-s.ctx.Done()
			<-s.readDone
			close(s.done)
		}()
	}
	return a, b
}

func (s *bidiStream) Write(p []byte) (int, error) { return s.w.Write(p) }

func (s *bidiStream) Read(p []byte) (int, error) {
	n, err := s.r.Read(p)
	if err != nil {
		s.endRead()
	}
	return n, err
}

func (s *bidiStream) Close() error {
	err := s.w.Close()
	s.cancel()
	return err
}

// CancelRead is STOP_SENDING: it ends the peer's send side.
func (s *bidiStream) CancelRead(uint64) {
	_ = s.r.CloseWithError(errCancelled)
	s.peerCancel()
	s.endRead()
}

func (s *bidiStream) CancelWrite(uint64) {
	_ = s.w.CloseWithError(errCancelled)
	s.cancel()
}

func (s *bidiStream) Context() context.Context { return s.ctx }

func (s *bidiStream) endRead() { s.once.Do(func() { close(s.readDone) }) }
