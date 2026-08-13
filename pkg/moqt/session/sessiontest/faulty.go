package sessiontest

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// Op identifies the [session.Conn] or [session.Stream] operation a [FaultFunc]
// is being consulted about.
type Op int

const (
	OpOpenStream Op = iota
	OpOpenUniStream
	OpAcceptStream
	OpAcceptUniStream
	OpSendDatagram
	OpReceiveDatagram
	OpStreamWrite
	OpStreamRead
	OpStreamClose
)

// numOps sizes the per-Op counter arrays. It is deliberately an untyped
// constant rather than a trailing iota member: as an Op it would be a phantom
// enum value every exhaustive switch had to handle.
const numOps = int(OpStreamClose) + 1

func (o Op) String() string {
	switch o {
	case OpOpenStream:
		return "OpenStream"
	case OpOpenUniStream:
		return "OpenUniStream"
	case OpAcceptStream:
		return "AcceptStream"
	case OpAcceptUniStream:
		return "AcceptUniStream"
	case OpSendDatagram:
		return "SendDatagram"
	case OpReceiveDatagram:
		return "ReceiveDatagram"
	case OpStreamWrite:
		return "StreamWrite"
	case OpStreamRead:
		return "StreamRead"
	case OpStreamClose:
		return "StreamClose"
	}
	return fmt.Sprintf("Op(%d)", int(o))
}

// FaultOp describes the operation a [FaultFunc] is being consulted about.
type FaultOp struct {
	// Op is the operation about to be performed.
	Op Op

	// Stream is the ordinal of the stream the operation is on, numbered from
	// 1 in the order the conn handed streams out. Unidirectional and
	// bidirectional streams share the one sequence, and both opening and
	// accepting allocate from it. Stream is 0 for connection-level operations
	// (the opens and accepts themselves, and the datagram calls).
	//
	// Ordinals are only stable when the test controls the order streams are
	// created in. Code under test that opens streams from several goroutines
	// — a relay fanning out to subscribers, for one — does not give that;
	// match on Buf there instead.
	Stream int

	// N counts occurrences of this Op, from 1: per stream for the stream
	// operations, per conn for the connection-level ones.
	N int

	// Buf is the caller's buffer for OpStreamWrite and OpStreamRead, and the
	// payload for OpSendDatagram; nil for every other Op. On OpStreamRead it
	// is the destination buffer, which the read has not filled yet — its
	// length is the size of the read, not data. Do not retain or modify it.
	Buf []byte
}

// FaultFunc is consulted before each wrapped operation. Returning a non-nil
// error makes the operation fail with that error instead of being performed;
// returning nil lets it through untouched.
//
// It runs on whichever goroutine drives the operation — for a relay under
// test, several at once — so it must be safe for concurrent use.
type FaultFunc func(FaultOp) error

// Faulty wraps c so fault is consulted before every operation on the conn and
// on every stream the conn hands out, letting a test make a chosen write, open
// or read fail. It exists to reach the error branches a healthy in-process pipe
// never takes: the "reply failed" and "write failed" paths that in production
// run when a peer's transport goes bad.
//
// Faults are injected in front of the wrapped operation, so a failed one never
// reaches the underlying conn: a failed Write puts no bytes on the stream, and
// a failed OpenStream consumes no stream credit.
//
// Two limitations, both deliberate:
//
//   - A failed Write reports (0, err). Real QUIC can fail part-way through and
//     report a short write; nothing in this tree distinguishes the two, so the
//     wrapper does not model it.
//   - Wrapped streams do not forward the optional [session.PrioritizedSendStream]
//     and [session.ReliableResetStream] interfaces. No sessiontest stream
//     implements either, and silently dropping §7.2 priority or RESET_STREAM_AT
//     would be a confusing way to find that out, so Faulty panics rather than
//     wrap a stream that does.
func Faulty(c session.Conn, fault FaultFunc) session.Conn {
	fc := &faultyConn{Conn: c}
	fc.fault = fault
	return fc
}

// FailNth returns a [FaultFunc] failing the nth occurrence of op with err,
// counted from 1 across the whole conn. Every other operation succeeds.
func FailNth(op Op, n int, err error) FaultFunc {
	var seen atomic.Int64
	return func(f FaultOp) error {
		if f.Op != op {
			return nil
		}
		if seen.Add(1) == int64(n) {
			return err
		}
		return nil
	}
}

// FailAll returns a [FaultFunc] failing every occurrence of op with err.
func FailAll(op Op, err error) FaultFunc {
	return func(f FaultOp) error {
		if f.Op != op {
			return nil
		}
		return err
	}
}

// faultyCounter holds the fault state shared by the conn and its streams: the
// hook, which stream this is (0 for the conn itself), and how many times each
// Op has been seen here. It contains atomics, so it is embedded and filled in
// place — never copied.
type faultyCounter struct {
	fault  FaultFunc
	stream int
	counts [numOps]atomic.Int64
}

func (f *faultyCounter) check(op Op, buf []byte) error {
	return f.fault(FaultOp{
		Op:     op,
		Stream: f.stream,
		N:      int(f.counts[op].Add(1)),
		Buf:    buf,
	})
}

type faultyConn struct {
	session.Conn
	faultyCounter

	streams atomic.Int64 // stream ordinal allocator
}

func (c *faultyConn) OpenStream() (session.Stream, error) {
	if err := c.check(OpOpenStream, nil); err != nil {
		return nil, err
	}
	s, err := c.Conn.OpenStream()
	if err != nil {
		return nil, err
	}
	return c.newStream(s), nil
}

func (c *faultyConn) AcceptStream(ctx context.Context) (session.Stream, error) {
	if err := c.check(OpAcceptStream, nil); err != nil {
		return nil, err
	}
	s, err := c.Conn.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return c.newStream(s), nil
}

func (c *faultyConn) OpenUniStream() (session.SendStream, error) {
	if err := c.check(OpOpenUniStream, nil); err != nil {
		return nil, err
	}
	s, err := c.Conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	assertPlainSend(s)
	fs := &faultySendStream{SendStream: s}
	c.initCounter(&fs.faultyCounter)
	return fs, nil
}

func (c *faultyConn) AcceptUniStream(ctx context.Context) (session.ReceiveStream, error) {
	if err := c.check(OpAcceptUniStream, nil); err != nil {
		return nil, err
	}
	s, err := c.Conn.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	fs := &faultyRecvStream{ReceiveStream: s}
	c.initCounter(&fs.faultyCounter)
	return fs, nil
}

func (c *faultyConn) SendDatagram(payload []byte) error {
	if err := c.check(OpSendDatagram, payload); err != nil {
		return err
	}
	return c.Conn.SendDatagram(payload)
}

func (c *faultyConn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	if err := c.check(OpReceiveDatagram, nil); err != nil {
		return nil, err
	}
	return c.Conn.ReceiveDatagram(ctx)
}

func (c *faultyConn) newStream(s session.Stream) *faultyStream {
	assertPlainSend(s)
	fs := &faultyStream{Stream: s}
	c.initCounter(&fs.faultyCounter)
	return fs
}

// initCounter fills a stream's fault state in place and allocates its ordinal.
func (c *faultyConn) initCounter(fc *faultyCounter) {
	fc.fault = c.fault
	fc.stream = int(c.streams.Add(1))
}

// assertPlainSend panics if s implements one of the optional SendStream
// interfaces the wrappers cannot forward. See [Faulty] for why this is loud
// rather than silent.
func assertPlainSend(s any) {
	switch s.(type) {
	case session.PrioritizedSendStream:
		panic("sessiontest.Faulty: refusing to wrap a session.PrioritizedSendStream — " +
			"the wrapper cannot forward SetSendPriority")
	case session.ReliableResetStream:
		panic("sessiontest.Faulty: refusing to wrap a session.ReliableResetStream — " +
			"the wrapper cannot forward SetReliableBoundary")
	}
}

type faultyStream struct {
	session.Stream
	faultyCounter
}

func (s *faultyStream) Write(p []byte) (int, error) {
	if err := s.check(OpStreamWrite, p); err != nil {
		return 0, err
	}
	return s.Stream.Write(p)
}

func (s *faultyStream) Read(p []byte) (int, error) {
	if err := s.check(OpStreamRead, p); err != nil {
		return 0, err
	}
	return s.Stream.Read(p)
}

func (s *faultyStream) Close() error {
	if err := s.check(OpStreamClose, nil); err != nil {
		return err
	}
	return s.Stream.Close()
}

type faultySendStream struct {
	session.SendStream
	faultyCounter
}

func (s *faultySendStream) Write(p []byte) (int, error) {
	if err := s.check(OpStreamWrite, p); err != nil {
		return 0, err
	}
	return s.SendStream.Write(p)
}

func (s *faultySendStream) Close() error {
	if err := s.check(OpStreamClose, nil); err != nil {
		return err
	}
	return s.SendStream.Close()
}

type faultyRecvStream struct {
	session.ReceiveStream
	faultyCounter
}

func (s *faultyRecvStream) Read(p []byte) (int, error) {
	if err := s.check(OpStreamRead, p); err != nil {
		return 0, err
	}
	return s.ReceiveStream.Read(p)
}
