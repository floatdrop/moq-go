// Package quicconn adapts github.com/quic-go/quic-go's *quic.Conn to the
// transport-neutral session.Conn interface.
//
// This is the sole boundary in the moqt tree where quic-go's concrete types
// meet the session abstraction. Putting it in a dedicated subpackage lets the
// rest of pkg/moqt (and its tests) stay independent of quic-go's surface.
//
// quic-go uses typed-uint64 aliases (quic.StreamErrorCode,
// quic.ApplicationErrorCode) for error codes; session.Conn / SendStream /
// ReceiveStream use plain uint64. The wrappers below do the lossless
// conversion at each call site.
package quicconn

import (
	"context"
	"errors"
	"net"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// New wraps c so it satisfies session.Conn.
func New(c *quic.Conn) session.Conn { return &conn{q: c} }

// Compile-time satisfaction check.
var _ session.Conn = (*conn)(nil)

// conn holds a *quic.Conn by named field rather than embedding. Embedding
// would promote quic-go's CloseWithError(quic.ApplicationErrorCode, string)
// onto the wrapper; the session.Conn interface demands
// CloseWithError(uint64, string). Two methods of the same name with different
// signatures aren't allowed on a single Go type, so we delegate explicitly.
type conn struct{ q *quic.Conn }

func (c *conn) OpenUniStream() (session.SendStream, error) {
	s, err := c.q.OpenUniStream()
	if err != nil {
		if _, ok := errors.AsType[*quic.StreamLimitReachedError](err); ok {
			return nil, session.ErrNoStreamCredit
		}
		return nil, err
	}
	return &sendStream{s: s}, nil
}

func (c *conn) AcceptUniStream(ctx context.Context) (session.ReceiveStream, error) {
	s, err := c.q.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return &recvStream{s: s}, nil
}

// OpenStream opens a bidirectional stream without blocking. quic-go returns a
// *quic.StreamLimitReachedError when the peer's stream limit is exhausted; we
// map that onto session.ErrNoStreamCredit so callers can detect it
// transport-neutrally with errors.Is.
func (c *conn) OpenStream() (session.Stream, error) {
	s, err := c.q.OpenStream()
	if err != nil {
		if _, ok := errors.AsType[*quic.StreamLimitReachedError](err); ok {
			return nil, session.ErrNoStreamCredit
		}
		return nil, err
	}
	return &bidiStream{s: s}, nil
}

func (c *conn) AcceptStream(ctx context.Context) (session.Stream, error) {
	s, err := c.q.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &bidiStream{s: s}, nil
}

func (c *conn) CloseWithError(code uint64, reason string) error {
	return c.q.CloseWithError(quic.ApplicationErrorCode(code), reason)
}

func (c *conn) Context() context.Context { return c.q.Context() }

func (c *conn) SendDatagram(payload []byte) error {
	return c.q.SendDatagram(payload)
}

func (c *conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return c.q.ReceiveDatagram(ctx)
}

// sendStream wraps *quic.SendStream. Named field for the same reason as conn.
type sendStream struct{ s *quic.SendStream }

func (s *sendStream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s *sendStream) Close() error                { return s.s.Close() }
func (s *sendStream) CancelWrite(code uint64) {
	s.s.CancelWrite(quic.StreamErrorCode(code))
}

// SetReliableBoundary satisfies [session.ReliableResetStream] by forwarding to
// quic-go's RESET_STREAM_AT support. It is a no-op unless the peer enabled the
// extension (quic.Config.EnableStreamResetPartialDelivery).
func (s *sendStream) SetReliableBoundary() { s.s.SetReliableBoundary() }

// Context is cancelled when all data has been acknowledged by the peer or
// the stream is reset. quic-go's SendStream.Context() provides this directly.
func (s *sendStream) Context() context.Context { return s.s.Context() }

// recvStream wraps *quic.ReceiveStream.
type recvStream struct{ s *quic.ReceiveStream }

func (s *recvStream) Read(p []byte) (int, error) { return s.s.Read(p) }
func (s *recvStream) CancelRead(code uint64) {
	s.s.CancelRead(quic.StreamErrorCode(code))
}

// bidiStream wraps *quic.Stream.
type bidiStream struct{ s *quic.Stream }

func (s *bidiStream) Read(p []byte) (int, error)  { return s.s.Read(p) }
func (s *bidiStream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s *bidiStream) Close() error                { return s.s.Close() }
func (s *bidiStream) CancelRead(code uint64) {
	s.s.CancelRead(quic.StreamErrorCode(code))
}
func (s *bidiStream) CancelWrite(code uint64) {
	s.s.CancelWrite(quic.StreamErrorCode(code))
}

// Context is cancelled when all data has been acknowledged or the stream is
// reset. quic-go's Stream embeds SendStream which has Context().
func (s *bidiStream) Context() context.Context { return s.s.Context() }

// Listener adapts a *quic.Listener so it can be handed directly to the
// relay's accept loop. The relay's listener interface requires
// Accept(ctx) → session.Conn, Addr() → net.Addr, and Close() → error;
// this type satisfies it structurally without forcing this package to
// import pkg/relay.
//
// The caller owns the underlying *quic.Listener — its TLS config, ALPN
// selection ("moqt-20"), QUIC parameters, and listening socket. Close
// on the Listener forwards to the underlying *quic.Listener, which is
// also what the caller would call themselves on shutdown; both paths
// are equivalent.
type Listener struct{ ql *quic.Listener }

// NewListener wraps ql so it can be passed to relay.New.
//
// Typical wiring:
//
//	ql, err := quic.ListenAddr(":4433", tlsCfg, quicCfg)
//	if err != nil { … }
//	r := relay.New(quicconn.NewListener(ql), relay.Config{ … })
//	go r.Start(ctx)
func NewListener(ql *quic.Listener) *Listener { return &Listener{ql: ql} }

// Accept blocks until the next inbound *quic.Conn arrives, then wraps
// it via [New] into a session.Conn the relay can hand to session.Server.
// ctx cancellation propagates to the underlying Accept.
func (l *Listener) Accept(ctx context.Context) (session.Conn, error) {
	c, err := l.ql.Accept(ctx)
	if err != nil {
		return nil, err
	}
	return New(c), nil
}

// Addr returns the address the underlying quic-go listener is bound to.
func (l *Listener) Addr() net.Addr { return l.ql.Addr() }

// Close closes the underlying *quic.Listener. Subsequent Accept calls
// unblock with the quic-go close error.
func (l *Listener) Close() error { return l.ql.Close() }
