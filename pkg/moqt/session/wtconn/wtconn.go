// Package wtconn adapts github.com/quic-go/webtransport-go's *webtransport.Session
// to the transport-neutral session.Conn interface.
//
// This is the WebTransport counterpart of the quicconn package. It is the sole
// boundary in the moqt tree where webtransport-go's concrete types meet the
// session abstraction. Putting it in a dedicated subpackage lets the rest of
// pkg/moqt (and its tests) stay independent of webtransport-go's surface.
//
// webtransport-go uses webtransport.StreamErrorCode (uint32) for stream-level
// error codes and webtransport.SessionErrorCode (uint32) for session-level
// error codes; session.Conn / SendStream / ReceiveStream use plain uint64.
// The wrappers below do the narrowing conversion at each call site. MoQT
// error codes fit comfortably in 32 bits (the largest defined code is 0x34),
// so no information is lost in practice.
package wtconn

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// New wraps s so it satisfies session.Conn.
func New(s *webtransport.Session) session.Conn { return &conn{s: s} }

// Compile-time satisfaction check.
var _ session.Conn = (*conn)(nil)

// conn holds a *webtransport.Session by named field rather than embedding.
// Embedding would promote webtransport's CloseWithError(SessionErrorCode, string)
// onto the wrapper; the session.Conn interface demands CloseWithError(uint64, string).
// Two methods of the same name with different signatures aren't allowed on a
// single Go type, so we delegate explicitly.
type conn struct{ s *webtransport.Session }

func (c *conn) OpenUniStream() (session.SendStream, error) {
	s, err := c.s.OpenUniStream()
	if err != nil {
		if _, ok := errors.AsType[*quic.StreamLimitReachedError](err); ok {
			return nil, session.ErrNoStreamCredit
		}
		return nil, err
	}
	return &sendStream{s: s}, nil
}

func (c *conn) AcceptUniStream(ctx context.Context) (session.ReceiveStream, error) {
	s, err := c.s.AcceptUniStream(ctx)
	if err != nil {
		return nil, err
	}
	return &recvStream{s: s}, nil
}

// OpenStream opens a bidirectional stream without blocking. webtransport-go's
// OpenStream delegates to the underlying *quic.Conn, so an exhausted stream
// limit surfaces as a *quic.StreamLimitReachedError; we map that onto
// session.ErrNoStreamCredit for transport-neutral detection.
func (c *conn) OpenStream() (session.Stream, error) {
	s, err := c.s.OpenStream()
	if err != nil {
		if _, ok := errors.AsType[*quic.StreamLimitReachedError](err); ok {
			return nil, session.ErrNoStreamCredit
		}
		return nil, err
	}
	return &bidiStream{s: s}, nil
}

func (c *conn) AcceptStream(ctx context.Context) (session.Stream, error) {
	s, err := c.s.AcceptStream(ctx)
	if err != nil {
		return nil, err
	}
	return &bidiStream{s: s}, nil
}

func (c *conn) CloseWithError(code uint64, reason string) error {
	//nolint:gosec // G115: MoQT session error codes fit uint32 (WebTransport's error-code width).
	return c.s.CloseWithError(webtransport.SessionErrorCode(code), reason)
}

func (c *conn) Context() context.Context { return c.s.Context() }

func (c *conn) SendDatagram(payload []byte) error {
	return c.s.SendDatagram(payload)
}

func (c *conn) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	return c.s.ReceiveDatagram(ctx)
}

// sendStream wraps *webtransport.SendStream. Named field for the same reason
// as conn.
type sendStream struct{ s *webtransport.SendStream }

func (s *sendStream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s *sendStream) Close() error                { return s.s.Close() }
func (s *sendStream) CancelWrite(code uint64) {
	//nolint:gosec // G115: MoQT stream error codes fit uint32 (WebTransport's error-code width).
	s.s.CancelWrite(webtransport.StreamErrorCode(code))
}

// Context is cancelled when all data has been acknowledged by the peer or
// the stream is reset. webtransport-go's SendStream.Context() provides this
// directly.
func (s *sendStream) Context() context.Context { return s.s.Context() }

// recvStream wraps *webtransport.ReceiveStream.
type recvStream struct{ s *webtransport.ReceiveStream }

func (s *recvStream) Read(p []byte) (int, error) { return s.s.Read(p) }
func (s *recvStream) CancelRead(code uint64) {
	//nolint:gosec // G115: MoQT stream error codes fit uint32 (WebTransport's error-code width).
	s.s.CancelRead(webtransport.StreamErrorCode(code))
}

// bidiStream wraps *webtransport.Stream.
type bidiStream struct{ s *webtransport.Stream }

func (s *bidiStream) Read(p []byte) (int, error)  { return s.s.Read(p) }
func (s *bidiStream) Write(p []byte) (int, error) { return s.s.Write(p) }
func (s *bidiStream) Close() error                { return s.s.Close() }
func (s *bidiStream) CancelRead(code uint64) {
	//nolint:gosec // G115: MoQT stream error codes fit uint32 (WebTransport's error-code width).
	s.s.CancelRead(webtransport.StreamErrorCode(code))
}
func (s *bidiStream) CancelWrite(code uint64) {
	//nolint:gosec // G115: MoQT stream error codes fit uint32 (WebTransport's error-code width).
	s.s.CancelWrite(webtransport.StreamErrorCode(code))
}

// Context is cancelled when all data has been acknowledged or the stream is
// reset. webtransport-go's Stream embeds SendStream which has Context().
func (s *bidiStream) Context() context.Context { return s.s.Context() }

// defaultBacklog bounds the pending-session queue used by [Listener].
// 16 mirrors the depth of the in-process sessiontest queue.
const defaultBacklog = 16

// Listener adapts a *webtransport.Server so it can be handed directly
// to the relay's accept loop. WebTransport sessions arrive via HTTP/3
// handler invocations, not via a synchronous Accept on a socket, so
// the listener registers a path handler on the caller's *http.ServeMux
// and bridges accepted sessions through a buffered channel.
//
// The caller owns:
//
//   - The *webtransport.Server (typically constructed with
//     [webtransport.ConfigureHTTP3Server] on the underlying
//     *http3.Server).
//   - The HTTP/3 server's lifecycle: ListenAndServe / Serve on the
//     desired socket, and Close on shutdown. [Listener.Close] only
//     stops Accept from yielding new sessions so the relay's accept
//     loop unwinds — it does NOT close the *webtransport.Server.
//
// The Listener type satisfies relay.Listener structurally without
// importing pkg/relay.
type Listener struct {
	server *webtransport.Server
	addr   net.Addr
	queue  chan session.Conn

	closeOnce sync.Once
	done      chan struct{}
}

// NewListener registers a WebTransport upgrade handler at path on mux
// and returns a Listener suitable for relay.New.
//
//   - server: the configured *webtransport.Server. Upgrade is called
//     on this server for every inbound request that hits path.
//   - mux: the HTTP/3 server's request mux. The Listener does NOT
//     mount its own mux so the caller can multiplex MOQT-over-
//     WebTransport with other HTTP/3 endpoints on the same server.
//   - path: the HTTP/3 path the WebTransport CONNECT must target
//     (e.g. "/moq").
//   - addr: the address the Listener reports via [Listener.Addr];
//     pass nil if you don't need it (the relay only uses it for
//     log lines).
//   - queueSize: bounds the pending-session backlog before the
//     upgrade handler starts dropping sessions on the floor. Pass
//     0 for the package default ([defaultBacklog]).
//
// Typical wiring:
//
//	h3 := &http3.Server{TLSConfig: tlsCfg}
//	webtransport.ConfigureHTTP3Server(h3)
//	wts := &webtransport.Server{H3: h3, CheckOrigin: …}
//	mux := http.NewServeMux()
//	udpConn, _ := net.ListenPacket("udp", ":4433")
//	wts.H3.Handler = mux
//
//	listener := wtconn.NewListener(wts, mux, "/moq", udpConn.LocalAddr(), 0)
//	r := relay.New(listener, relay.Config{ … })
//
//	go wts.Serve(udpConn)
//	go r.Start(ctx)
func NewListener(
	server *webtransport.Server,
	mux *http.ServeMux,
	path string,
	addr net.Addr,
	queueSize int,
) *Listener {
	if queueSize <= 0 {
		queueSize = defaultBacklog
	}
	l := &Listener{
		server: server,
		addr:   addr,
		queue:  make(chan session.Conn, queueSize),
		done:   make(chan struct{}),
	}
	mux.HandleFunc(path, l.upgrade)
	return l
}

// upgrade is the HTTP/3 handler the Listener registers on the mux.
// It performs the WebTransport upgrade and hands the resulting
// *webtransport.Session to Accept via the bounded queue. If Accept is
// not draining (closed Listener or a burst exceeding queueSize), the
// freshly-accepted session is closed immediately so the client sees
// the failure rather than hanging.
func (l *Listener) upgrade(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	slog.DebugContext(ctx, "wtconn: upgrade request",
		"remote", r.RemoteAddr, "method", r.Method, "path", r.URL.Path,
		"proto", r.Proto, "origin", r.Header.Get("Origin"))
	sess, err := l.server.Upgrade(w, r)
	if err != nil {
		slog.DebugContext(ctx, "wtconn: upgrade failed", "remote", r.RemoteAddr, "err", err)
		return
	}
	slog.DebugContext(ctx, "wtconn: upgrade ok",
		"remote", r.RemoteAddr, "wt_protocol", sess.SessionState().ApplicationProtocol)
	select {
	case l.queue <- New(sess):
	case <-l.done:
		_ = sess.CloseWithError(0, "listener closed")
	default:
		slog.WarnContext(ctx, "wtconn: dropping session: accept backlog full", "remote", r.RemoteAddr)
		_ = sess.CloseWithError(0, "accept backlog full")
	}
}

// Accept blocks until the next upgraded WebTransport session arrives,
// then returns it as a session.Conn. ctx cancellation and Close both
// unblock Accept.
func (l *Listener) Accept(ctx context.Context) (session.Conn, error) {
	select {
	case c := <-l.queue:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.done:
		return nil, net.ErrClosed
	}
}

// Addr returns the address the caller passed to NewListener, or nil
// if none was provided.
func (l *Listener) Addr() net.Addr { return l.addr }

// Close signals the Listener to stop yielding new sessions. The
// underlying *webtransport.Server keeps running; closing it is the
// caller's responsibility.
//
// Close is idempotent. Subsequent Accept calls return [net.ErrClosed].
func (l *Listener) Close() error {
	l.closeOnce.Do(func() { close(l.done) })
	return nil
}
