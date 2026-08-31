package relaynet

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"slices"
	"sync"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/session/wtconn"
)

// DualALPNs lists the ALPNs of both MOQT transport mappings, for a listener that
// serves them on one socket — see [Listen]. A TLS config built with these accepts
// a raw-QUIC client offering "moqt-NN" and an HTTP/3 client offering "h3"; each
// connection's negotiated ALPN then says which mapping it is.
var DualALPNs = slices.Concat(MOQTQUICALPNs, WebTransportALPNs)

// dualBacklog bounds the queue of accepted-but-not-yet-Accepted connections. Both
// halves feed it, and the relay drains it promptly; the bound only matters for a
// burst arriving faster than the accept loop consumes.
const dualBacklog = 16

// Listen serves both MOQT transport mappings on a single UDP socket: raw QUIC for
// peers and native clients that dial a moqt URI, and WebTransport (HTTP/3) at
// wtPath for anything dialing the https form of the same URI (§3.1.3, §3.1.4) —
// browsers included. tlsCfg must advertise [DualALPNs].
//
// This is what a relay behind a load balancer wants, and it is why no transport
// flag is needed: the two mappings differ only in ALPN, so one listener can offer
// both and decide per connection. Clients choose by URL scheme, peer relays keep
// dialing raw QUIC, and nothing has to agree deployment-wide.
//
// The returned listener owns the socket; Close releases it along with both halves.
// A connection whose ALPN is not "h3" is treated as raw QUIC: the ALPN set the
// handshake selected from is tlsCfg's, so nothing else can get that far.
//
// CheckOrigin accepts every origin, as [ListenWebTransport] does — see the
// package doc. Serving both mappings means a relay is reachable from a browser by
// default, so a deployment that cares about which pages may open sessions needs
// its own policy here.
//
// opts tune the QUIC config this listener serves on, independently of whatever a
// cross-relay [DialQUIC] uses — see [WithQUICConfig].
func Listen(addr, wtPath string, tlsCfg *tls.Config, logger *slog.Logger, opts ...Option) (*DualListener, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if wtPath == "" {
		// ServeMux panics on an empty pattern; fail at startup with a message
		// instead of taking the process down inside NewListener.
		return nil, fmt.Errorf("relaynet: empty WebTransport path (use %q for the default)", "/moq")
	}
	qcfg := quicConfig(opts)
	// Neither of these is the caller's to switch off here, whatever WithQUICConfig
	// asked for: webtransport.Server.ServeQUICConn checks them one at a time and
	// refuses the connection on the first one missing, so a listener lacking
	// either would silently serve only half of what it advertises. MOQT can use
	// both (§11.3, §11.4.3), but neither is what forces the hand here.
	qcfg.EnableDatagrams = true
	qcfg.EnableStreamResetPartialDelivery = true

	// Not ListenEarly: an early listener yields connections before the handshake
	// completes, which would break this listener's contract that ALPN is already
	// negotiated (the dispatch below reads it) and would have the relay write
	// SETUP as 0.5-RTT data to a peer whose certificate is unverified. 0-RTT would
	// need Config.Allow0RTT, which defaultQUICConfig deliberately leaves unset.
	ql, err := quic.ListenAddr(addr, tlsCfg, qcfg)
	if err != nil {
		return nil, fmt.Errorf("relaynet: listen %s: %w", addr, err)
	}

	mux := http.NewServeMux()
	if wtPath != "/" {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			logger.WarnContext(r.Context(), "webtransport: unmatched request",
				"method", r.Method, "host", r.Host, "path", r.URL.Path,
				"proto", r.Proto, "upgrade", r.Header.Get(":protocol"))
			http.NotFound(w, r)
		})
	}
	// This half is what the two re-asserted fields above are for; see there.
	h3 := &http3.Server{TLSConfig: tlsCfg, Handler: mux}
	webtransport.ConfigureHTTP3Server(h3)
	wts := &webtransport.Server{
		H3: h3,
		// The WebTransport sub-protocol, not the TLS ALPN, carries the draft
		// version for this mapping (§3.1) — same identifiers as raw QUIC so the
		// two version signals share one source.
		ApplicationProtocols: MOQTQUICALPNs,
		CheckOrigin:          func(*http.Request) bool { return true },
	}

	ctx, cancel := context.WithCancel(context.Background())
	l := &DualListener{
		ql:     ql,
		wts:    wts,
		conns:  make(chan session.Conn, dualBacklog),
		ctx:    ctx,
		cancel: cancel,
		log:    logger,
	}
	l.wt = wtconn.NewListener(wts, mux, wtPath, ql.Addr(), dualBacklog)

	go l.acceptQUIC()
	go l.pumpWebTransport()
	return l, nil
}

// DualListener is the [Listen] listener: one QUIC socket whose connections are
// split by negotiated ALPN into the raw-QUIC and WebTransport halves, then merged
// into one Accept queue so the relay cannot tell them apart.
type DualListener struct {
	ql  *quic.Listener
	wt  *wtconn.Listener
	wts *webtransport.Server

	conns  chan session.Conn
	ctx    context.Context
	cancel context.CancelFunc
	log    *slog.Logger

	closeOnce sync.Once
}

// acceptQUIC is the demultiplexer: HTTP/3 connections go to the WebTransport
// server, whose upgrade handler feeds the wtconn listener that pumpWebTransport
// drains; everything else is a raw-QUIC MOQT connection and is queued directly.
func (l *DualListener) acceptQUIC() {
	for {
		conn, err := l.ql.Accept(l.ctx)
		if err != nil {
			return // listener closed
		}
		if conn.ConnectionState().TLS.NegotiatedProtocol == http3.NextProtoH3 {
			go func() {
				// Returns when the HTTP/3 connection ends, which is routine.
				if err := l.wts.ServeQUICConn(conn); err != nil && l.ctx.Err() == nil {
					l.log.Debug("relaynet: http/3 connection ended", "err", err.Error())
				}
			}()
			continue
		}
		// The mapping is not recorded on the conn, but it stays recoverable by
		// type — quicconn and wtconn produce distinct implementations — which is
		// what a future §10.3.1.1/§10.3.1.2 check would need (PATH and AUTHORITY
		// MUST NOT be used over WebTransport).
		l.deliver(quicconn.New(conn))
	}
}

// pumpWebTransport moves upgraded WebTransport sessions onto the shared queue, so
// Accept has a single source regardless of transport.
func (l *DualListener) pumpWebTransport() {
	for {
		conn, err := l.wt.Accept(l.ctx)
		if err != nil {
			return // Close, or the wtconn listener shut down
		}
		l.deliver(conn)
	}
}

// deliver queues conn, or closes it if the listener is shutting down — a conn
// nobody will Accept must not be left believing it has a session.
func (l *DualListener) deliver(conn session.Conn) {
	select {
	case l.conns <- conn:
	case <-l.ctx.Done():
		_ = conn.CloseWithError(uint64(moqt.SessionNoError), "listener closed")
	}
}

// Accept returns the next connection from either transport. It satisfies the
// relay's Listener interface.
func (l *DualListener) Accept(ctx context.Context) (session.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-l.ctx.Done():
		return nil, net.ErrClosed
	}
}

// Addr returns the UDP address both transports are served on.
func (l *DualListener) Addr() net.Addr { return l.ql.Addr() }

// Close stops accepting new connections. Connections already accepted keep
// working, and the UDP socket stays open until the last of them ends — quic-go
// releases it once its transport has no connections left.
//
// That is load-bearing, not incidental: [relay.Relay.Stop] closes the listener as
// an early step and only then broadcasts GOAWAY and waits out the grace period
// (§10.4, §3.6). A Close that dropped the socket would kill every draining
// session with it, and no peer would ever see its GOAWAY.
//
// Close is idempotent and joins the failures of every step.
func (l *DualListener) Close() error {
	var err error
	l.closeOnce.Do(func() {
		l.cancel()
		err = errors.Join(l.wt.Close(), l.wts.Close(), l.ql.Close())
	})
	return err
}
