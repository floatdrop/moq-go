package wtconn

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/url"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
	"github.com/quic-go/webtransport-go"

	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// closeLinger is how long the QUIC connection [Dial] dialed outlives the
// session on it, unless the peer closes it first: long enough for the
// session's CLOSE_WEBTRANSPORT_SESSION capsule to arrive, retransmitted once
// or twice over a short path.
const closeLinger = time.Second

// Dial is d.Dial, returning the session as a [session.Conn] (over its
// connection, as [NewOver]) and closing the QUIC connection under it in a way
// that lets the peer learn why the session closed (§6.6). d.Dial closes that
// connection the moment the session closes, and a peer reads the
// connection's close (H3_NO_ERROR) ahead of the CLOSE_WEBTRANSPORT_SESSION
// capsule still buffered on the CONNECT stream: it never sees the session's
// code and reason, which [session.PeerClosedError] carries. Dial instead
// closes the connection closeLinger after the session, or once the peer has.
//
// d's fields mean what they do for d.Dial, defaults included. Unlike d.Dial,
// Dial dials port 443 for a URL that names no port.
func Dial(ctx context.Context, d *webtransport.Transport, rawURL string) (session.Conn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	// d.Dial's defaults and checks: it applies them before dialing, and
	// NewClientConn, which dials nothing, checks only the connection it is
	// handed, after its handshake.
	quicCfg := d.QUICConfig
	switch {
	case quicCfg == nil:
		quicCfg = &quic.Config{EnableDatagrams: true, EnableStreamResetPartialDelivery: true}
	case !quicCfg.EnableDatagrams:
		return nil, errors.New("webtransport: DATAGRAM support required, enable it via QUICConfig.EnableDatagrams")
	case !quicCfg.EnableStreamResetPartialDelivery:
		return nil, errors.New(
			"webtransport: stream reset partial delivery required, " +
				"enable it via QUICConfig.EnableStreamResetPartialDelivery",
		)
	}
	tlsCfg := &tls.Config{}
	if d.TLSClientConfig != nil {
		tlsCfg = d.TLSClientConfig.Clone()
	}
	if len(tlsCfg.NextProtos) == 0 {
		tlsCfg.NextProtos = []string{http3.NextProtoH3}
	}
	dialAddr := d.DialAddr
	if dialAddr == nil {
		dialAddr = quic.DialAddrEarly
	}
	// An https URL without a port names https's default, 443 (RFC 9110
	// §4.2.2); the dial address has to spell it out. d.Dial dials u.Host as is.
	host := u.Host
	if u.Port() == "" {
		host = net.JoinHostPort(u.Hostname(), "443")
	}

	q, err := dialAddr(ctx, host, tlsCfg, quicCfg)
	if err != nil {
		return nil, err
	}
	cc, err := d.NewClientConn(q)
	if err != nil {
		_ = q.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
		return nil, err
	}
	//nolint:bodyclose // the response body is the CONNECT stream the session rides on.
	_, s, err := cc.Dial(ctx, rawURL, nil)
	if err != nil {
		code, msg := quic.ApplicationErrorCode(http3.ErrCodeNoError), ""
		if rerr, ok := errors.AsType[*webtransport.RequirementsNotMetError](err); ok {
			code, msg = webtransport.WTRequirementsNotMetErrorCode, rerr.Message
		}
		_ = q.CloseWithError(code, msg)
		return nil, err
	}
	context.AfterFunc(s.Context(), func() {
		linger := time.NewTimer(closeLinger)
		defer linger.Stop()
		select {
		case <-q.Context().Done():
		case <-linger.C:
		}
		_ = q.CloseWithError(quic.ApplicationErrorCode(http3.ErrCodeNoError), "")
	})
	return NewOver(s, q), nil
}
