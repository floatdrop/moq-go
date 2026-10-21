package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// PeerClosedError is a Conn's word that the peer closed the connection, with
// the code and reason it closed with (§6.6): what its streams, accepts and
// opens fail with from then on. A transport adapter makes it of its own
// close error (wtconn of a WebTransport session's, quicconn of a QUIC
// connection's), so that the session can tell the peer's leaving, with its
// code, from a stream that broke.
type PeerClosedError struct {
	Code   uint64
	Reason string
	Err    error // the transport's own
}

func (e *PeerClosedError) Error() string {
	return fmt.Sprintf("moqt/session: peer closed with code %#x: %s", e.Code, e.Reason)
}

func (e *PeerClosedError) Unwrap() error { return e.Err }

// closeOnTransport ends the session for err, a transport's failure: as the
// peer closed it, with the peer's code and reason, if err says it did;
// else with code and reason, this side's.
func (s *Session) closeOnTransport(err error, code moqt.SessionErrorCode, reason string) {
	if pc, ok := errors.AsType[*PeerClosedError](err); ok {
		s.terminate(&ClosedError{Code: moqt.SessionErrorCode(pc.Code), Reason: pc.Reason, Remote: true}, pc.Code, pc.Reason)
		return
	}
	_ = s.Close(code, reason)
}

// CloseCauser is a Conn that says why it closed once its Context is done: a
// *PeerClosedError when the peer closed it, with its code and reason.
type CloseCauser interface {
	CloseCause() error
}

// peerCloseGrace is how long a session whose control stream failed waits
// for the connection's close: a peer closing a session resets its control
// streams first (Close), and the code it closes with comes after.
const peerCloseGrace = 500 * time.Millisecond

// closeOnControlLoss ends the session for err, its control stream's
// failure: if the connection closes within peerCloseGrace and its Conn says
// the peer closed it, with the peer's code and reason; else with code and
// reason, this side's.
func (s *Session) closeOnControlLoss(err error, code moqt.SessionErrorCode, reason string) {
	if _, ok := errors.AsType[*PeerClosedError](err); !ok {
		if cc, ok := s.conn.(CloseCauser); ok {
			select {
			case <-s.conn.Context().Done():
				if cause := cc.CloseCause(); cause != nil {
					err = cause
				}
			case <-time.After(peerCloseGrace):
			case <-s.done:
				return
			}
		}
	}
	s.closeOnTransport(err, code, reason)
}
