package session

import (
	"errors"
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// sendControl queues a control message for the send loop. Blocks if the queue
// is full or the session is done.
func (s *Session) sendControl(msg message.Message) error {
	select {
	case s.controlOut <- msg:
		return nil
	case <-s.done:
		return errors.New("moqt/session: closed")
	}
}

// controlSendLoop serializes writes onto the send-control stream. It exits on
// session shutdown or on the first write error.
func (s *Session) controlSendLoop() {
	for {
		select {
		case msg := <-s.controlOut:
			if err := message.Marshal(s.sendCtrl, msg); err != nil {
				if s.sessionDoneAlready() {
					return
				}
				_ = s.Close(moqt.SessionInternalError, "control send failure")
				return
			}
		case <-s.done:
			return
		}
	}
}

// controlRecvLoop reads framed control messages off the recv-control stream
// and dispatches them. The loop owns shutdown on read failure unless the
// session is already terminating.
func (s *Session) controlRecvLoop() {
	for {
		msg, err := message.Parse(s.recvCtrl)
		if err != nil {
			if s.sessionDoneAlready() {
				return
			}
			if errors.Is(err, io.EOF) {
				// Peer closed the control stream cleanly. §3.3 forbids
				// this during the session lifetime; treat it as a
				// protocol violation.
				_ = s.Close(moqt.SessionProtocolViolation, "peer closed control stream")
				return
			}
			_ = s.Close(moqt.SessionProtocolViolation, err.Error())
			return
		}
		if err := s.dispatchControl(msg); err != nil {
			if s.sessionDoneAlready() {
				return
			}
			// Most control-plane violations close with PROTOCOL_VIOLATION,
			// but some rules mandate a specific code (e.g. §10.4 GOAWAY
			// Request-ID parity → INVALID_REQUEST_ID) — handlers carry it
			// via *sessionCloseError.
			code := moqt.SessionProtocolViolation
			if ce, ok := errors.AsType[*sessionCloseError](err); ok {
				code = ce.code
			}
			_ = s.Close(code, err.Error())
			return
		}
	}
}

func (s *Session) sessionDoneAlready() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// dispatchControl handles a single control-stream message after SETUP. Per
// table 5 in §10, only GOAWAY is valid on the control stream after SETUP for
// the messages in scope; anything else is a protocol violation.
func (s *Session) dispatchControl(msg message.Message) error {
	switch m := msg.(type) {
	case *message.Goaway:
		return s.handleGoaway(m)
	case *message.Setup:
		return errors.New("duplicate SETUP on control stream")
	default:
		return fmt.Errorf("unexpected %s on control stream", msg.Type())
	}
}

// sessionCloseError is a control-dispatch error that mandates a specific
// §3.5 SESSION_ERROR close code instead of the default PROTOCOL_VIOLATION.
type sessionCloseError struct {
	code moqt.SessionErrorCode
	msg  string
}

func (e *sessionCloseError) Error() string { return e.msg }
