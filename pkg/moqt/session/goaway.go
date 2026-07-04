package session

import (
	"errors"
	"fmt"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// GoawayReceived returns a channel that is closed when a GOAWAY arrives from
// the peer. After the channel closes, PeerGoaway returns the parsed message.
func (s *Session) GoawayReceived() <-chan struct{} { return s.goawayCh }

// PeerGoaway returns the GOAWAY most recently received from the peer, or nil
// if none has arrived.
func (s *Session) PeerGoaway() *message.Goaway {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.goawayReceived
}

// OnGoaway registers a callback invoked exactly once when the first GOAWAY
// arrives from the peer, passing the parsed message (whose NewSessionURI and
// Timeout drive client-side session migration per §3.6/§10.4). The handler
// runs in its own goroutine so it must not assume any ordering with other
// session activity, and it may safely block (e.g. to dial a new session and
// re-issue subscriptions) without stalling the control-receive loop.
//
// OnGoaway is level-triggered: if a GOAWAY has already been received when
// OnGoaway is called, the handler fires immediately. Only the most recently
// registered handler is retained, and the at-most-once guarantee is per
// session — a handler registered after the GOAWAY has already fired the
// previously registered one will itself fire (once) on registration.
//
// Passing a nil handler clears any previously registered callback (provided
// it has not yet fired).
func (s *Session) OnGoaway(handler func(*message.Goaway)) {
	s.mu.Lock()
	// If a GOAWAY already arrived and no handler has fired yet, run this one
	// now and mark it fired so handleGoaway won't double-invoke.
	if s.goawayReceived != nil && !s.goawayFired {
		g := s.goawayReceived
		s.goawayFired = true
		s.mu.Unlock()
		if handler != nil {
			go handler(g)
		}
		return
	}
	s.goawayHandler = handler
	s.mu.Unlock()
}

// SendGoaway sends a GOAWAY on the control stream and transitions the session
// to the draining state. newURI may be empty; timeout is the grace period
// before the local side may forcibly close the session with GOAWAY_TIMEOUT.
// Returns an error if GOAWAY has already been sent, or if the local role is
// client and newURI is non-empty (§10.4: "A client MUST NOT include a New
// Session URI").
func (s *Session) SendGoaway(timeout time.Duration, newURI string) error {
	if s.role == roleClient && newURI != "" {
		return errors.New("moqt/session: client MUST NOT send GOAWAY with New Session URI")
	}
	s.mu.Lock()
	if s.goawaySent {
		s.mu.Unlock()
		return errors.New("moqt/session: GOAWAY already sent")
	}
	s.goawaySent = true

	// §10.4: Request ID is "the smallest Request ID that was not or might
	// not have been processed." With at least one inbound Request ID seen,
	// that is normally peerRequestIDMax + 2 (peer increments by 2 per
	// §10.1) — unless delivery reordering left open gaps below the mark
	// ([Session.CheckPeerRequestID]): a gap is a peer request we have NOT
	// processed, so the smallest open gap is the honest watermark and the
	// peer must re-issue from there. If no inbound requests have arrived,
	// use the per-role minimum: 0 when we are the server (peer is client,
	// even IDs) or 1 when we are the client (peer is server, odd IDs).
	var watermark uint64
	if s.peerRequestIDSeen {
		watermark = s.peerRequestIDMax + 2
		for id := range s.peerRequestIDGaps {
			watermark = min(watermark, id)
		}
	} else if s.role == roleClient {
		watermark = 1
	}
	s.mu.Unlock()

	msg := &message.Goaway{
		NewSessionURI: []byte(newURI),
		//nolint:gosec // G115: timeout is non-negative; whole ms fits a varint.
		Timeout:      uint64(timeout / time.Millisecond),
		HasRequestID: true,
		RequestID:    watermark,
	}
	return s.sendControl(msg)
}

// handleGoaway records a received GOAWAY and notifies any waiter on
// GoawayReceived. §10.4: a second GOAWAY on the same control stream MUST
// terminate the session with PROTOCOL_VIOLATION.
//
// §10.4 also defines an OPTIONAL per-request GOAWAY: a server MAY include a
// Request ID (decoded into m.HasRequestID / m.RequestID) to ask the peer to
// re-issue just that request against a new session. The session deliberately
// does not act on it — whether and how to re-issue a request is migration
// policy that belongs to the application, which receives the full message
// (Request ID included) via PeerGoaway / OnGoaway and can drive the re-issue.
func (s *Session) handleGoaway(m *message.Goaway) error {
	// §10.4: the optional Request ID names "the smallest peer Request ID
	// that was not or might not have been processed" — a request WE sent,
	// so it must carry our parity (client even, server odd). "If the parity
	// of the Request ID does not match the receiver's parity, the endpoint
	// MUST close the session with INVALID_REQUEST_ID."
	if m.HasRequestID {
		wantOdd := s.role == roleServer
		if (m.RequestID%2 == 1) != wantOdd {
			return &sessionCloseError{
				code: moqt.SessionInvalidRequestID,
				msg: fmt.Sprintf("GOAWAY Request ID %d does not match receiver parity (%s)",
					m.RequestID, s.role),
			}
		}
	}

	s.mu.Lock()
	if s.goawayReceived != nil {
		s.mu.Unlock()
		return errors.New("duplicate GOAWAY on control stream")
	}
	// §10.4: a client cannot direct a server to migrate, so a non-empty URI
	// from a client is a PROTOCOL_VIOLATION. From our perspective, that means
	// if we are the server we must reject a GOAWAY with a URI.
	if s.role == roleServer && len(m.NewSessionURI) > 0 {
		s.mu.Unlock()
		return errors.New("GOAWAY from client carries non-empty URI")
	}
	s.goawayReceived = m
	// Snapshot the registered handler under the lock and mark it fired so a
	// later OnGoaway call won't re-invoke it. Run it in its own goroutine
	// (outside the lock) so a blocking migration handler can't stall the
	// control-receive loop.
	var handler func(*message.Goaway)
	if s.goawayHandler != nil && !s.goawayFired {
		handler = s.goawayHandler
		s.goawayFired = true
	}
	s.mu.Unlock()
	close(s.goawayCh)
	if handler != nil {
		go handler(m)
	}
	return nil
}
