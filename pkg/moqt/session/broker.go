package session

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// RequestBroker owns an established request stream's read side and
// serializes its writes, so REQUEST_UPDATE (§10.9) and long-lived follow-up
// traffic can safely coexist. [Session.UpdateRequest] reads its response
// directly off the stream and therefore cannot run concurrently with any
// other reader; once a request outlives its initial response — a relay's
// upstream subscription, a publisher answering subscriber updates — exactly
// one reader must own the stream, and that reader is [RequestBroker.Serve]:
//
//   - REQUEST_OK / REQUEST_ERROR answer in-flight [RequestBroker.Update]
//     calls, including §10.9's coalescing rule (a peer may answer N
//     pipelined updates with a single REQUEST_ERROR, which fails them all).
//   - AUTHORIZATION_TOKEN parameters on follow-ups are resolved through the
//     session token cache (§10.2.2); a cache fault closes the session with
//     the mandated code.
//   - A peer REQUEST_UPDATE is answered (§10.9) by the handler installed with
//     [RequestBroker.HandleUpdates], or declined with NOT_SUPPORTED when there
//     is none, since acknowledging an unapplied update would misstate the
//     request's state.
//   - Everything else (PUBLISH_DONE, unsolicited responses, …) is handed to
//     Serve's callback.
//
// Obtain one from a typed request handle's Broker method (e.g.
// [Publication.Broker]) or [Session.NewRequestBroker]; from then on every
// write to the stream must go through the broker ([RequestBroker.Update],
// [RequestBroker.WriteMessage], or broker-aware handle methods such as
// [Publication.Done]) — session streams do not serialize concurrent writers.
type RequestBroker struct {
	stream Stream
	sess   *Session

	// mu serializes stream writes and guards the waiter queue. It is
	// deliberately held across the REQUEST_UPDATE write: §10.9 responses
	// arrive in request order, so the waiter queue order must match the
	// write order.
	mu      sync.Mutex
	waiters []chan updateResult
	// updatesClosed is latched when the stream's reader exits (or Close is
	// called); subsequent Update calls fail immediately instead of queueing
	// a waiter nothing will ever answer. Plain WriteMessage stays allowed —
	// e.g. a PUBLISH_DONE after the peer tore its side down.
	updatesClosed bool
	streamClosed  bool

	// onUpdate decides each peer REQUEST_UPDATE; nil declines it.
	// onUpdateFailed runs after a declined update (§10.9.1). Both are set
	// before Serve runs.
	onUpdate       UpdateHandler
	onUpdateFailed func()

	// updateScope is the §10.2.1 scope of peer REQUEST_UPDATEs; 0 skips the
	// check.
	updateScope message.ParamScope

	// See [RequestBroker.PeerMessages].
	noPeerUpdate bool
	noPeerNotify bool

	// handle is the typed handle this broker came from, if any: Serve and
	// Close report the subscription's end to it (see requestHandle.terminated).
	handle *requestHandle
}

// PeerMessages declares whether the peer may send REQUEST_UPDATE (§10.9) and
// PUBLISH_STATE_NOTIFY (§10.10) on this stream; a disallowed one closes the
// session with PROTOCOL_VIOLATION. Typed handles set this; a broker from
// [Session.NewRequestBroker] allows both. Call it before [RequestBroker.Serve].
func (b *RequestBroker) PeerMessages(requestUpdate, publishStateNotify bool) {
	b.noPeerUpdate, b.noPeerNotify = !requestUpdate, !publishStateNotify
}

// UpdateScope sets the §10.2.1 parameter scope of the peer's REQUEST_UPDATEs;
// one carrying a parameter outside it closes the session with
// PROTOCOL_VIOLATION. Typed handles set this; a broker from
// [Session.NewRequestBroker] checks nothing. Call it before
// [RequestBroker.Serve].
func (b *RequestBroker) UpdateScope(s message.ParamScope) { b.updateScope = s }

// UpdateHandler decides a peer's REQUEST_UPDATE (§10.9). It returns the
// REQUEST_OK to send, or an error: a *[RequestRejectedError] is sent as
// REQUEST_ERROR with its code, reason and Retry Interval, any other error as
// INTERNAL_ERROR.
type UpdateHandler func(upd *message.RequestUpdate) (*message.RequestOK, error)

// HandleUpdates installs the handler that decides peer REQUEST_UPDATEs,
// replacing any earlier one. Call it before [RequestBroker.Serve].
func (b *RequestBroker) HandleUpdates(h UpdateHandler) { b.onUpdate = h }

// answerUpdate writes the §10.9 response to upd and reports whether the
// update was accepted.
func (b *RequestBroker) answerUpdate(upd *message.RequestUpdate) (bool, error) {
	var (
		ok  *message.RequestOK
		err error
	)
	if b.onUpdate == nil {
		err = &RequestRejectedError{Code: moqt.RequestNotSupported, Reason: "REQUEST_UPDATE not supported"}
	} else {
		ok, err = b.onUpdate(upd)
	}
	// A handler that closed the session leaves no request to answer.
	select {
	case <-b.sess.Done():
		if err == nil {
			err = ErrRequestStreamClosed
		}
		return false, err
	default:
	}
	if err == nil {
		if ok == nil {
			ok = &message.RequestOK{}
		}
		if len(ok.TrackProperties) > 0 {
			// §10.5: REQUEST_UPDATE_OK's Track Properties are empty.
			err = fmt.Errorf("%w: REQUEST_UPDATE_OK", ErrTrackPropertiesNotAllowed)
		}
	}
	if err == nil {
		if werr := b.WriteMessage(ok); werr != nil {
			return false, fmt.Errorf("moqt/session: write REQUEST_UPDATE_OK: %w", werr)
		}
		return true, nil
	}
	rej, isRej := errors.AsType[*RequestRejectedError](err)
	if !isRej {
		rej = &RequestRejectedError{Code: moqt.RequestInternalError, Reason: err.Error()}
	}
	if werr := b.WriteMessage(&message.RequestError{
		ErrorCode:     rej.Code,
		RetryInterval: rej.RetryInterval,
		ErrorReason:   rej.Reason,
	}); werr != nil {
		return false, fmt.Errorf("moqt/session: write REQUEST_UPDATE error: %w", werr)
	}
	return false, nil
}

// updateResult carries one §10.9 response to a waiting Update call.
type updateResult struct {
	ok  *message.RequestOK
	err error
}

// ErrRequestStreamClosed is returned by [RequestBroker.Update] when the
// request stream's reader has exited (peer FIN/reset or session shutdown) —
// no further REQUEST_UPDATE can be answered.
var ErrRequestStreamClosed = errors.New("moqt/session: request stream closed")

// NewRequestBroker builds a [RequestBroker] for an established request
// stream. Typed request handles expose a Broker method that fills this in;
// use this constructor for accept-side streams (a [Request] this endpoint
// accepted).
func (s *Session) NewRequestBroker(stream Stream) *RequestBroker {
	return &RequestBroker{stream: stream, sess: s}
}

// mapUpdateResponse converts a §10.9 response: REQUEST_OK passes through,
// REQUEST_ERROR becomes a *RequestRejectedError, anything else is an error. A
// REQUEST_UPDATE_OK carrying Track Properties closes the session (§10.5).
func (s *Session) mapUpdateResponse(msg message.Message) (*message.RequestOK, error) {
	switch m := msg.(type) {
	case *message.RequestOK:
		if err := s.checkRequestOKTrackProperties(nil, m); err != nil {
			return nil, err
		}
		if err := s.CheckPeerParams(message.ScopeRequestUpdateOK, m); err != nil {
			return nil, err
		}
		return m, nil
	case *message.RequestError:
		return nil, &RequestRejectedError{Code: m.ErrorCode, Reason: m.ErrorReason, RetryInterval: m.RetryInterval}
	default:
		return nil, fmt.Errorf("moqt/session: unexpected %s in REQUEST_UPDATE response", msg.Type())
	}
}

// Update sends a REQUEST_UPDATE (§10.9) on the request stream and awaits the
// single REQUEST_OK / REQUEST_ERROR the spec mandates, delivered by the
// [RequestBroker.Serve] reader. params carries only the fields to change;
// any parameter omitted keeps its prior value on the peer.
//
// A REQUEST_ERROR is surfaced as a *RequestRejectedError. On ctx expiry the
// waiter is removed from the queue, so a peer that never answers cannot
// permanently shift response routing for later updates. (If the response is
// merely late, routing for updates written after the removal shifts by one —
// the lesser evil versus permanent poisoning; conforming peers answer.)
//
// Known limitation: the REQUEST_UPDATE write itself runs under the write
// lock and is not ctx-bounded — a peer that stalls stream flow control
// blocks Update until the session dies and errors the write.
func (b *RequestBroker) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	ch := make(chan updateResult, 1)

	b.mu.Lock()
	if b.updatesClosed {
		b.mu.Unlock()
		return nil, ErrRequestStreamClosed
	}
	// Write while holding mu: it serializes writers on the stream AND keeps
	// the waiter queue order equal to the write order, which is what lets
	// the reader pair each §10.9 response with its update. The ID is
	// allocated under the same lock so IDs appear on this stream in
	// increasing order — §10.1: REQUEST_UPDATE consumes a fresh Request ID
	// from the sender's space (the stream, not the ID, names the request
	// being updated; a reused ID is a session-fatal duplicate).
	err := message.Marshal(b.stream, &message.RequestUpdate{
		RequestID:  b.sess.AllocRequestID(),
		Parameters: params,
	})
	if err == nil {
		b.waiters = append(b.waiters, ch)
	}
	b.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("moqt/session: write REQUEST_UPDATE: %w", err)
	}

	select {
	case res := <-ch:
		return res.ok, res.err
	case <-ctx.Done():
		b.mu.Lock()
		if i := slices.Index(b.waiters, ch); i >= 0 {
			b.waiters = slices.Delete(b.waiters, i, i+1)
		}
		b.mu.Unlock()
		return nil, ctx.Err()
	}
}

// WriteMessage marshals a control message onto the request stream under the
// same lock that serializes Update's REQUEST_UPDATE writes.
func (b *RequestBroker) WriteMessage(msg message.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	return message.Marshal(b.stream, msg)
}

// WriteMessageAfterSetup runs setup and then marshals msg, both under the
// write lock. It exists for responder-side visibility ordering: setup
// typically publishes state that lets other goroutines write to this stream
// through the broker (e.g. a relay registering an upstream subscription
// that a concurrent propagation path may immediately Update). Running both
// under the lock guarantees that msg is the stream's next message — a write
// triggered by the new visibility serializes behind it — while the peer
// cannot observe msg before setup completed. A setup error aborts the
// write. setup must not use the broker or write to the stream itself, and
// must not acquire locks that stream writers hold while using the broker.
func (b *RequestBroker) WriteMessageAfterSetup(setup func() error, msg message.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := setup(); err != nil {
		return err
	}
	return message.Marshal(b.stream, msg)
}

// writeThenClose marshals msg and FINs the send side under the write lock —
// the broker-aware backend of terminal handle methods like
// [Publication.Done].
func (b *RequestBroker) writeThenClose(msg message.Message) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := message.Marshal(b.stream, msg); err != nil {
		return err
	}
	return b.stream.Close()
}

// route delivers a REQUEST_OK / REQUEST_ERROR read off the stream to
// in-flight Update calls: a REQUEST_OK answers the oldest waiter; a
// REQUEST_ERROR answers ALL of them, because §10.9 lets the peer coalesce
// pipelined updates and "only a single REQUEST_ERROR will be sent" for the
// batch. It reports whether any waiter consumed the message; false means
// none was pending (an unsolicited response Serve hands to its callback).
func (b *RequestBroker) route(msg message.Message) bool {
	b.mu.Lock()
	if len(b.waiters) == 0 {
		b.mu.Unlock()
		return false
	}
	var recipients []chan updateResult
	if _, isErr := msg.(*message.RequestError); isErr {
		recipients, b.waiters = b.waiters, nil
	} else {
		recipients, b.waiters = b.waiters[:1], b.waiters[1:]
	}
	b.mu.Unlock()

	ok, err := b.sess.mapUpdateResponse(msg)
	res := updateResult{ok: ok, err: err}
	for _, ch := range recipients {
		ch <- res
	}
	return true
}

// closeUpdates latches the broker shut for updates and fails every pending
// Update with [ErrRequestStreamClosed]. Idempotent; Serve calls it on exit
// and Close calls it as part of full teardown.
func (b *RequestBroker) closeUpdates() {
	b.mu.Lock()
	waiters := b.waiters
	b.waiters = nil
	b.updatesClosed = true
	b.mu.Unlock()
	for _, ch := range waiters {
		ch <- updateResult{err: ErrRequestStreamClosed}
	}
}

// Close cancels the request (§3.3.3): pending and future Updates fail with
// [ErrRequestStreamClosed] and both directions are reset with code, which
// unblocks a running Serve. On a broker from a [Subscription] or
// [IncomingPublication] the subscription is Terminated (§5.1) and its Track
// Alias released (§11.1). Serialized against in-flight writes; idempotent.
// Must not be called with locks that Serve's callback might need held.
func (b *RequestBroker) Close(code moqt.StreamResetCode) {
	if b.handle != nil {
		b.handle.terminated()
	}
	b.closeUpdates()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streamClosed {
		return
	}
	b.streamClosed = true
	b.stream.CancelRead(uint64(code))
	b.stream.CancelWrite(uint64(code))
}

// Serve owns every read on the request stream until the peer tears it down
// (EOF / reset), ctx is cancelled (the read side is then reset to unblock
// the parse), or onMsg returns false. On exit, pending and future Update
// calls fail with [ErrRequestStreamClosed].
//
// Responses route to Update waiters; a token cache fault closes the session
// (§10.2.2); peer REQUEST_UPDATEs are answered as described on
// [RequestBroker]. Every other message, including each REQUEST_UPDATE and any
// unsolicited response, is passed to onMsg (nil means "discard"); return false
// from onMsg to stop serving.
//
// A read error resets the read side with INTERNAL_ERROR; a malformed follow-up
// also closes the session with PROTOCOL_VIOLATION (§10). Serve returns nil on
// a clean FIN or an onMsg stop, ctx.Err() on cancellation, and the read/token
// error otherwise.
//
// On a broker from a [Subscription] or [IncomingPublication], every exit but
// an onMsg stop Terminates the subscription (§5.1) and releases its Track
// Alias (§11.1).
func (b *RequestBroker) Serve(ctx context.Context, onMsg func(message.Message) bool) error {
	defer b.closeUpdates()
	stopped := false // by onMsg: the stream may still carry the subscription
	defer func() {
		if !stopped && b.handle != nil {
			b.handle.terminated()
		}
	}()
	stop := context.AfterFunc(ctx, func() {
		b.stream.CancelRead(uint64(moqt.StreamResetSessionClosed))
	})
	defer stop()

	// §10.3.1.7: per-stream MAX_REQUEST_UPDATES enforcement. One limiter per
	// stream, since the limit is scoped to a single request stream.
	updates := b.sess.NewRequestUpdateLimiter()

	for {
		msg, err := message.Parse(b.stream)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, io.EOF):
				if b.handle != nil {
					b.handle.peerFinished()
				}
				return nil
			case errors.Is(err, message.ErrMalformedMessage):
				// §10, and §10.2 for an unknown parameter.
				b.stream.CancelRead(uint64(moqt.StreamResetInternalError))
				return b.sess.closeProtocolViolation(err)
			default:
				// Covers peer resets too (a STOP_SENDING on an
				// already-reset stream is a transport no-op).
				b.stream.CancelRead(uint64(moqt.StreamResetInternalError))
				return err
			}
		}

		// §10.2.2: follow-ups may REGISTER/DELETE token aliases; skipping
		// this would silently desynchronize the peer's view of the token
		// cache. A cache fault is session-fatal with the mandated code.
		if _, err := b.sess.ProcessFollowupTokens(msg); err != nil {
			if tce, ok := errors.AsType[*TokenCacheError](err); ok {
				_ = b.sess.Close(tce.Code, tce.Error())
			}
			return err
		}

		switch m := msg.(type) {
		case *message.RequestOK, *message.RequestError:
			// The request's own response was read before the broker
			// attached, so every REQUEST_OK here is a REQUEST_UPDATE_OK
			// (§10.5), even one whose Update gave up.
			if err := b.sess.checkRequestOKTrackProperties(nil, m); err != nil {
				return err
			}
			if err := b.sess.CheckPeerParams(message.ScopeRequestUpdateOK, m); err != nil {
				return err
			}
			if b.route(msg) {
				continue
			}
			// Unsolicited response — surface via onMsg below.
		case *message.PublishStateNotify:
			if b.noPeerNotify {
				return b.sess.closeProtocolViolation(errors.New(
					"moqt/session: PUBLISH_STATE_NOTIFY from a peer that may not send one"))
			}
			if err := b.sess.CheckPeerParams(message.ScopePublishStateNotify, m); err != nil {
				return err
			}
		case *message.RequestUpdate:
			if b.noPeerUpdate {
				return b.sess.closeProtocolViolation(errors.New(
					"moqt/session: REQUEST_UPDATE from a peer that may not send one"))
			}
			if b.updateScope != 0 {
				if err := b.sess.CheckPeerParams(b.updateScope, m); err != nil {
					return err
				}
			}
			// §10.1: a REQUEST_UPDATE consumes a Request ID from the
			// sender's space; a wrong-parity or duplicate ID is
			// session-fatal.
			if err := b.sess.CheckPeerRequestID(m.RequestID); err != nil {
				_ = b.sess.Close(moqt.SessionInvalidRequestID, err.Error())
				return err
			}
			// §10.3.1.7: reject a REQUEST_UPDATE that exceeds the per-stream
			// MAX_REQUEST_UPDATES limit before acting on it.
			if err := updates.Received(); err != nil {
				_ = b.sess.Close(moqt.SessionTooManyRequestUpdates, err.Error())
				return err
			}
			// §10.9: "MUST respond with exactly one REQUEST_OK or
			// REQUEST_ERROR". onMsg still observes the update.
			accepted, err := b.answerUpdate(m)
			if err != nil {
				return err
			}
			updates.Responded()
			if !accepted && b.onUpdateFailed != nil {
				b.onUpdateFailed()
			}
		}

		if onMsg != nil && !onMsg(msg) {
			stopped = true
			return nil
		}
	}
}
