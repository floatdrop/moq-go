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
//   - A peer REQUEST_UPDATE is answered with the single REQUEST_OK §10.9
//     mandates; the broker applies no parameters.
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

// mapUpdateResponse converts a §10.9 response message into the
// (*message.RequestOK, error) shape Update-style callers return: REQUEST_OK
// passes through, REQUEST_ERROR becomes a *RequestRejectedError, anything
// else is a protocol-shape error.
func mapUpdateResponse(msg message.Message) (*message.RequestOK, error) {
	switch m := msg.(type) {
	case *message.RequestOK:
		return m, nil
	case *message.RequestError:
		return nil, &RequestRejectedError{Code: m.ErrorCode, Reason: m.ErrorReason}
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

	ok, err := mapUpdateResponse(msg)
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

// Close tears the request stream down: pending and future Updates fail with
// [ErrRequestStreamClosed], the read side is reset with code (unblocking a
// running Serve), and the send side is FIN'd — closing the request stream is
// how a requester ends the request (§10.7). Serialized against in-flight
// writes; idempotent. Must not be called with locks that Serve's callback
// might need held.
func (b *RequestBroker) Close(code moqt.StreamResetCode) {
	b.closeUpdates()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.streamClosed {
		return
	}
	b.streamClosed = true
	b.stream.CancelRead(uint64(code))
	_ = b.stream.Close()
}

// Serve owns every read on the request stream until the peer tears it down
// (EOF / reset), ctx is cancelled (the read side is then reset to unblock
// the parse), or onMsg returns false. On exit, pending and future Update
// calls fail with [ErrRequestStreamClosed].
//
// Responses route to Update waiters; token parameters go through the
// session's token cache (a cache fault closes the session with the §10.2.2
// code and ends Serve); peer REQUEST_UPDATEs are acknowledged with
// REQUEST_OK. Every other message — and any unsolicited response — is passed
// to onMsg (nil means "discard"); return false from onMsg to stop serving.
//
// A malformed follow-up (any non-EOF parse error) resets the read side with
// INTERNAL_ERROR so the peer learns reads stopped instead of filling flow
// control into a void. Serve returns nil on a clean FIN or an onMsg stop,
// ctx.Err() on cancellation, and the read/token error otherwise.
func (b *RequestBroker) Serve(ctx context.Context, onMsg func(message.Message) bool) error {
	defer b.closeUpdates()
	stop := context.AfterFunc(ctx, func() {
		b.stream.CancelRead(uint64(moqt.StreamResetSessionClosed))
	})
	defer stop()

	for {
		msg, err := message.Parse(b.stream)
		if err != nil {
			switch {
			case ctx.Err() != nil:
				return ctx.Err()
			case errors.Is(err, io.EOF):
				return nil
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
			if b.route(msg) {
				continue
			}
			// Unsolicited response — surface via onMsg below.
		case *message.RequestUpdate:
			// §10.1: a REQUEST_UPDATE consumes a Request ID from the
			// sender's space; a wrong-parity or duplicate ID is
			// session-fatal.
			if err := b.sess.CheckPeerRequestID(m.RequestID); err != nil {
				_ = b.sess.Close(moqt.SessionInvalidRequestID, err.Error())
				return err
			}
			// §10.9: the receiver of a REQUEST_UPDATE "MUST respond with
			// exactly one REQUEST_OK or REQUEST_ERROR". The broker keeps
			// no mutable per-request parameters, so the update is
			// acknowledged without further action; onMsg still observes it.
			if err := b.WriteMessage(&message.RequestOK{}); err != nil {
				return fmt.Errorf("moqt/session: write REQUEST_UPDATE_OK: %w", err)
			}
		}

		if onMsg != nil && !onMsg(msg) {
			return nil
		}
	}
}
