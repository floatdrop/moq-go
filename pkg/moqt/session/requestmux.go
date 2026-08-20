package session

import (
	"context"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// RequestHandler handles one inbound request that a [RequestMux] routed to it by
// the [message.Type] of its first message. It is invoked synchronously by
// [RequestMux.Run]; spawn a goroutine inside it when a request must be serviced
// concurrently with accepting the next one (see [RequestMux.Run]).
type RequestHandler func(*Request)

// RequestMux routes the requests accepted from a [Session] to per-type handlers,
// replacing the hand-rolled "AcceptRequest loop + type-switch + dispatch" a
// server otherwise writes. It is the request-stream counterpart of [Demux],
// which does the same for inbound data streams.
//
// Requests are dispatched by the [message.Type] of their first message — e.g.
// [message.TypeSubscribe] for an inbound SUBSCRIBE. A request whose type has no
// registered handler is passed to the OnUnknown callback.
//
// Handlers may be registered or replaced at any time, including while
// [RequestMux.Run] is executing. Registration is safe for concurrent use.
//
// The zero value is not ready for use — construct with [NewRequestMux].
type RequestMux struct {
	mu        sync.RWMutex
	handlers  map[message.Type]RequestHandler
	onUnknown func(*Request)
}

// NewRequestMux returns an empty RequestMux ready for handler registration.
func NewRequestMux() *RequestMux {
	return &RequestMux{handlers: make(map[message.Type]RequestHandler)}
}

// Handle registers h for inbound requests whose first message is of type t
// (e.g. [message.TypeSubscribe]). A nil h unregisters t; registering a type
// that already has a handler replaces it.
func (m *RequestMux) Handle(t message.Type, h RequestHandler) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if h == nil {
		delete(m.handlers, t)
		return
	}
	m.handlers[t] = h
}

// HandleType registers h for inbound requests whose first message is the
// concrete type T (e.g. *message.Subscribe), handing h the already-asserted
// typed message alongside the [*Request]. It is the generic form of
// [RequestMux.Handle]: the [message.Type] key is derived from T, and the type
// assertion a Handle callback would otherwise repeat (req.First.(*message.X)) is
// done once, here.
//
// A nil h unregisters T's type; registering a type that already has a handler
// replaces it.
func (m *RequestMux) HandleType[T message.WithRequestID](h func(*Request, T)) {
	var zero T // nil pointer; message Type() methods are constant returns
	if h == nil {
		m.Handle(zero.Type(), nil)
		return
	}
	m.Handle(zero.Type(), func(req *Request) {
		msg, _ := req.First.(T)
		h(req, msg)
	})
}

// OnUnknown sets the callback invoked for an accepted request whose type has no
// registered handler. With no callback set (the default, or a nil f), an
// unmatched request is rejected with REQUEST_ERROR NOT_SUPPORTED and its stream
// FIN'd so it does not leak.
func (m *RequestMux) OnUnknown(f func(*Request)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onUnknown = f
}

// Run accepts requests from sess and dispatches each to its registered handler
// until ctx is cancelled or [Session.AcceptRequest] returns an error, which Run
// returns.
//
// Some AcceptRequest errors are session-fatal protocol violations — a §10.1
// Request-ID parity/monotonicity violation (*ErrRequestIDParityViolation /
// *ErrDuplicateRequestID) or a token-cache fault (*TokenCacheError) — that the
// caller MUST escalate by closing the session with the mapped code (see
// [Session.AcceptRequest]). Run surfaces the error unchanged so the caller can
// inspect it with errors.As and Close accordingly.
//
// Dispatch is synchronous: a handler runs to completion before Run accepts the
// next request, mirroring a hand-written accept loop and [Demux.Run]. A handler
// that keeps a request stream open for the lifetime of a subscription therefore
// blocks the loop, so spawn a goroutine inside the handler when requests must be
// serviced concurrently.
func (m *RequestMux) Run(ctx context.Context, sess *Session) error {
	for {
		req, err := sess.AcceptRequest(ctx)
		if err != nil {
			return err
		}
		m.dispatch(req)
	}
}

// dispatch routes one accepted request to its registered handler, or to the
// unknown path when none matches.
func (m *RequestMux) dispatch(req *Request) {
	m.mu.RLock()
	h := m.handlers[req.First.Type()]
	f := m.onUnknown
	m.mu.RUnlock()
	if h != nil {
		h(req)
		return
	}
	if f != nil {
		f(req)
		return
	}
	_ = req.RejectError(moqt.RequestNotSupported, "moqt/session: no handler for request type")
}
