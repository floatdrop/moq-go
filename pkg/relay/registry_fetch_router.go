package relay

import (
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// fetchResponseGrace bounds how long an upstream FETCH response stream waits
// for its requesting reader to register before the router gives up and resets
// it. It only matters when the response data stream is dispatched by the
// upstream session's data loop before the downstream handler has registered:
// the Request ID is known only after [session.Session.Fetch] returns, so a
// fast upstream can race the registration. Generous relative to the in-process
// and LAN round-trips it guards against.
const fetchResponseGrace = 5 * time.Second

// fetchKey identifies an in-flight upstream FETCH by the session it was issued
// on and the Request ID the session assigned.
type fetchKey struct {
	sess  *session.Session
	reqID uint64
}

// fetchRouter rendezvouses upstream FETCH response streams with the downstream
// handler that issued the FETCH. The two sides run on different goroutines —
// the requester (a downstream FETCH handler) calls [fetchRouter.register] and
// awaits, while the upstream session's data loop calls [fetchRouter.deliver] —
// and they may arrive in either order, so each side get-or-creates the
// rendezvous and a buffered slot holds the stream until the reader takes it.
//
// One fetchRouter is shared per [Relay] and injected into every session
// handler.
type fetchRouter struct {
	mu      sync.Mutex
	pending map[fetchKey]chan *session.IncomingFetchStream
}

func newFetchRouter() *fetchRouter {
	return &fetchRouter{pending: make(map[fetchKey]chan *session.IncomingFetchStream)}
}

// chanLocked returns the rendezvous channel for key, creating it if absent.
// created reports whether this call created the entry. The caller holds r.mu.
func (r *fetchRouter) chanLocked(key fetchKey) (ch chan *session.IncomingFetchStream, created bool) {
	ch, ok := r.pending[key]
	if !ok {
		ch = make(chan *session.IncomingFetchStream, 1)
		r.pending[key] = ch
		created = true
	}
	return ch, created
}

// register reserves the rendezvous for an upstream FETCH the caller is about
// to issue (or just issued) on sess with the assigned reqID. It returns the
// channel the response stream will arrive on and a cleanup func the caller
// MUST defer: cleanup removes the entry and resets any stream that arrived but
// was never consumed (e.g. the caller timed out waiting).
func (r *fetchRouter) register(
	sess *session.Session,
	reqID uint64,
) (<-chan *session.IncomingFetchStream, func()) {
	key := fetchKey{sess: sess, reqID: reqID}
	r.mu.Lock()
	ch, _ := r.chanLocked(key)
	r.mu.Unlock()

	cleanup := func() {
		r.mu.Lock()
		if cur, ok := r.pending[key]; ok && cur == ch {
			delete(r.pending, key)
		}
		r.mu.Unlock()
		// Reset a stream that landed after the reader gave up.
		select {
		case s := <-ch:
			if s != nil {
				s.Cancel(moqt.StreamResetInternalError)
			}
		default:
		}
	}
	return ch, cleanup
}

// deliver hands an upstream FETCH response stream to its waiting reader. It
// reports whether the stream was accepted into the rendezvous. When deliver
// creates the rendezvous (the response arrived before the reader registered),
// it schedules a grace timer that resets the stream if no reader claims it, so
// a stray response can't leak. It returns false only when a stream is already
// parked for the same key (a duplicate or unexpected response); the caller
// resets the incoming stream in that case.
func (r *fetchRouter) deliver(sess *session.Session, reqID uint64, stream *session.IncomingFetchStream) bool {
	key := fetchKey{sess: sess, reqID: reqID}
	r.mu.Lock()
	ch, created := r.chanLocked(key)
	r.mu.Unlock()

	select {
	case ch <- stream:
	default:
		return false // a stream is already parked for this key
	}

	if created {
		time.AfterFunc(fetchResponseGrace, func() {
			r.mu.Lock()
			cur, ok := r.pending[key]
			if !ok || cur != ch {
				r.mu.Unlock()
				return
			}
			delete(r.pending, key)
			r.mu.Unlock()
			select {
			case s := <-ch:
				if s != nil {
					s.Cancel(moqt.StreamResetInternalError)
				}
			default:
			}
		})
	}
	return true
}
