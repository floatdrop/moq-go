package session

import (
	"context"
	"errors"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// SubgroupHandler handles one inbound SUBGROUP_HEADER stream that a [Demux]
// routed to it by §11.1 Track Alias. It is invoked synchronously by
// [Demux.Run]; spawn a goroutine inside it when streams must be processed
// concurrently (see [Demux.Run]).
type SubgroupHandler func(*IncomingSubgroupStream)

// FetchHandler handles one inbound FETCH_HEADER stream that a [Demux] routed to
// it by §11.5 Request ID. Invoked synchronously by [Demux.Run].
type FetchHandler func(*IncomingFetchStream)

// Demux routes the data streams accepted from a [Session] to per-track and
// per-request handlers, replacing the hand-rolled "AcceptDataStream loop +
// type-switch + Track-Alias match" a subscriber otherwise writes.
//
// Subgroup streams are dispatched by their §11.1 Track Alias — the value a
// subscriber gets from [Subscription.TrackAlias]; FETCH streams by their §11.5
// Request ID — the ID the subscriber's FETCH was assigned. A stream with no
// registered handler is passed to the OnUnknown callback.
//
// Handlers may be registered or replaced at any time, including while
// [Demux.Run] is executing: a subscriber learns a track's alias only from its
// SUBSCRIBE_OK, which can arrive after Run has started. Registration is
// safe for concurrent use.
//
// The zero value is not ready for use — construct with [NewDemux].
type Demux struct {
	mu        sync.RWMutex
	subgroup  map[uint64]SubgroupHandler // keyed by Track Alias
	fetch     map[uint64]FetchHandler    // keyed by Request ID
	onUnknown func(DataStream)
}

// NewDemux returns an empty Demux ready for handler registration.
func NewDemux() *Demux {
	return &Demux{
		subgroup: make(map[uint64]SubgroupHandler),
		fetch:    make(map[uint64]FetchHandler),
	}
}

// HandleTrack registers h for inbound subgroup streams whose Track Alias is
// alias — typically the [Subscription.TrackAlias] of a subscription this side
// opened. A nil h unregisters alias; registering an alias that already has a
// handler replaces it.
func (d *Demux) HandleTrack(alias uint64, h SubgroupHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if h == nil {
		delete(d.subgroup, alias)
		return
	}
	d.subgroup[alias] = h
}

// HandleFetch registers h for the inbound FETCH stream answering the FETCH with
// the given Request ID. A nil h unregisters it; re-registering replaces.
func (d *Demux) HandleFetch(requestID uint64, h FetchHandler) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if h == nil {
		delete(d.fetch, requestID)
		return
	}
	d.fetch[requestID] = h
}

// OnUnknown sets the callback invoked for an accepted data stream that matches
// no registered handler. With no callback set (the default, or a nil f), an
// unmatched stream is reset with StreamResetInternalError and dropped so it
// does not leak.
func (d *Demux) OnUnknown(f func(DataStream)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onUnknown = f
}

// Run accepts data streams from sess and dispatches each to its registered
// handler until ctx is cancelled or [Session.AcceptDataStream] returns a
// non-padding error, which Run returns. Padding streams (§11.6) are skipped.
//
// Dispatch is synchronous: a handler runs to completion before Run accepts the
// next stream, mirroring a hand-written accept loop. A handler that reads a
// long-lived stream therefore blocks the loop, so spawn a goroutine inside the
// handler when streams must be read concurrently.
func (d *Demux) Run(ctx context.Context, sess *Session) error {
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			if errors.Is(err, ErrPaddingStream) {
				continue
			}
			return err
		}
		d.dispatch(ds)
	}
}

// dispatch routes one accepted data stream to its registered handler, or to the
// unknown path when none matches.
func (d *Demux) dispatch(ds DataStream) {
	switch s := ds.(type) {
	case *IncomingSubgroupStream:
		d.mu.RLock()
		h := d.subgroup[s.Header.TrackAlias]
		d.mu.RUnlock()
		if h != nil {
			h(s)
			return
		}
	case *IncomingFetchStream:
		d.mu.RLock()
		h := d.fetch[s.Header.RequestID]
		d.mu.RUnlock()
		if h != nil {
			h(s)
			return
		}
	}
	d.unknown(ds)
}

func (d *Demux) unknown(ds DataStream) {
	d.mu.RLock()
	f := d.onUnknown
	d.mu.RUnlock()
	if f != nil {
		f(ds)
		return
	}
	ds.Cancel(moqt.StreamResetInternalError)
}
