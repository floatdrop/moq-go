package registry

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
)

// InFlight is the Object an inbound subgroup stream is part way through: its
// header has been read, its payload not yet. The relay knows that Object
// exists and is arriving, so a FETCH or fill that covers it waits for it
// rather than reporting its status unknown (§3.1.4, §3.2: a FETCH "includes
// that Object in full; the publisher delivers the remainder as it becomes
// available").
//
// One InFlight serves a stream for its lifetime, reused Object after Object,
// so tracking adds no per-Object allocation. Its fields are guarded by the
// owning TrackEntry's mu.
type InFlight struct {
	loc     message.Location
	active  bool
	waiters []*FlightWait
}

// NewInFlight registers a slot for one inbound stream of e; each Object's
// header marks it active ([TrackEntry.UpdateLargestAndDetectNew]). Release it
// with [TrackEntry.ReleaseInFlight] when the stream ends.
func (e *TrackEntry) NewInFlight() *InFlight {
	f := &InFlight{}
	e.mu.Lock()
	if e.inflight == nil {
		e.inflight = make(map[*InFlight]struct{})
	}
	e.inflight[f] = struct{}{}
	e.mu.Unlock()
	return f
}

// EndInFlight records that f's Object is no longer arriving. obj is the Object
// as it was cached, handed to every FETCH waiting on it — which therefore gets
// it even where the cache will not serve it (§10.3) — or nil when it was not
// cached: abandoned mid-payload, a duplicate of a copy already cached, or
// known not to exist. A waiter then looks at the cache and the ledger again.
func (e *TrackEntry) EndInFlight(f *InFlight, obj *cache.CachedObject) {
	e.mu.Lock()
	e.endInFlightLocked(f, obj)
	e.mu.Unlock()
}

func (e *TrackEntry) endInFlightLocked(f *InFlight, obj *cache.CachedObject) {
	if !f.active {
		return
	}
	for _, w := range f.waiters {
		w.results = append(w.results, flightResult{loc: f.loc, obj: obj})
		select {
		case w.notify <- struct{}{}:
		default: // a wake-up is already pending
		}
	}
	f.active, f.waiters = false, nil
}

// ReleaseInFlight unregisters f as its stream ends; an Object it was still
// reading is abandoned.
func (e *TrackEntry) ReleaseInFlight(f *InFlight) {
	e.mu.Lock()
	e.endInFlightLocked(f, nil)
	delete(e.inflight, f)
	e.mu.Unlock()
}

// FlightWait is a FETCH's wait on the Objects arriving within its range.
type FlightWait struct {
	entry *TrackEntry
	// slots and results are guarded by entry.mu; notify wakes Wait when
	// results grows.
	slots   []*InFlight
	results []flightResult
	notify  chan struct{}
	// pending counts, per Location, the streams still reading that Object.
	pending map[message.Location]int
	// done holds the Objects handed over on completion.
	done []*cache.CachedObject
}

type flightResult struct {
	loc message.Location
	obj *cache.CachedObject
}

// WaitInFlight registers a wait on every Object in [start, end] an inbound
// stream of e is part way through, or returns nil when there is none.
// Register before reading the cache: an Object that completes first is then
// already cached, and one that completes later is handed to the wait. End it
// with [FlightWait.Wait] or [FlightWait.Stop].
func (e *TrackEntry) WaitInFlight(start, end message.Location) *FlightWait {
	e.mu.Lock()
	defer e.mu.Unlock()
	var w *FlightWait
	for f := range e.inflight {
		if !f.active || f.loc.Less(start) || end.Less(f.loc) {
			continue
		}
		if w == nil {
			w = &FlightWait{entry: e, pending: make(map[message.Location]int), notify: make(chan struct{}, 1)}
		}
		w.registerLocked(f)
	}
	return w
}

// registerLocked adds w to f's waiters. Callers hold w.entry.mu.
func (w *FlightWait) registerLocked(f *InFlight) {
	w.pending[f.loc]++
	w.slots = append(w.slots, f)
	f.waiters = append(f.waiters, w)
}

// Forget stops waiting for the Object at loc, which the caller already holds:
// a copy of it was cached while another stream is still reading one.
func (w *FlightWait) Forget(loc message.Location) { delete(w.pending, loc) }

// Wait waits until every Object the wait covers has arrived or been
// abandoned, or until deadline or ctx ends, then stops the wait. It returns
// the Objects handed over and the Locations still arriving when it gave up;
// an abandoned Object is in neither, its status left to the cache and the
// ledger.
func (w *FlightWait) Wait(
	ctx context.Context,
	deadline time.Time,
) (done []*cache.CachedObject, stillArriving []message.Location) {
	defer w.Stop()
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	for len(w.pending) > 0 {
		select {
		case <-w.notify:
			w.drain()
		case <-timer.C:
			w.drain() // what completed just in time still counts
			return w.done, w.StillArriving()
		case <-ctx.Done():
			w.drain()
			return w.done, w.StillArriving()
		}
	}
	return w.done, nil
}

// drain applies the results the slots have posted. An Object every stream it
// was registered on abandoned is still waited for if another stream has
// started reading it since: its copy can still arrive whole.
func (w *FlightWait) drain() {
	w.entry.mu.Lock()
	defer w.entry.mu.Unlock()
	results := w.results
	w.results = nil
	for _, r := range results {
		if _, ok := w.pending[r.loc]; !ok {
			continue // resolved by another stream's copy, or forgotten
		}
		if r.obj != nil {
			w.done = append(w.done, r.obj)
			delete(w.pending, r.loc)
			continue
		}
		if w.pending[r.loc]--; w.pending[r.loc] > 0 {
			continue
		}
		delete(w.pending, r.loc)
		for f := range w.entry.inflight {
			if f.active && f.loc == r.loc && !slices.Contains(f.waiters, w) {
				w.registerLocked(f)
			}
		}
	}
}

// StillArriving returns the Locations the wait covers and has not resolved:
// after a Wait that gave up, those still arriving; before any Wait, what a
// FETCH that may not wait (FILL_TIMEOUT 0) reports as timed out.
func (w *FlightWait) StillArriving() []message.Location {
	return slices.Collect(maps.Keys(w.pending))
}

// Stop unregisters the wait from the slots that have not ended, so a stream
// stalled mid-payload does not hold the waits of FETCHes that gave up.
func (w *FlightWait) Stop() {
	w.entry.mu.Lock()
	for _, f := range w.slots {
		f.waiters = slices.DeleteFunc(f.waiters, func(x *FlightWait) bool { return x == w })
	}
	w.entry.mu.Unlock()
}
