package session

import (
	"context"
	"errors"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// SubgroupHandler handles one inbound SUBGROUP_HEADER stream that a [Demux]
// routed to it by §11.1 Track Alias. It is invoked synchronously — usually by
// [Demux.Run], but also by [Demux.HandleTrack], which hands over any streams
// parked before the handler existed on the goroutine that registers it. Those
// two can therefore overlap: a handler holding per-track state of its own must
// synchronise it. Spawn a goroutine inside the handler when streams must be
// processed concurrently (see [Demux.Run]).
type SubgroupHandler func(*IncomingSubgroupStream)

// FetchHandler handles one inbound FETCH_HEADER stream that a [Demux] routed to
// it by §11.5 Request ID. Invoked synchronously by [Demux.Run].
type FetchHandler func(*IncomingFetchStream)

// Demux routes the data streams accepted from a [Session] to per-track and
// per-request handlers, replacing the hand-rolled "AcceptDataStream loop +
// type-switch + Track-Alias match" a subscriber otherwise writes.
//
// Subgroup streams are dispatched by their §11.1 Track Alias — the value a
// subscriber gets from [Subscription.TrackAlias]; FETCH streams by their §11.4.4
// Request ID — the ID the subscriber's FETCH was assigned. A FETCH stream with
// no registered handler is passed to the OnUnknown callback; an unmatched
// subgroup stream is parked instead, for the reasons below.
//
// Handlers may be registered or replaced at any time, including while
// [Demux.Run] is executing: a subscriber learns a track's alias only from its
// SUBSCRIBE_OK, which can arrive after Run has started. Registration is
// safe for concurrent use.
//
// A subgroup stream whose Track Alias has no handler yet is PARKED rather than
// passed to OnUnknown, and released to the handler [Demux.HandleTrack]
// registers for that alias. This is not a nicety: a publisher may start
// sending a track's subgroup streams as soon as it has accepted the SUBSCRIBE,
// which can be before SUBSCRIBE_OK has come back and named the alias to
// register under, so the first Groups of a live broadcast routinely arrive
// with nowhere to go. Resetting them loses that media, and against at least
// one CDN it did worse — two streams reset on arrival and the subscription
// then delivered nothing for the rest of the run.
//
// §11.4.2 says exactly what the choice is — "if an endpoint receives a
// subgroup with an unknown Track Alias, it MAY abandon the stream, or choose
// to buffer it for a brief period to handle reordering with the control
// message that establishes the Track Alias" — and abandoning was measured to
// cost whole runs.
//
// "A brief period" is what [parkLimit] bounds: at most that many streams wait
// per alias, past which the oldest is reset, and [Demux.Run] resets whatever
// is still parked when it returns. Without a bound a stream for an alias
// nobody ever resolves would sit open with its flow control withheld for the
// life of the session, where resetting it at least frees the peer.
//
// OnUnknown therefore sees FETCH streams with no registered handler, not
// subgroup streams.
//
// The zero value is not ready for use — construct with [NewDemux].
type Demux struct {
	mu        sync.Mutex
	subgroup  map[uint64]SubgroupHandler // keyed by Track Alias
	fetch     map[uint64]FetchHandler    // keyed by Request ID
	parked    map[uint64][]*IncomingSubgroupStream
	parkedN   int                 // total across parked, kept in step with it
	retired   map[uint64]struct{} // aliases registered and then unregistered
	onUnknown func(DataStream)
}

// parkLimit is how many subgroup streams may wait for one Track Alias at once,
// and parkTotalLimit how many may wait across all of them. The window being
// covered is a single control-message round trip, so a few Groups' worth is
// the right order; past either bound a stream is reset, which is §11.4.2's
// other option ("MAY abandon the stream").
//
// Both are counts, where §11.4.2 says "for a brief period" — time. Nothing
// here evicts on a timer: with fewer than parkLimit streams for an alias that
// is never claimed, they wait until [Demux.Run] returns. What the counts bound
// is how much can be pinned open at once, which is the part that matters for
// the deadlock §11.4.2 warns about.
//
// parkTotalLimit exists because per-alias bounding alone is not a bound: a
// peer opening subgroup streams for many bogus aliases would park parkLimit of
// each. A parked stream is header-parsed and then unread, so its body sits in
// the transport's receive buffers and consumes the CONNECTION-level window —
// and §11.4.2 continues, past the sentence quoted on [Demux]: "To prevent
// deadlocks, endpoints MUST allocate connection flow control to the control
// streams before allocating it to any data streams. Otherwise, a receiver
// might wait for a control message containing a Track Alias to release flow
// control, while the sender waits for flow control to send the message." That
// MUST binds the transport adapter, which is below this layer and does not
// currently enforce it; parkTotalLimit is what keeps Demux from being the
// thing that walks into it.
const (
	parkLimit      = 8
	parkTotalLimit = 32
)

// NewDemux returns an empty Demux ready for handler registration.
func NewDemux() *Demux {
	return &Demux{
		subgroup: make(map[uint64]SubgroupHandler),
		fetch:    make(map[uint64]FetchHandler),
		parked:   make(map[uint64][]*IncomingSubgroupStream),
		retired:  make(map[uint64]struct{}),
	}
}

// HandleTrack registers h for inbound subgroup streams whose Track Alias is
// alias — typically the [Subscription.TrackAlias] of a subscription this side
// opened. A nil h unregisters alias; registering an alias that already has a
// handler replaces it.
//
// Streams for alias that arrived before this call were parked (see [Demux])
// and are handed to h now, in arrival order, before HandleTrack returns.
// It reports how many — a caller whose output is timing-sensitive wants to
// know that those Groups arrived earlier than they were read.
func (d *Demux) HandleTrack(alias uint64, h SubgroupHandler) (released int) {
	d.mu.Lock()
	if h == nil {
		delete(d.subgroup, alias)
		// §11.1: "Objects can arrive after a subscription has been
		// cancelled. Subscribers SHOULD retain sufficient state to quickly
		// discard these unwanted Objects, rather than treating them as
		// belonging to an unknown Track Alias." Retiring the alias is that
		// state: what is already parked goes now, and anything later for
		// this alias is discarded on arrival rather than parked, since the
		// control message parking waits for is never coming.
		d.retired[alias] = struct{}{}
		stale := d.parked[alias]
		d.dropParkedLocked(alias)
		d.mu.Unlock()
		for _, s := range stale {
			s.Cancel(moqt.StreamResetCancelled)
		}
		return 0
	}
	delete(d.retired, alias) // re-subscribed under the same alias
	d.subgroup[alias] = h
	held := d.parked[alias]
	d.dropParkedLocked(alias)
	d.mu.Unlock()

	// Outside the lock: a handler may block for the life of the stream.
	for _, s := range held {
		h(s)
	}
	return len(held)
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

// OnUnknown sets the callback invoked for an accepted FETCH stream that
// matches no registered handler. With no callback set (the default, or a nil
// f), such a stream is reset with StreamResetInternalError and dropped so it
// does not leak.
//
// It does NOT see subgroup streams. One whose Track Alias has no handler is
// parked (see [Demux]) rather than reported, because at that point it is far
// more likely to be early than unwanted. One arriving for an alias that was
// registered and then unregistered is reset immediately, per §11.1.
func (d *Demux) OnUnknown(f func(DataStream)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.onUnknown = f
}

// parkLocked holds s until [Demux.HandleTrack] claims its alias, resetting the
// oldest once parkLimit is exceeded. Caller holds d.mu.
func (d *Demux) parkLocked(s *IncomingSubgroupStream) bool {
	alias := s.Header.TrackAlias
	if _, retired := d.retired[alias]; retired {
		return false // §11.1: discard promptly, do not buffer.
	}
	if d.parkedN >= parkTotalLimit {
		return false // see parkTotalLimit.
	}
	d.parked[alias] = append(d.parked[alias], s)
	d.parkedN++
	for len(d.parked[alias]) > parkLimit {
		d.parked[alias][0].Cancel(moqt.StreamResetCancelled)
		d.parked[alias] = d.parked[alias][1:]
		d.parkedN--
	}
	return true
}

// dropParkedLocked forgets alias's queue, keeping parkedN in step. It does not
// touch the streams; the caller owns them. Caller holds d.mu.
func (d *Demux) dropParkedLocked(alias uint64) {
	d.parkedN -= len(d.parked[alias])
	delete(d.parked, alias)
}

// discardParked resets everything still waiting for an alias nobody claimed,
// so a stream does not sit open holding its flow control for the life of the
// session. §3.3.4 asks for a relevant code, and a subscriber winding down is
// not an implementation fault: reporting one would have a peer's metrics blame
// this end for a normal end of run.
func (d *Demux) discardParked() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, held := range d.parked {
		for _, s := range held {
			s.Cancel(moqt.StreamResetCancelled)
		}
	}
	clear(d.parked)
	d.parkedN = 0
}

// Run accepts data streams from sess and dispatches each to its registered
// handler until ctx is cancelled or [Session.AcceptDataStream] returns a
// non-padding error, which Run returns. Padding streams (§11.5.1) are skipped.
//
// Dispatch is synchronous: a handler runs to completion before Run accepts the
// next stream, mirroring a hand-written accept loop. A handler that reads a
// long-lived stream therefore blocks the loop, so spawn a goroutine inside the
// handler when streams must be read concurrently.
func (d *Demux) Run(ctx context.Context, sess *Session) error {
	defer d.discardParked()
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
		d.mu.Lock()
		h := d.subgroup[s.Header.TrackAlias]
		if h == nil {
			// Early, not unwanted — unless the alias is retired or the
			// park is full, in which case §11.4.2's other option applies.
			parked := d.parkLocked(s)
			d.mu.Unlock()
			if !parked {
				s.Cancel(moqt.StreamResetCancelled)
			}
			return
		}
		d.mu.Unlock()
		h(s)
		return
	case *IncomingFetchStream:
		d.mu.Lock()
		h := d.fetch[s.Header.RequestID]
		d.mu.Unlock()
		if h != nil {
			h(s)
			return
		}
	}
	d.unknown(ds)
}

func (d *Demux) unknown(ds DataStream) {
	d.mu.Lock()
	f := d.onUnknown
	d.mu.Unlock()
	if f != nil {
		f(ds)
		return
	}
	ds.Cancel(moqt.StreamResetInternalError)
}
