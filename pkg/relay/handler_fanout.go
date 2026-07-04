package relay

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// fwdObject pairs a SubgroupObject with its absolute Object ID on the
// inbound stream. The writer goroutine needs both: the ObjectIDDelta on the
// wire is *relative to the previous object on its own stream*, so once
// filtering or §11.4.3 gap-driven stream resets can punch holes in the
// forwarded object sequence the relay MUST re-encode the delta on the
// outbound side or the subscriber's decoded absolute IDs will drift.
type fwdObject struct {
	obj        *message.SubgroupObject
	absID      uint64
	enqueuedAt time.Time // stamped in publish; used for the §8 lag window

	// first marks the subgroup's true first object: the first object read
	// off an inbound stream whose header had the §11.4.2 FIRST_OBJECT bit
	// set. A writer whose outbound stream begins with this object — and
	// only such a stream — sets FIRST_OBJECT on its own header.
	first bool
}

// subgroupWriterSet is the parent-managed payload of a
// [registry.SharedSubgroup]: the one outbound writer per downstream subscriber
// for a single (GroupID, SubgroupID), shared across every inbound runFanout
// goroutine producing that Subgroup (including redundant upstream publishers).
// All access is serialised by the [registry.SharedSubgroup.Mu] the registry
// hands back, so two contributors never double-open a writer or race the
// joiner scan.
//
// The map key is the *registry.DownstreamSub pointer: the sub's identity is
// exactly what the writer serves, with no ID indirection (IDs are globally
// unique since allocSubID went process-wide, but the pointer needs no lookup).
// A nil value records "sub wasn't Established when scanned" so we don't retry.
type subgroupWriterSet struct {
	writers map[*registry.DownstreamSub]*subgroupWriter
	// hdr is the canonical SUBGROUP_HEADER (the first contributor's), reused for
	// every writer open so joiners added by a redundant contributor get the same
	// Group/Subgroup framing. TrackAlias is overwritten per subscriber.
	hdr message.SubgroupHeader
	// gen is the entry.downstreamGen observed on the last joiner scan, so the
	// O(len(Downstream)) scan is skipped while membership is unchanged.
	gen uint64

	// sawClean records that at least one contributor ended its inbound stream
	// cleanly (io.EOF). With redundant upstreams a clean completion of the
	// Subgroup is authoritative: the merged outbound stream then FINs even if a
	// peer upstream reset. resetCode is the §3.3.3 code used only when NO
	// contributor ended cleanly (every upstream reset). These are written by each
	// contributor at release under the SharedSubgroup mutex.
	sawClean  bool
	resetCode moqt.StreamResetCode
}

// resolveImplicitSubgroupID handles §11.4.2 SUBGROUP_ID_MODE 0b01, where the
// Subgroup ID equals the stream's first Object ID: it reads the first object
// and rewrites hdr in place to the explicit form. The returned pending
// object must be processed as the stream's first (its delta is the absolute
// ID). Headers in any other mode pass through untouched with (nil, true).
//
// ok=false means the stream ended before its identity resolved — an empty
// 0b01 stream (clean EOF) has nothing to forward, and a read error means the
// stream died; there is no subgroup state to join or tear down yet, so the
// caller just returns.
func (h *sessionHandler) resolveImplicitSubgroupID(
	ctx context.Context,
	stream *session.IncomingSubgroupStream,
	hdr *message.SubgroupHeader,
) (pending *message.SubgroupObject, ok bool) {
	if hdr.SubgroupIDMode != message.SubgroupIDImplicitFirstObject {
		return nil, true
	}
	if hdr.ReplayingSubgroup {
		// §11.4.2's receiver rule is mechanical (Subgroup ID = first
		// object on the stream), but on a replay the first object is not
		// necessarily the subgroup's first — the implied ID is only as
		// reliable as the sender. Worth a trace when it leads to
		// mis-keyed subgroups.
		h.log.LogAttrs(ctx, slog.LevelDebug,
			"fanout: implicit-first-object Subgroup ID on a replay stream",
			slog.Uint64("group", hdr.GroupID))
	}
	obj, err := stream.ReadObject()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"fanout: inbound stream ended before first-object Subgroup ID resolved",
				slog.String("err", err.Error()))
			// Stop a publisher still writing into a stream nobody reads
			// (a STOP_SENDING on an already-reset stream is a transport
			// no-op).
			stream.Cancel(moqt.StreamResetInternalError)
		}
		return nil, false
	}
	hdr.SubgroupID = obj.ObjectIDDelta // first object: the delta IS the absolute ID
	hdr.SubgroupIDMode = message.SubgroupIDExplicit
	return obj, true
}

// runFanout is the subgroup-stream fanout entry point. One inbound
// SUBGROUP_HEADER stream produces one or more outbound SUBGROUP_HEADER
// streams per downstream subscriber, with the publisher's Track Alias
// remapped to the subscriber's per-session outbound alias.
//
// §9.5 multiple publishers: many inbound streams may carry the same
// (GroupID, SubgroupID) — independent publishers, a switchover overlap, or
// redundant origins. They share ONE outbound writer per subscriber (§2.2 forbids
// splitting a Subgroup across streams) via the entry's [registry.SharedSubgroup],
// and the §2.1 dedup ledger ([registry.TrackEntry.ClaimDelivered]) drops the
// second and later copy of each {GroupID, ObjectID} so the subscriber sees each
// object exactly once. A single publisher is just the one-contributor case.
//
// The per-subscriber forward path runs in a dedicated [subgroupWriter]
// goroutine fed by a bounded send queue: §5.1.2 filters are evaluated
// pre-enqueue and ObjectIDDelta re-encoded outbound so drops don't shift the
// subscriber's absolute IDs. The inbound FIN-vs-reset distinction propagates to
// the outbound streams only when the LAST contributor leaves.
func (h *sessionHandler) runFanout(ctx context.Context, stream *session.IncomingSubgroupStream) {
	hdr := stream.Header

	key, ok := stream.TrackKey()
	if !ok {
		// Per §11.1, a Track Alias on a data stream must have been
		// previously registered (via SUBSCRIBE_OK or PUBLISH). An
		// unknown alias is a publisher protocol error scoped to this
		// stream — reset the stream but keep the session alive.
		h.log.LogAttrs(ctx, slog.LevelWarn, "fanout: unknown inbound Track Alias",
			slog.Uint64("alias", hdr.TrackAlias))
		stream.Cancel(moqt.StreamResetInternalError)
		return
	}

	entry, ok := h.tracks.Get(key)
	if !ok {
		// Alias was registered but the entry has since been removed —
		// the subscription terminated between alias registration and
		// the first object arriving.
		h.log.LogAttrs(ctx, slog.LevelDebug, "fanout: track entry gone, dropping stream",
			slog.Uint64("alias", hdr.TrackAlias))
		stream.Cancel(moqt.StreamResetInternalError)
		return
	}

	// §11.4.2 mode 0b01: the Subgroup ID is implied by the stream's FIRST
	// object's ID. Everything from here on keys on hdr.SubgroupID — the
	// shared-subgroup key, the cache (and thus FETCH responses), and the
	// outbound header template — so resolve it before touching any of that.
	// The pre-read object is fed through the normal loop below.
	pending, ok := h.resolveImplicitSubgroupID(ctx, stream, &hdr)
	if !ok {
		return
	}

	// Join (or create) the shared fan-out state for this (group, subgroup). The
	// first contributor opens writers for the current Downstream snapshot;
	// redundant contributors reuse the existing set and only add joiners /
	// deliver deduped objects.
	sgKey := registry.SubgroupKey{Group: hdr.GroupID, Subgroup: hdr.SubgroupID}
	sg, created := entry.AcquireSubgroup(sgKey, func() any {
		return &subgroupWriterSet{
			writers: make(map[*registry.DownstreamSub]*subgroupWriter),
			hdr:     hdr,
		}
	})
	set, _ := sg.Set.(*subgroupWriterSet)

	if created {
		// Open initial writers from the current Downstream snapshot, under
		// sg.Mu so a concurrent contributor's joiner scan can't double-open.
		// Per §9.7 we drain even with zero subscribers (publisher flow control);
		// the per-object joiner scan picks up subs that join mid-stream.
		initialSubs, gen := entry.CopyDownstreamWithGen()
		sg.Mu.Lock()
		set.gen = gen
		for _, sub := range initialSubs {
			h.openWriterForSub(ctx, set.hdr, sub, set.writers)
		}
		sg.Mu.Unlock()
	}

	// inboundReset records THIS contributor's termination mode: false on clean
	// io.EOF, true on any other read error. inboundResetCode is the §3.3.3 code.
	// They are applied to the outbound streams only when this is the LAST
	// contributor to leave the Subgroup — a single publisher dropping out (clean
	// or reset) while others still feed the Subgroup must not disturb the
	// subscribers' streams (§9.5 fault tolerance).
	var (
		inboundReset     bool
		inboundResetCode = moqt.StreamResetCancelled
	)
	defer func() {
		// Record this contributor's outcome into the shared set before we drop
		// our reference, so the last contributor can decide FIN vs reset over ALL
		// contributors (§11.4.3 redundancy: a clean completion by any upstream
		// FINs the merged stream even if a peer reset).
		sg.Mu.Lock()
		if inboundReset {
			set.resetCode = inboundResetCode
		} else {
			set.sawClean = true
		}
		last := entry.ReleaseSubgroup(sgKey)
		if !last {
			sg.Mu.Unlock()
			return // other upstreams still feed this Subgroup — leave writers up.
		}
		// Last contributor: close and drain every downstream writer. FIN if any
		// upstream completed cleanly; otherwise reset with the recorded code.
		reset := !set.sawClean
		code := set.resetCode
		ws := make([]*subgroupWriter, 0, len(set.writers))
		for _, w := range set.writers {
			if w == nil {
				continue
			}
			wReset, wCode := reset, code
			// §11.4.3: a Subgroup whose group has fallen outside the
			// subscription's range (e.g. a REQUEST_UPDATE narrowed it) MUST
			// be reset, not FIN'd, even on a clean inbound EOF — a FIN would
			// falsely signal the group was fully delivered.
			if !wReset && registry.GroupOutOfRange(hdr.GroupID, w.sub.GetFilter()) {
				wReset, wCode = true, moqt.StreamResetCancelled
			}
			w.close(wReset, wCode)
			ws = append(ws, w)
		}
		sg.Mu.Unlock()
		joinWriters(ws)
	}()

	// objectID tracks the running absolute Object ID across the inbound
	// stream. Per §11.4.2, ObjectIDDelta on the wire is the absolute Object
	// ID for the first object, and (currentID - previousID - 1) for every
	// subsequent object — so sequential IDs all encode as 0.
	var (
		objectID uint64
		firstObj = true
		// terminalSeen records that a terminal-status object (EndOfGroup /
		// EndOfTrack) has been seen on this Subgroup stream. Per §11.4.3
		// no further objects may follow it; one that does makes the track
		// malformed (§2.4.2). Tracked per inbound stream so a redundant
		// upstream's own terminal accounting is independent.
		terminalSeen bool
	)

	for {
		// The first object of a mode-0b01 stream was already read during
		// Subgroup ID resolution above.
		obj, err := pending, error(nil)
		pending = nil
		if obj == nil {
			obj, err = stream.ReadObject()
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // clean end of stream — last contributor will FIN.
			}
			if errors.Is(err, context.Canceled) {
				// ctx cancellation is treated as a reset — the
				// session is going away and we can't safely
				// FIN the outbound streams.
				inboundReset = true
				return
			}
			h.log.LogAttrs(ctx, slog.LevelDebug, "fanout: inbound ReadObject failed",
				slog.String("err", err.Error()))
			// A malformed object (not a transport reset) leaves the
			// publisher still writing; stop it. On an already-reset
			// stream the STOP_SENDING is a transport no-op.
			stream.Cancel(moqt.StreamResetInternalError)
			inboundReset = true
			return
		}

		// §11.4.3 / §2.4.2: an object after a terminal-status object on the
		// same Subgroup stream is a protocol violation. Reset the inbound and
		// (if last) outbound streams with MALFORMED_TRACK rather than forwarding.
		if terminalSeen {
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"fanout: object after EndOfGroup/EndOfTrack — malformed track",
				slog.Uint64("group", hdr.GroupID), slog.Uint64("subgroup", hdr.SubgroupID))
			stream.Cancel(moqt.StreamResetMalformedTrack)
			inboundReset = true
			inboundResetCode = moqt.StreamResetMalformedTrack
			return
		}

		// §11.4.2: the subgroup's true first object is the first object on
		// an inbound stream whose header carried the FIRST_OBJECT bit
		// (ReplayingSubgroup false). Writers use this to set the bit on
		// their own outbound headers only when their stream really begins
		// with it.
		isTrueFirst := firstObj && !hdr.ReplayingSubgroup
		if firstObj {
			objectID = obj.ObjectIDDelta
			firstObj = false
		} else {
			objectID += obj.ObjectIDDelta + 1
		}

		// §11.4.3: terminal status is tracked per inbound stream regardless of
		// whether this copy wins the dedup claim below, so a post-terminal object
		// on THIS stream is still caught at the top of the next iteration.
		terminal := obj.IsTerminal()

		// §2.1 dedup across redundant upstreams: claim {GroupID, ObjectID} on the
		// entry's persistent, group-windowed ledger. The first upstream to reach an
		// object forwards it; a later copy from a peer — even one that is lagging,
		// or that arrives on a fresh stream after the first upstream's stream has
		// already FIN'd — is dropped here so the subscriber sees each object once.
		// Done outside sg.Mu (its own lock) so dedup losers never touch the writer
		// set.
		if !entry.ClaimDelivered(hdr.GroupID, objectID) {
			if terminal {
				terminalSeen = true
			}
			continue // redundant copy already forwarded by a peer upstream.
		}

		// Deliver to the shared writer set under sg.Mu so joiner detection, writer
		// open, and the publish loop are atomic against a concurrent contributor
		// and the last-contributor teardown.
		sg.Mu.Lock()

		// Cache the object (for joining FETCHes) before bumping LARGEST_OBJECT so
		// a concurrent handleSubscribe-then-FETCH that snapshots the new watermark
		// always finds it cached.
		entry.Cache.Put(&cache.CachedObject{
			GroupID:           hdr.GroupID,
			ObjectID:          objectID,
			SubgroupID:        hdr.SubgroupID,
			PublisherPriority: hdr.PublisherPriority,
			ForwardingPref:    cache.ForwardingSubgroup,
			Status:            obj.ObjectStatus,
			Properties:        obj.Properties,
			Payload:           obj.Payload,
		})

		// Atomically bump §10.2.11 LARGEST_OBJECT and snapshot any Downstream
		// subs that joined since the last scan. The entry.mu acquisition inside
		// serialises with handleSubscribe's AddDownstreamSnapshotLargest: a new
		// sub either snapshots the pre-update Largest AND appears in newSubs
		// (delivered live below), or snapshots the post-update Largest (its
		// Joining FETCH covers this object — already cached above).
		loc := message.Location{Group: hdr.GroupID, Object: objectID}
		var newSubs []*registry.DownstreamSub
		newSubs, set.gen = entry.UpdateLargestAndDetectNew(loc,
			func(s *registry.DownstreamSub) bool { _, ok := set.writers[s]; return ok }, set.gen)
		for _, sub := range newSubs {
			h.openWriterForSub(ctx, set.hdr, sub, set.writers)
		}

		// §5.1.2 filter evaluation per-subscriber, pre-enqueue. A filter miss
		// means we don't take a queue slot. Per §9.7 the relay does not modify
		// the object; it is purely a forwarding gate.
		for _, w := range set.writers {
			if w == nil {
				continue
			}
			// One lock acquisition folds the §9.2 Forward-State gate and the
			// §5.1.2 filter test. A paused subscription (Forward State 0) takes
			// no queue slot; control messages on its request stream still flow.
			forward, groupExhausted := w.sub.ForwardDecision(hdr.GroupID, objectID)
			if !forward {
				// §11.4.3: if the subscription has narrowed so this whole
				// group is now out of range, the stream will never carry
				// another object — reset it promptly (not FIN). close is
				// idempotent; the teardown still waits on w.done.
				if groupExhausted {
					w.close(true, moqt.StreamResetCancelled)
				}
				continue
			}
			w.publish(fwdObject{obj: obj, absID: objectID, first: isTrueFirst})
		}
		sg.Mu.Unlock()

		if terminal {
			terminalSeen = true
		}
	}
}

// openWriterForSub builds a subgroupWriter for sub and records it in writers
// keyed by the *registry.DownstreamSub pointer. If sub is not Established,
// writers[sub] is set to nil so we don't retry. The §11.4.2 FIRST_OBJECT bit
// is not decided here: the writer computes it per outbound stream, from
// whether the first object it actually writes is the subgroup's true first
// (see [fwdObject.first]) — a joiner, a filter that drops the head of the
// subgroup, and a §11.4.3 gap-reopen all end up with the bit clear.
//
// Deliberately NO transport I/O happens here: both call sites run under
// sg.Mu — the lock every contributor takes per forwarded object — and
// writing the SUBGROUP_HEADER can block on ONE subscriber's flow control,
// which would stall the entire subgroup's fanout (plus the inbound read
// loop) on one slow peer. The writer goroutine opens the stream and writes
// the header lazily, before the first object it forwards; a subscriber whose
// filter drops every object never gets an empty header-only stream at all.
func (h *sessionHandler) openWriterForSub(
	ctx context.Context,
	hdr message.SubgroupHeader,
	sub *registry.DownstreamSub,
	writers map[*registry.DownstreamSub]*subgroupWriter,
) {
	if _, already := writers[sub]; already {
		return
	}
	if !sub.IsEstablished() {
		writers[sub] = nil
		return
	}
	subHdr := hdr
	subHdr.TrackAlias = sub.TrackAlias
	// ioCtx bounds every blocking stream operation the writer performs
	// (open, header write, object writes): cancelIO unblocks a writer
	// wedged on a subscriber that stopped reading, so the teardown join
	// cannot be held hostage (see [subgroupWriter.join]).
	ioCtx, cancelIO := context.WithCancel(ctx)
	w := &subgroupWriter{
		sub:                 sub,
		ctx:                 ioCtx,
		cancelIO:            cancelIO,
		hdr:                 subHdr,
		inbox:               make(chan fwdObject, h.sendQueueSize),
		done:                make(chan struct{}),
		log:                 h.log,
		metrics:             h.metrics,
		maxDropsBeforeReset: h.maxDropsBeforeReset,
		maxLag:              h.maxFanoutLag,
	}
	writers[sub] = w
	h.spawn(w.run)
}

// subgroupWriter is the per-subscriber writer goroutine. It consumes
// objects from an inbox channel and writes them to outbound
// SUBGROUP_HEADER streams on the subscriber's session.
//
// §11.4.3 lifecycle:
//
//   - When the next forwarded Object ID is not (prevWrittenID + 1) — i.e.
//     the inbound or filter punched a hole — the current outbound stream
//     is reset and a fresh one opened. Per §11.4.3 the relay MUST NOT
//     forward a non-consecutive Object on an existing subgroup stream.
//   - On clean inbound EOF the outbound stream is FIN'd; on inbound error
//     (or ctx-cancel) it is reset.
//   - When the inbox overflows (publisher fills it faster than the QUIC
//     send window drains), the publish path drops the object. Each object
//     records its enqueue time; if the writer later dequeues one that waited
//     longer than maxLag, the subscriber has fallen too far behind the live
//     edge (§8 Delivery Timeouts) and the writer resets its outbound stream
//     with TOO_FAR_BEHIND (§3.3.3), transitions the [registry.DownstreamSub] to
//     [registry.SubTerminated], and exits. The optional maxDropsBeforeReset cap is a
//     coarse backstop on cumulative drops, reset with EXCESSIVE_LOAD instead.
type subgroupWriter struct {
	sub *registry.DownstreamSub
	// ctx is writer-scoped: it bounds every blocking stream operation
	// (open, header write, object writes). cancelIO cancels it, resetting
	// the in-flight stream via the per-stream bridge in reopen — the
	// escape hatch for a writer wedged on a subscriber that stopped
	// reading (close only closes the inbox, and the §8 lag check runs
	// only between dequeues).
	ctx                 context.Context
	cancelIO            context.CancelFunc
	hdr                 message.SubgroupHeader          // template; TrackAlias already remapped
	out                 *session.OutgoingSubgroupStream // nil until run opens it lazily
	unbridge            func() bool                     // stops the current stream's ctx→Cancel bridge
	inbox               chan fwdObject
	done                chan struct{}
	log                 *slog.Logger
	metrics             Metrics
	maxDropsBeforeReset int
	maxLag              time.Duration

	closeOnce        sync.Once
	dropsMu          sync.Mutex
	drops            int
	closed           bool                 // set under dropsMu inside close
	inboundReset     bool                 // set under dropsMu inside close
	inboundResetCode moqt.StreamResetCode // §3.3.3 reset code when inboundReset; set inside close
}

// publish does a non-blocking send onto the inbox, stamping the enqueue time
// so the writer goroutine can measure how long the object waited before it was
// written (the §8 lag window — see [subgroupWriter.run]). On overflow the
// object is dropped; if the optional MaxDropsBeforeReset cap is enabled and
// exceeded, the writer is closed in reset mode so its goroutine terminates the
// subscription.
//
// After close has been called the writer no longer accepts objects; publish
// returns silently in that case to avoid sending on a closed channel
// (which would panic).
func (w *subgroupWriter) publish(fwd fwdObject) {
	w.dropsMu.Lock()
	if w.closed {
		w.dropsMu.Unlock()
		return
	}
	w.dropsMu.Unlock()

	fwd.enqueuedAt = time.Now()
	select {
	case w.inbox <- fwd:
		w.metrics.ObjectForwarded()
	default:
		w.metrics.ObjectDropped()
		w.dropsMu.Lock()
		w.drops++
		drops := w.drops
		capped := w.maxDropsBeforeReset > 0 && w.drops > w.maxDropsBeforeReset
		w.dropsMu.Unlock()

		w.log.Debug("fanout: dropped object on full inbox",
			"sub_id", w.sub.ID, "drops", drops)
		if capped {
			w.log.Warn("fanout: subscriber hit MaxDropsBeforeReset cap, terminating",
				"sub_id", w.sub.ID, "drops", drops)
			// Close the inbox; the writer goroutine's post-drain path resets
			// the outbound stream with EXCESSIVE_LOAD and terminates the sub.
			w.close(true, moqt.StreamResetExcessiveLoad)
		}
	}
}

// run is the writer goroutine. It drains the inbox and writes objects to the
// outbound stream until the inbox is closed, then decides the stream's fate
// (FIN / reset) from the flags close recorded — see the post-drain block below.
//
// If WriteObject fails mid-stream (QUIC-level error) the writer cancels the
// outbound stream, marks writeFailed, and keeps draining until close is called,
// keeping publish non-blocking without a second drain goroutine.
func (w *subgroupWriter) run() {
	defer close(w.done)

	var (
		prevID      uint64
		hasWritten  bool
		writeFailed bool
	)

	// reopen cancels the current outbound stream (if any) and opens a fresh
	// one, writing its SUBGROUP_HEADER. Used for the lazy first open and
	// when a §11.4.3 gap is detected — the current stream is no longer
	// eligible to carry the next forwarded object. The effective §7.2
	// priority is reapplied on the new stream.
	//
	// first is whether the object about to go out is the subgroup's true
	// first object: only then does the header carry the §11.4.2
	// FIRST_OBJECT bit ("the first object in this subgroup stream is the
	// first object published in the subgroup by the original publisher").
	// A gap-reopen or a filtered head therefore clears it, and a header
	// whose Subgroup ID was implied by its first object (mode 0b01) is
	// rewritten to the explicit form — the replayed stream's first object
	// would imply the wrong ID.
	//
	// All blocking I/O is bounded by w.ctx: the open itself via
	// OpenSubgroupContext, and the stream's later writes via a
	// context.AfterFunc bridge to Cancel.
	reopen := func(first bool) bool {
		if w.unbridge != nil {
			w.unbridge()
			w.unbridge = nil
		}
		if w.out != nil {
			w.out.Cancel(moqt.StreamResetCancelled)
		}
		hdr := w.hdr
		hdr.ReplayingSubgroup = !first
		if !first && hdr.SubgroupIDMode == message.SubgroupIDImplicitFirstObject {
			// A replay stream's first object would imply the wrong ID, so
			// spell the Subgroup ID out. (Defensive: runFanout resolves
			// 0b01 headers to the explicit form at ingest, so the template
			// should never carry this mode here.)
			hdr.SubgroupIDMode = message.SubgroupIDExplicit
		}
		fresh, err := w.sub.Session.OpenSubgroupContext(w.ctx, hdr)
		if err != nil {
			w.log.Debug("fanout: OpenSubgroup (reopen) failed",
				"sub_id", w.sub.ID, "err", err.Error())
			w.out = nil
			return false
		}
		w.out = fresh
		w.unbridge = context.AfterFunc(w.ctx, func() {
			fresh.Cancel(moqt.StreamResetCancelled)
		})
		hasWritten = false
		w.applyPriority()
		// §11.4.3: keep the new stream's header reliable across resets.
		w.out.MarkReliable()
		return true
	}
	defer func() {
		if w.unbridge != nil {
			w.unbridge()
		}
	}()

	// failWrites latches this writer broken: no further stream writes will
	// be attempted, and — via w.closed — contributors stop enqueueing (and
	// stop counting ObjectForwarded for objects that would be discarded).
	// The inbox channel itself is only ever closed by close() under sg.Mu.
	var writeFailedLatched bool
	failWrites := func() {
		writeFailed = true
		if !writeFailedLatched {
			writeFailedLatched = true
			w.dropsMu.Lock()
			w.closed = true
			w.dropsMu.Unlock()
		}
	}

	var lagExceeded bool
	for fwd := range w.inbox {
		// §8 lag window: how long this object waited in the queue is how far
		// behind the live edge the subscriber is. Once that exceeds maxLag the
		// subscriber has been unable to keep up for too long — stop draining
		// and escalate to a reset below.
		if w.maxLag > 0 && time.Since(fwd.enqueuedAt) > w.maxLag {
			w.log.Warn("fanout: subscriber exceeded MaxFanoutLag, terminating",
				"sub_id", w.sub.ID, "lag", time.Since(fwd.enqueuedAt).String())
			lagExceeded = true
			break
		}

		if writeFailed {
			continue
		}

		// Lazy first open: openWriterForSub runs under sg.Mu and must not
		// perform transport I/O, so the stream (and its SUBGROUP_HEADER
		// write, which can block on this subscriber's flow control) happens
		// here, on this subscriber's own goroutine.
		if w.out == nil {
			if !reopen(fwd.first) {
				failWrites()
				continue
			}
		}

		// §11.4.3: the relay MUST NOT forward a non-consecutive
		// Object on an existing subgroup stream. When the next
		// forwarded Object ID isn't prevID + 1 — gap from a filter
		// drop, REQUEST_UPDATE end-shift, or out-of-order inbound —
		// reset the current outbound stream and open a new one.
		if hasWritten && fwd.absID != prevID+1 {
			if !reopen(fwd.first) {
				failWrites()
				continue
			}
		}

		// Re-encode ObjectIDDelta against the previous *forwarded*
		// Object ID on this outbound stream. After a fresh stream
		// open hasWritten is false and the first object carries its
		// absolute ID as the delta.
		out := *fwd.obj
		if !hasWritten {
			out.ObjectIDDelta = fwd.absID
		} else {
			out.ObjectIDDelta = fwd.absID - prevID - 1
		}
		if err := w.out.WriteObject(&out); err != nil {
			w.log.Debug("fanout: WriteObject failed",
				"sub_id", w.sub.ID, "err", err.Error())
			w.out.Cancel(moqt.StreamResetInternalError)
			w.out = nil
			failWrites()
			continue
		}
		prevID = fwd.absID
		hasWritten = true
		// §11.4.3: extend the reliable boundary to include this object so a
		// later reset (gap-reopen, inbound-reset propagation) still delivers
		// the Objects already forwarded on this stream.
		w.out.MarkReliable()
	}

	w.dropsMu.Lock()
	dropCapped := w.maxDropsBeforeReset > 0 && w.drops > w.maxDropsBeforeReset
	inboundReset := w.inboundReset
	inboundResetCode := w.inboundResetCode
	w.dropsMu.Unlock()

	if lagExceeded || dropCapped {
		// §8 slow-reader escalation: the subscriber fell too far behind the
		// live edge (lag window) or hit the optional drop cap. Reset the
		// outbound subgroup stream and terminate the subscription; the
		// subscriber must re-subscribe (likely with a more selective filter or
		// lower priority) to resume forwarding.
		//
		// §3.3.3 reset code: a lag-window breach is precisely TOO_FAR_BEHIND
		// (the subscriber can't keep up with the live edge). The cumulative
		// drop-cap backstop is server-side resource pressure, so it uses
		// EXCESSIVE_LOAD. A lag breach wins if both fired.
		resetCode := moqt.StreamResetTooFarBehind
		if dropCapped && !lagExceeded {
			resetCode = moqt.StreamResetExcessiveLoad
		}
		w.metrics.SubscriptionResetSlowReader()
		// Refuse further enqueues so contributors stop stamping objects into
		// an inbox nobody drains. The channel itself stays open (publish and
		// close serialize under sg.Mu; the writer must not close it from
		// here). Objects already queued stay pinned until the whole writer
		// becomes unreachable after the last contributor's teardown joins
		// this goroutine — bounded by the queue size.
		w.dropsMu.Lock()
		w.closed = true
		w.dropsMu.Unlock()
		if w.out != nil {
			w.out.Cancel(resetCode)
		}
		w.sub.Terminate()

		// Also cancel the subscriber's request stream so the
		// handleSubscribe goroutine's readSubscribeUpdates loop returns and
		// its defer removes this registry.DownstreamSub from the registry.TrackRegistry.
		// Without this the sub would linger in registry.SubTerminated state in
		// entry.Downstream until the subscriber's session itself
		// dies — runFanout would skip it (because !IsEstablished()),
		// but the registry entry would stay around.
		if w.sub.Stream != nil {
			w.sub.Stream.CancelRead(uint64(resetCode))
			w.sub.Stream.CancelWrite(uint64(resetCode))
		}
		return
	}

	if writeFailed {
		// Outbound stream is already cancelled (or never opened).
		return
	}

	if w.out == nil {
		// Either every object was filtered before the lazy first open (no
		// outbound stream ever existed) or a reopen failed; nothing to close.
		return
	}

	if inboundReset {
		// Inbound reset/error propagation per §11.4.3 ("Processing a
		// reset means that there might be other objects in the
		// Subgroup beyond the last one received. A relay might
		// immediately reset the corresponding downstream stream...").
		// inboundResetCode carries the §3.3.3 reason (CANCELLED for an
		// upstream reset / ctx-cancel, MALFORMED_TRACK for a §11.4.3
		// post-terminal-object violation).
		w.out.Cancel(inboundResetCode)
		return
	}

	// Clean inbound FIN propagation: every forwarded object that this
	// subscription wanted was delivered, so we FIN the outbound stream
	// per §11.4.3.
	_ = w.out.Close()
}

// applyPriority pushes the §7.2 effective priority for this writer's current
// outbound stream into the transport. It is called on stream open and §11.4.3
// reopen, so a mid-stream SUBSCRIBER_PRIORITY change takes effect on the next
// (re)open rather than in-flight. The key combines the publisher-priority,
// Group ID and Subgroup ID from the inbound header with the subscriber-priority
// and group-order from the subscription (§7.2 rules 1–4).
func (w *subgroupWriter) applyPriority() {
	if w.out == nil {
		return
	}
	w.out.SetSendPriority(w.sub.EffectiveStreamPriority(
		w.hdr.PublisherPriority, w.hdr.GroupID, w.hdr.SubgroupID,
	))
}

// close is idempotent. The reset argument is recorded so the writer
// goroutine's post-drain path can decide between FIN and Cancel on its
// current outbound stream; code is the §3.3.3 reason used when reset is true.
// Multiple callers may race to close; the first to enter the sync.Once wins,
// which matches the §11.4.3 intent: once an outbound stream's fate is decided,
// later changes don't apply.
//
// close never interrupts in-flight stream I/O — even in reset mode the
// writer first drains the objects already queued (they arrived before the
// inbound stream's fate was known and the subscriber is entitled to them).
// A writer that cannot finish because a write is wedged on the subscriber's
// flow control is bounded by [subgroupWriter.join].
func (w *subgroupWriter) close(reset bool, code moqt.StreamResetCode) {
	w.closeOnce.Do(func() {
		w.dropsMu.Lock()
		w.closed = true
		w.inboundReset = reset
		w.inboundResetCode = code
		w.dropsMu.Unlock()
		close(w.inbox)
	})
}

// defaultWriterJoinTimeout bounds [joinWriters] when no MaxFanoutLag is
// configured. It only matters for a writer wedged in a blocking stream
// write (subscriber alive but not reading), so it can be generous.
const defaultWriterJoinTimeout = 5 * time.Second

// joinTimeout is the escalation deadline for [joinWriters]: a healthy
// writer either finishes its drain within the §8 lag window or terminates
// itself via the lag check, so MaxFanoutLag (when configured) also bounds
// how long a drain can legitimately take.
func (w *subgroupWriter) joinTimeout() time.Duration {
	if w.maxLag > 0 {
		return w.maxLag
	}
	return defaultWriterJoinTimeout
}

// joinWriters waits for every writer goroutine to finish after close. A
// writer wedged inside a blocking stream write (open, header, or object —
// the subscriber is alive but not reading) never dequeues again, so neither
// the closed inbox nor the §8 lag check can end it; without a bound the
// caller (the subgroup's last inbound contributor) would be held hostage
// until the subscriber's session dies. All writers share ONE escalation
// deadline: when it expires, every still-running writer's stream I/O is
// cancelled at once — unblocking the wedged writes — so N stalled
// subscribers cost one timeout, not N.
func joinWriters(ws []*subgroupWriter) {
	if len(ws) == 0 {
		return
	}
	t := time.NewTimer(ws[0].joinTimeout()) // same handler config across ws
	defer t.Stop()
	for i, w := range ws {
		select {
		case <-w.done:
			continue
		case <-t.C:
			for _, u := range ws[i:] {
				select {
				case <-u.done:
					continue
				default:
				}
				u.log.Warn("fanout: writer did not finish draining, cancelling its stream I/O",
					"sub_id", u.sub.ID)
				u.cancelIO()
			}
			for _, u := range ws[i:] {
				<-u.done
			}
			return
		}
	}
}
