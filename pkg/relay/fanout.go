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
	enqueuedAt time.Time // stamped in publish; used for the §7.3 lag window
}

// groupOutOfRange reports whether a Subgroup belonging to group is entirely
// outside the subscription's filter range — i.e. no object in that group can
// ever pass — which makes its in-flight stream eligible for a §11.4.3 reset
// (e.g. after a REQUEST_UPDATE narrowed the End Group or raised the Start
// Location to a higher group). Only the absolute filters carry a fixed range;
// the dynamic (LargestObject / NextGroupStart) and unset filters never put a
// whole group permanently out of range, so they return false.
//
// A group equal to the Start Location's group is NOT out of range even when
// the Start Location's Object rose — objects at or above it still pass, so the
// stream stays relevant and object-level filtering handles the boundary.
func groupOutOfRange(group uint64, f *message.SubscriptionFilter) bool {
	if f == nil {
		return false
	}
	switch f.Type {
	case message.FilterAbsoluteStart:
		return group < f.StartLocation.Group
	case message.FilterAbsoluteRange:
		return group < f.StartLocation.Group || group > f.EndGroup()
	case message.FilterNextGroupStart, message.FilterLargestObject:
		// Dynamic start derived from the largest object; no fixed range that
		// puts a whole group permanently out of range.
		return false
	default:
		return false
	}
}

// runFanout is the subgroup-stream fanout entry point. One inbound
// SUBGROUP_HEADER stream produces one or more outbound SUBGROUP_HEADER
// streams per downstream subscriber, with the publisher's Track Alias
// remapped to the subscriber's per-session outbound alias.
//
// The per-subscriber forward path runs in a dedicated writer goroutine
// fed by a bounded send queue. §5.1.2 filters are evaluated pre-enqueue
// and ObjectIDDelta is re-encoded on the outbound side so filter drops
// don't shift the subscriber's decoded absolute IDs. §11.4.3 gap-driven
// reset/reopen is implemented; the inbound FIN-vs-reset distinction is
// propagated to the outbound streams; [TrackEntry.LargestObject] (§10.2.11)
// is bumped on every forwarded object.
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

	// writers maps a downstream sub to its outbound writer. The key is
	// the *DownstreamSub pointer itself rather than sub.ID because IDs
	// are allocated per-session-handler (allocSubID), so two subs from
	// different sessions can collide on ID. A nil value means we tried
	// to open a writer for that sub and failed — the entry keeps us
	// from retrying.
	//
	// Using a map (rather than the previous slice + parallel snapshot)
	// lets us cheaply detect "subs in entry.Downstream that we don't
	// have a writer for yet" on every object, which is how we close the
	// race against subs that join the entry after our initial snapshot.
	writers := make(map[*DownstreamSub]*subgroupWriter)

	// Open initial writers from the current Downstream snapshot. Done
	// outside any lock — OpenSubgroup is I/O.
	for _, sub := range entry.CopyDownstream() {
		h.openWriterForSub(ctx, hdr, sub, writers, false /* replaying */)
	}
	// Per §9.7 we still drain the inbound stream even with zero
	// subscribers so the publisher's flow control doesn't stall; the
	// per-object syncWriters loop below will pick up any sub that joins
	// mid-stream and open a (ReplayingSubgroup=true) writer for it, so
	// even a "no subscribers at first object" subgroup correctly delivers
	// to a sub that joins later.

	// inboundReset records the inbound termination mode for the writers:
	// false on clean io.EOF, true on any other read error. inboundResetCode is
	// the §3.3.3 code the outbound streams are reset with when inboundReset is
	// true. The deferred cleanup propagates both to the outbound streams per
	// §11.4.3 (FIN when the upstream FINs, reset when the upstream resets).
	var (
		inboundReset     bool
		inboundResetCode = moqt.StreamResetCancelled
	)
	defer func() {
		for _, w := range writers {
			if w == nil {
				continue
			}
			reset, code := inboundReset, inboundResetCode
			// §11.4.3: a Subgroup whose group has fallen outside the
			// subscription's range (e.g. a REQUEST_UPDATE narrowed it) MUST
			// be reset, not FIN'd, even on a clean inbound EOF — a FIN would
			// falsely signal the group was fully delivered.
			if !reset && groupOutOfRange(hdr.GroupID, w.sub.GetFilter()) {
				reset, code = true, moqt.StreamResetCancelled
			}
			w.close(reset, code)
		}
		for _, w := range writers {
			if w == nil {
				continue
			}
			<-w.done
		}
	}()

	// objectID tracks the running absolute Object ID across the inbound
	// stream. Per §11.4.2, ObjectIDDelta on the wire is the absolute Object
	// ID for the first object, and (currentID - previousID - 1) for every
	// subsequent object — so sequential IDs all encode as 0.
	var (
		objectID uint64
		firstObj = true
		// terminalSeen records that a terminal-status object (EndOfGroup /
		// EndOfTrack) has been forwarded on this Subgroup stream. Per §11.4.3
		// no further objects may follow it; one that does makes the track
		// malformed (§2.4.2).
		terminalSeen bool
	)

	for {
		obj, err := stream.ReadObject()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return // clean end of stream — writers will FIN.
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
			inboundReset = true
			return
		}

		// §11.4.3 / §2.4.2: an object after a terminal-status object on the
		// same Subgroup stream is a protocol violation. Reset the inbound and
		// outbound streams with MALFORMED_TRACK rather than forwarding it.
		if terminalSeen {
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"fanout: object after EndOfGroup/EndOfTrack — malformed track",
				slog.Uint64("group", hdr.GroupID), slog.Uint64("subgroup", hdr.SubgroupID))
			stream.Cancel(moqt.StreamResetMalformedTrack)
			inboundReset = true
			inboundResetCode = moqt.StreamResetMalformedTrack
			return
		}

		if firstObj {
			objectID = obj.ObjectIDDelta
			firstObj = false
		} else {
			objectID += obj.ObjectIDDelta + 1
		}

		// Atomically bump §10.2.11 LARGEST_OBJECT and snapshot any
		// Downstream subs that joined between the previous syncWriters
		// pass and this one. Doing both under one entry.mu acquisition
		// serialises with handleSubscribe's AddDownstreamSnapshotLargest:
		// a new sub either snapshots the pre-update Largest AND appears
		// in newSubs (so we open a writer for it below and deliver this
		// object via live), or it snapshots the post-update Largest (so
		// its Joining FETCH covers this object). Either way, no gap.
		loc := message.Location{Group: hdr.GroupID, Object: objectID}
		newSubs := entry.updateLargestAndDetectNew(loc, writers)

		// Cache the object AFTER UpdateLargest+newSubs detection so
		// that the LARGEST_OBJECT a concurrent
		// handleSubscribe-then-FETCH may snapshot is always backed by
		// a cached object the FETCH can serve.
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

		// Open writers for newly-joined subs outside the lock — I/O.
		// Mark ReplayingSubgroup since they're starting mid-stream.
		for _, sub := range newSubs {
			h.openWriterForSub(ctx, hdr, sub, writers, true /* replaying */)
		}

		// §5.1.2 filter evaluation per-subscriber, pre-enqueue. A
		// filter miss means we don't take a queue slot — slow-reader
		// escalation should be driven by objects the subscriber asked
		// for, not by ones it never wanted in the first place. Per
		// §9.7 the relay does not modify the object as part of this
		// decision; it is purely a forwarding gate.
		for _, w := range writers {
			if w == nil {
				continue
			}
			// §9.2: a subscription with Forward State 0 is paused — the
			// publisher does not send Objects until the subscriber sets
			// Forward State back to 1 (via REQUEST_UPDATE). Control
			// messages on the request stream still flow regardless.
			if w.sub.ForwardState() == 0 {
				continue
			}
			if !w.sub.PassesFilter(hdr.GroupID, objectID) {
				// §11.4.3: if the subscription has narrowed so this whole
				// group is now out of range, the stream will never carry
				// another object — reset it promptly (not FIN) rather than
				// leaving it open until the inbound subgroup ends. close is
				// idempotent, so repeating it on later filtered objects is a
				// no-op; the deferred cleanup still waits on w.done.
				if groupOutOfRange(hdr.GroupID, w.sub.GetFilter()) {
					w.close(true, moqt.StreamResetCancelled)
				}
				continue
			}
			w.publish(fwdObject{obj: obj, absID: objectID})
		}

		// §11.4.3: after a terminal-status object the Subgroup is complete;
		// any further object on this stream is a malformed-track violation,
		// caught at the top of the next iteration.
		if obj.IsTerminal() {
			terminalSeen = true
		}
	}
}

// openWriterForSub opens an outbound subgroup stream for sub, builds a
// subgroupWriter, and records it in writers keyed by the *DownstreamSub
// pointer. On any failure (sub not Established, OpenSubgroup error)
// writers[sub] is set to nil so we don't retry. If replaying is true, the outbound
// stream's header sets §11.4.2 ReplayingSubgroup — per the spec, "when
// the first Object on this stream is NOT the first object the original
// publisher pushed for this subgroup".
func (h *sessionHandler) openWriterForSub(
	ctx context.Context,
	hdr message.SubgroupHeader,
	sub *DownstreamSub,
	writers map[*DownstreamSub]*subgroupWriter,
	replaying bool,
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
	if replaying {
		subHdr.ReplayingSubgroup = true
	}
	os, err := sub.Session.OpenSubgroup(subHdr)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "fanout: OpenSubgroup failed",
			slog.Uint64("sub_id", sub.ID),
			slog.String("err", err.Error()))
		writers[sub] = nil
		return
	}
	w := &subgroupWriter{
		sub:                 sub,
		ctx:                 ctx,
		hdr:                 subHdr,
		out:                 os,
		inbox:               make(chan fwdObject, h.sendQueueSize),
		done:                make(chan struct{}),
		log:                 h.log,
		metrics:             h.metrics,
		maxDropsBeforeReset: h.maxDropsBeforeReset,
		maxLag:              h.maxFanoutLag,
	}
	w.applyPriority()
	// §11.4.3: mark the just-written SUBGROUP_HEADER as reliable so a later
	// reset still delivers it (the receiver can identify the subscription).
	w.out.MarkReliable()
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
//     edge (§7.3) and the writer resets its outbound stream, transitions the
//     [DownstreamSub] to [SubTerminated], and exits. The optional
//     maxDropsBeforeReset cap is a coarse backstop on cumulative drops.
type subgroupWriter struct {
	sub                 *DownstreamSub
	ctx                 context.Context
	hdr                 message.SubgroupHeader // template; TrackAlias already remapped
	out                 *session.OutgoingSubgroupStream
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
// written (the §7.3 lag window — see [subgroupWriter.run]). On overflow the
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

// run is the writer goroutine. It exits when its inbox is closed; close
// signals whether the inbound side ended cleanly (FIN propagation) or
// abnormally (reset propagation).
//
// If WriteObject fails mid-stream (QUIC-level error: stream torn down or
// session dead) the writer cancels the outbound stream, marks writeFailed,
// and keeps draining the inbox until close is called. This keeps publish
// non-blocking from the producer's side without spawning a second goroutine
// just to discard the tail of the queue.
//
// Stream-lifecycle decisions:
//
//   - The outbound stream was opened eagerly by runFanout, so the very
//     first forwarded object writes against that stream (with the
//     publisher's first absolute Object ID as the delta).
//   - A non-consecutive Object ID (gap) cancels the current outbound
//     stream and opens a new one before writing — §11.4.3 forbids
//     forwarding non-consecutive objects on the same subgroup stream.
//   - When the inbox closes, the post-drain path either FINs (clean
//     inbound EOF), cancels with [moqt.StreamResetCancelled] (inbound
//     reset propagation), or cancels with [moqt.StreamResetExcessiveLoad]
//     (slow-reader escalation).
func (w *subgroupWriter) run() {
	defer close(w.done)

	var (
		prevID      uint64
		hasWritten  bool
		writeFailed bool
	)

	// reopen cancels the current outbound stream and opens a fresh one
	// with the same header template. Used when a §11.4.3 gap is detected
	// — the current stream is no longer eligible to carry the next
	// forwarded object. The effective §7.2 priority is reapplied on
	// the new stream.
	reopen := func() bool {
		if w.out != nil {
			w.out.Cancel(moqt.StreamResetCancelled)
		}
		fresh, err := w.sub.Session.OpenSubgroup(w.hdr)
		if err != nil {
			w.log.Debug("fanout: OpenSubgroup (reopen) failed",
				"sub_id", w.sub.ID, "err", err.Error())
			w.out = nil
			return false
		}
		w.out = fresh
		hasWritten = false
		w.applyPriority()
		// §11.4.3: keep the new stream's header reliable across resets.
		w.out.MarkReliable()
		return true
	}

	var lagExceeded bool
	for fwd := range w.inbox {
		// §7.3 lag window: how long this object waited in the queue is how far
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

		// §11.4.3: the relay MUST NOT forward a non-consecutive
		// Object on an existing subgroup stream. When the next
		// forwarded Object ID isn't prevID + 1 — gap from a filter
		// drop, REQUEST_UPDATE end-shift, or out-of-order inbound —
		// reset the current outbound stream and open a new one.
		if hasWritten && fwd.absID != prevID+1 {
			if !reopen() {
				writeFailed = true
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
			writeFailed = true
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
		// §7.3 slow-reader escalation: the subscriber fell too far behind the
		// live edge (lag window) or hit the optional drop cap. Reset the
		// outbound subgroup stream and terminate the subscription; the
		// subscriber must re-subscribe (likely with a more selective filter or
		// lower priority) to resume forwarding.
		w.metrics.SubscriptionResetSlowReader()
		if w.out != nil {
			w.out.Cancel(moqt.StreamResetExcessiveLoad)
		}
		_ = w.sub.SetState(SubTerminated)

		// Also cancel the subscriber's request stream so the
		// handleSubscribe goroutine's DrainAndWait returns and its
		// defer removes this DownstreamSub from the TrackRegistry.
		// Without this the sub would linger in SubTerminated state in
		// entry.Downstream until the subscriber's session itself
		// dies — runFanout would skip it (because !IsEstablished()),
		// but the registry entry would stay around.
		if w.sub.Stream != nil {
			w.sub.Stream.CancelRead(uint64(moqt.StreamResetExcessiveLoad))
			w.sub.Stream.CancelWrite(uint64(moqt.StreamResetExcessiveLoad))
		}
		return
	}

	if writeFailed {
		// Outbound stream is already cancelled (or never opened).
		return
	}

	if w.out == nil {
		// reopen() failed earlier; nothing to close.
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

// applyPriority pushes the §7.2 effective priority for this writer's
// current outbound stream into the transport.
// [session.OutgoingSubgroupStream.SetSendPriority] forwards to the
// underlying [session.SendStream] iff it implements
// [session.PrioritizedSendStream]; adapters that don't (currently all of
// them — see the interface docs) silently no-op.
//
// applyPriority is called on stream open and on §11.4.3 reopen so the
// priority is always in sync with the latest subscriber + publisher
// values. Future REQUEST_UPDATE handling will call it again when the
// peer changes SUBSCRIBER_PRIORITY.
//
// The publisher-priority byte, Group ID and Subgroup ID all come from the
// inbound SubgroupHeader; the subscriber-priority and group-order halves come
// from the subscription. Together they form the full §7.2 key (rules 1–4).
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
