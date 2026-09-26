package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// fwdObject pairs a SubgroupObject with its absolute Object ID: filtering
// punches holes in the forwarded sequence, so the writer re-encodes the
// §11.4.2 ObjectIDDelta against its own outbound stream.
type fwdObject struct {
	obj        *message.SubgroupObject
	absID      uint64
	enqueuedAt time.Time // stamped in publish; used for the §8 lag window

	// maxCacheAge is the upstream's MAX_CACHE_DURATION (§12.3), zero for no
	// limit: the Object is not forwarded once older than that.
	maxCacheAge time.Duration

	// first marks the subgroup's true first object (§11.4.2 FIRST_OBJECT);
	// only an outbound stream beginning with it sets the bit.
	first bool
}

// subgroupWriterSet is the payload of a [registry.SharedSubgroup]: one
// outbound writer per downstream subscriber for a (GroupID, SubgroupID),
// shared by every inbound stream contributing that Subgroup. All access holds
// [registry.SharedSubgroup.Mu].
//
// A nil writer records a sub that was not Established when scanned, so it is
// not retried.
type subgroupWriterSet struct {
	writers map[*registry.DownstreamSub]*subgroupWriter
	// hdr is the first contributor's SUBGROUP_HEADER, reused for every writer;
	// TrackAlias is overwritten per subscriber.
	hdr message.SubgroupHeader
	// gen is the downstream generation at the last joiner scan; the scan is
	// skipped while it is unchanged.
	gen uint64

	// sawClean records that some contributor ended cleanly, so the merged
	// stream FINs even if a peer reset; resetCode is used only when every
	// contributor reset.
	sawClean  bool
	resetCode moqt.StreamResetCode
}

// resolveImplicitSubgroupID handles §11.4.2 SUBGROUP_ID_MODE 0b01 (Subgroup ID
// = first Object ID): it reads the first object and rewrites hdr to the
// explicit form. The returned pending object must be processed as the
// stream's first. Other modes return (nil, true).
//
// ok=false means the stream ended first; the caller just returns. A malformed
// first Object ends the track (§2.4.2).
func (h *sessionHandler) resolveImplicitSubgroupID(
	ctx context.Context,
	entry *registry.TrackEntry,
	stream *session.IncomingSubgroupStream,
	hdr *message.SubgroupHeader,
) (pending *message.SubgroupObject, ok bool) {
	if hdr.SubgroupIDMode != message.SubgroupIDImplicitFirstObject {
		return nil, true
	}
	if hdr.ReplayingSubgroup {
		// On a replay the first object need not be the subgroup's first,
		// so the implied ID is only as reliable as the sender.
		h.log.LogAttrs(ctx, slog.LevelDebug,
			"fanout: implicit-first-object Subgroup ID on a replay stream",
			slog.Uint64("group", hdr.GroupID))
	}
	obj, err := stream.ReadObject()
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
		case errors.Is(err, session.ErrMalformedTrack):
			stream.Cancel(moqt.StreamResetMalformedTrack)
			h.endMalformedTrack(ctx, entry, h.sess, err)
		default:
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"fanout: inbound stream ended before first-object Subgroup ID resolved",
				slog.String("err", err.Error()))
			// Stop a publisher still writing into a stream nobody reads.
			stream.Cancel(moqt.StreamResetInternalError)
		}
		return nil, false
	}
	hdr.SubgroupID = obj.ObjectIDDelta // first object: the delta IS the absolute ID
	hdr.SubgroupIDMode = message.SubgroupIDExplicit
	return obj, true
}

// A subgroup stream can arrive before the SUBSCRIBE_OK binding its Track Alias
// (§11.1). §11.4.2 lets the receiver "abandon the stream, or choose to buffer
// it for a brief period"; the relay leaves it unread for up to earlyAliasWait,
// then abandons it.
//
// The deadline also breaks a flow-control deadlock: the bundled transports do
// not reserve connection credit for control streams (§11.4.2), so unread
// early data can hold back the SUBSCRIBE_OK itself. maxEarlyStreams caps the
// waiting streams per session; past it they are abandoned at once.
const (
	earlyAliasWait  = time.Second
	maxEarlyStreams = 32
)

// testHookEarlyStreamWaiting, when set, runs as a subgroup stream starts
// waiting for its Track Alias, so a test can send the SUBSCRIBE_OK only once
// the stream is known to have arrived first.
var testHookEarlyStreamWaiting atomic.Pointer[func(alias uint64)]

// resolveInboundTrack returns what stream's Track Alias is bound to, waiting
// within the bounds above. An unresolved alias abandons the stream and reports
// false: EXCESSIVE_LOAD (§3.3.4) past maxEarlyStreams, else INTERNAL_ERROR.
func (h *sessionHandler) resolveInboundTrack(
	ctx context.Context,
	stream *session.IncomingSubgroupStream,
) (session.InboundTrack, bool) {
	if in, ok := stream.InboundTrack(); ok {
		return in, true
	}
	defer h.earlyStreams.Add(-1)
	if h.earlyStreams.Add(1) > maxEarlyStreams {
		h.log.LogAttrs(ctx, slog.LevelWarn, "fanout: too many streams waiting for their Track Alias",
			slog.Uint64("alias", stream.Header.TrackAlias))
		stream.Cancel(moqt.StreamResetExcessiveLoad)
		return session.InboundTrack{}, false
	}
	waitCtx, cancel := context.WithTimeout(ctx, earlyAliasWait)
	defer cancel()
	if hook := testHookEarlyStreamWaiting.Load(); hook != nil {
		(*hook)(stream.Header.TrackAlias)
	}
	in, ok := stream.AwaitInboundTrack(waitCtx)
	if !ok {
		h.log.LogAttrs(ctx, slog.LevelWarn, "fanout: Track Alias still unknown, abandoning stream",
			slog.Uint64("alias", stream.Header.TrackAlias))
		stream.Cancel(moqt.StreamResetInternalError)
	}
	return in, ok
}

// runFanout forwards one inbound subgroup stream to every downstream
// subscriber, remapping the Track Alias per subscriber.
//
// §9.3: inbound streams carrying the same (GroupID, SubgroupID) share one
// outbound writer per subscriber (§2.2: a Subgroup is not split across
// streams), and [registry.TrackEntry.ClaimDelivered] drops duplicate objects
// (§2.1). Each writer is a [subgroupWriter] goroutine behind a bounded queue.
// The inbound FIN-vs-reset reaches the outbound streams only when the last
// contributor leaves.
func (h *sessionHandler) runFanout(ctx context.Context, stream *session.IncomingSubgroupStream) {
	hdr := stream.Header

	in, ok := h.resolveInboundTrack(ctx, stream)
	if !ok {
		return // abandoned: the stream is reset, the session stays up
	}
	key := in.Key

	entry, ok := h.tracks.Get(key)
	if !ok {
		// The subscription ended after the alias was registered.
		h.log.LogAttrs(ctx, slog.LevelDebug, "fanout: track entry gone, dropping stream",
			slog.Uint64("alias", hdr.TrackAlias))
		stream.Cancel(moqt.StreamResetInternalError)
		return
	}

	// §11.4.2: a header without a Priority byte inherits the alias's
	// DEFAULT_PUBLISHER_PRIORITY (§12.4). Resolve it once so the cache, FETCH,
	// PRIORITY_FILTER and §7.2 scheduling see it; the outbound header still
	// omits the byte (see openWriterForSub for the exception).
	if !hdr.InlinePriority {
		hdr.PublisherPriority = in.DefaultPublisherPriority
	}

	// One TrackRef per stream: it allocates, and is reported per object.
	ref := h.trackRef(entry.FullName)

	// §12.3: a MAX_CACHE_DURATION of 0 limits only serving from the cache.
	var liveMaxAge time.Duration
	if in.HasMaxCacheDuration {
		liveMaxAge = in.MaxCacheDuration
	}

	// Everything below keys on hdr.SubgroupID, so resolve it first.
	pending, ok := h.resolveImplicitSubgroupID(ctx, entry, stream, &hdr)
	if !ok {
		return
	}

	sgKey := registry.SubgroupKey{Group: hdr.GroupID, Subgroup: hdr.SubgroupID}
	sg, created := entry.AcquireSubgroup(sgKey, func() any {
		return &subgroupWriterSet{
			writers: make(map[*registry.DownstreamSub]*subgroupWriter),
			hdr:     hdr,
		}
	})
	set, _ := sg.Set.(*subgroupWriterSet)

	if created {
		// Under sg.Mu so a concurrent contributor's joiner scan can't
		// double-open. The stream is drained even with no subscribers (§9.7).
		initialSubs, gen := entry.CopyDownstreamWithGen()
		pubTimeouts := entry.DeliveryTimeouts()
		sg.Mu.Lock()
		set.gen = gen
		for _, sub := range initialSubs {
			h.openWriterForSub(ctx, set.hdr, sub, set.writers, pubTimeouts, ref)
		}
		sg.Mu.Unlock()
	}

	// This contributor's termination, applied outbound only if it is the last
	// to leave the Subgroup (§9.3).
	var (
		inboundReset     bool
		inboundResetCode = moqt.StreamResetCancelled
	)
	defer func() {
		// Record the outcome before releasing, so the last contributor decides
		// FIN vs reset over all of them.
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
		reset := !set.sawClean
		code := set.resetCode
		ws := make([]*subgroupWriter, 0, len(set.writers))
		for _, w := range set.writers {
			if w == nil {
				continue
			}
			wReset, wCode := reset, code
			// §11.4.3: a group now outside the subscription's range is
			// reset, not FIN'd.
			if !wReset && registry.GroupOutOfRange(hdr.GroupID, w.sub.GetFilter()) {
				wReset, wCode = true, moqt.StreamResetCancelled
			}
			w.close(wReset, wCode)
			ws = append(ws, w)
		}
		sg.Mu.Unlock()
		joinWriters(ws)
	}()

	var (
		firstObj = true
		// terminalSeen: an EndOfGroup/EndOfTrack was read on this inbound
		// stream; any later object makes the track malformed (§11.4.3,
		// §2.4.2).
		terminalSeen bool
	)

	for {
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
				// The session is going away: reset, never FIN.
				inboundReset = true
				return
			}
			if errors.Is(err, session.ErrMalformedTrack) {
				stream.Cancel(moqt.StreamResetMalformedTrack)
				inboundReset = true
				inboundResetCode = moqt.StreamResetMalformedTrack
				h.endMalformedTrack(ctx, entry, h.sess, err)
				return
			}
			h.log.LogAttrs(ctx, slog.LevelDebug, "fanout: inbound ReadObject failed",
				slog.String("err", err.Error()))
			// An unparseable object leaves the publisher writing; stop it.
			stream.Cancel(moqt.StreamResetInternalError)
			inboundReset = true
			return
		}

		if terminalSeen {
			h.log.LogAttrs(ctx, slog.LevelDebug,
				"fanout: object after EndOfGroup/EndOfTrack — malformed track",
				slog.Uint64("group", hdr.GroupID), slog.Uint64("subgroup", hdr.SubgroupID))
			stream.Cancel(moqt.StreamResetMalformedTrack)
			inboundReset = true
			inboundResetCode = moqt.StreamResetMalformedTrack
			h.endMalformedTrack(ctx, entry, h.sess,
				fmt.Errorf("object after END_OF_GROUP / END_OF_TRACK in Group %d", hdr.GroupID))
			return
		}

		isTrueFirst := firstObj && !hdr.ReplayingSubgroup
		firstObj = false
		objectID := stream.ObjectID() // resolved by ReadObject (§11.4.2)

		// Tracked whether or not this copy wins the dedup claim below.
		terminal := obj.IsTerminal()

		// §2.1: the first upstream to deliver {GroupID, ObjectID} forwards it.
		// Outside sg.Mu, so dedup losers never touch the writer set.
		if !entry.ClaimDelivered(hdr.GroupID, objectID) {
			if terminal {
				terminalSeen = true
			}
			continue // redundant copy already forwarded by a peer upstream.
		}

		// Counted after the dedup claim, so redundant copies don't count.
		h.metrics.ObjectReceived(ref, hdr.SubgroupID)

		// Under sg.Mu: joiner detection, writer open and publish are atomic
		// against other contributors and the last-contributor teardown.
		sg.Mu.Lock()

		// Cache before bumping LARGEST_OBJECT, so a FETCH that snapshots the
		// new watermark finds the object cached.
		entry.Cache.Put(&cache.CachedObject{
			GroupID:           hdr.GroupID,
			ObjectID:          objectID,
			SubgroupID:        hdr.SubgroupID,
			PublisherPriority: hdr.PublisherPriority,
			ForwardingPref:    cache.ForwardingSubgroup,
			Status:            obj.ObjectStatus,
			Properties:        obj.Properties,
			Payload:           obj.Payload,

			MaxCacheDuration:    in.MaxCacheDuration,
			HasMaxCacheDuration: in.HasMaxCacheDuration,
		})

		// Serialised with AddDownstreamSnapshotLargest: a new sub either saw
		// the old Largest and appears in newSubs (delivered live), or saw the
		// new one (its fill fetch stream covers this object).
		loc := message.Location{Group: hdr.GroupID, Object: objectID}
		var newSubs []*registry.DownstreamSub
		newSubs, set.gen = entry.UpdateLargestAndDetectNew(loc,
			func(s *registry.DownstreamSub) bool { _, ok := set.writers[s]; return ok }, set.gen)
		for _, sub := range newSubs {
			h.openWriterForSub(ctx, set.hdr, sub, set.writers, entry.DeliveryTimeouts(), ref)
		}

		// §5.1.2 filters run before enqueue, so a miss takes no queue slot.
		for _, w := range set.writers {
			if w == nil {
				continue
			}
			if w.admit(hdr, objectID, obj.Properties) {
				w.publish(fwdObject{obj: obj, absID: objectID, first: isTrueFirst, maxCacheAge: liveMaxAge})
			}
		}
		sg.Mu.Unlock()

		if terminal {
			terminalSeen = true
		}
	}
}

// openWriterForSub starts a subgroupWriter for sub and records it in writers
// (nil when sub is not Established, so it is not retried).
//
// No transport I/O here: callers hold sg.Mu, and a header write blocked on
// one subscriber's flow control would stall the whole subgroup. The writer
// opens its stream lazily, before the first object it forwards.
func (h *sessionHandler) openWriterForSub(
	ctx context.Context,
	hdr message.SubgroupHeader,
	sub *registry.DownstreamSub,
	writers map[*registry.DownstreamSub]*subgroupWriter,
	pubTimeouts message.DeliveryTimeouts,
	ref TrackRef,
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
	// A subscriber without Track Properties (§10.2.21) cannot inherit
	// DEFAULT_PUBLISHER_PRIORITY (§12.4), so the priority is written out.
	if !sub.IncludesProperties() {
		subHdr.InlinePriority = true
	}
	// cancelIO unblocks a writer wedged on a subscriber that stopped
	// reading (see [joinWriters]).
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
		ref:                 ref,
		maxDropsBeforeReset: h.maxDropsBeforeReset,
		maxLag:              h.maxFanoutLag,
		// §8: kept apart, since the §12.1/§12.2 first-object override
		// applies to the publisher's half alone.
		pubTimeouts: pubTimeouts,
		subTimeouts: sub.GetDeliveryTimeouts(),
	}
	writers[sub] = w
	h.spawn(w.run)
}

// subgroupWriter is the per-subscriber writer goroutine: it drains an inbox
// onto outbound subgroup streams on the subscriber's session.
//
//   - A gap in Object IDs resets the stream and opens a fresh one (§11.4.3).
//   - A clean inbound EOF FINs the stream; an inbound error resets it.
//   - A full inbox drops the object. An object that waited longer than
//     maxLag resets with TOO_FAR_BEHIND and terminates the subscription
//     (§3.3.4); the optional maxDropsBeforeReset cap does so with
//     EXCESSIVE_LOAD.
//   - An elapsed §8 delivery timeout resets only that stream with
//     DELIVERY_TIMEOUT; the subscription survives (§3.3.4).
type subgroupWriter struct {
	sub *registry.DownstreamSub
	// ctx bounds every blocking stream operation; cancelIO resets the
	// in-flight stream, unwedging a writer blocked on a stalled subscriber.
	ctx      context.Context
	cancelIO context.CancelFunc
	hdr      message.SubgroupHeader          // template; TrackAlias already remapped
	out      *session.OutgoingSubgroupStream // nil until run opens it lazily
	unbridge func() bool                     // stops the current stream's ctx→Cancel bridge
	inbox    chan fwdObject
	done     chan struct{}
	log      *slog.Logger
	metrics  Metrics
	// ref labels every Metrics call; built once, since it allocates.
	ref                 TrackRef
	maxDropsBeforeReset int
	maxLag              time.Duration
	// pubTimeouts and subTimeouts are the §8 delivery-timeout halves, resolved
	// per outbound stream; zero disables a dimension.
	pubTimeouts message.DeliveryTimeouts
	subTimeouts message.DeliveryTimeouts

	closeOnce        sync.Once
	dropsMu          sync.Mutex
	drops            int
	closed           bool                 // set under dropsMu inside close
	inboundReset     bool                 // set under dropsMu inside close
	inboundResetCode moqt.StreamResetCode // §3.3.4 reset code when inboundReset; set inside close
	// incomplete records that this subscription skipped an Object after its
	// Start Location (filter, Forward State 0, overflow or expiry), so its
	// streams end with a reset, not a FIN (§11.4.3), with incompleteCode:
	// EXCESSIVE_LOAD after any overflow, else CANCELLED. Set under dropsMu.
	//
	// Interpretation: Objects published before the subscription joined count
	// as before its Start Location, so a joiner's stream may still FIN.
	incomplete     bool
	incompleteCode moqt.StreamResetCode
	// lastAdmitted is the last Object ID admit let through: a SkipBeforeStart
	// above it means the Start was raised past sent Objects; one below is a
	// straggler from another upstream. Only touched by admit, under sg.Mu.
	lastAdmitted uint64
	hasAdmitted  bool
}

// admit decides whether w takes the Object at objectID of the subgroup hdr
// names, closing w when it will take none again.
func (w *subgroupWriter) admit(hdr message.SubgroupHeader, objectID uint64, props []byte) bool {
	switch w.sub.ForwardDecision(hdr.GroupID, objectID, hdr.SubgroupID, hdr.PublisherPriority, props) {
	case registry.Forward:
		w.lastAdmitted, w.hasAdmitted = objectID, true
		return true
	case registry.SkipObject, registry.SkipPaused:
		// The stream stays open for later Objects, but the Subgroup is now
		// incomplete (§11.4.3).
		w.markIncomplete(moqt.StreamResetCancelled)
	case registry.SkipGroup, registry.SkipEnded:
		// No further Object will pass: reset once the queue is written
		// (§11.4.3), so a PUBLISH_DONE waiting on the streams can follow.
		w.close(true, moqt.StreamResetCancelled)
	case registry.SkipBeforeStart:
		// §11.4.3 allows a FIN after omitting these, unless a REQUEST_UPDATE
		// raised the Start past an admitted Object.
		if w.hasAdmitted && objectID > w.lastAdmitted {
			w.markIncomplete(moqt.StreamResetCancelled)
		}
	}
	return false
}

// markIncomplete sets subgroupWriter.incomplete; the first code recorded
// stands, except that EXCESSIVE_LOAD overrides.
func (w *subgroupWriter) markIncomplete(code moqt.StreamResetCode) {
	w.dropsMu.Lock()
	w.markIncompleteLocked(code)
	w.dropsMu.Unlock()
}

func (w *subgroupWriter) markIncompleteLocked(code moqt.StreamResetCode) {
	// The subscriber expects its own filter's omissions, not the relay's
	// load (§3.3.4).
	if !w.incomplete || code == moqt.StreamResetExcessiveLoad {
		w.incomplete = true
		w.incompleteCode = code
	}
}

// resetCode is incompleteCode when incomplete, else CANCELLED.
func (w *subgroupWriter) resetCode() moqt.StreamResetCode {
	w.dropsMu.Lock()
	defer w.dropsMu.Unlock()
	if w.incomplete {
		return w.incompleteCode
	}
	return moqt.StreamResetCancelled
}

// publish enqueues fwd without blocking, stamping its enqueue time for the
// lag check. On overflow the object is dropped, and past maxDropsBeforeReset
// the writer is closed in reset mode. It is a no-op after close.
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
		w.metrics.ObjectForwarded(w.ref, w.hdr.SubgroupID)
	default:
		w.metrics.ObjectDropped(w.ref, w.hdr.SubgroupID)
		w.dropsMu.Lock()
		w.drops++
		w.markIncompleteLocked(moqt.StreamResetExcessiveLoad)
		drops := w.drops
		capped := w.maxDropsBeforeReset > 0 && w.drops > w.maxDropsBeforeReset
		w.dropsMu.Unlock()

		w.log.Debug("fanout: dropped object on full inbox",
			"sub_id", w.sub.ID, "drops", drops)
		if capped {
			w.log.Warn("fanout: subscriber hit MaxDropsBeforeReset cap, terminating",
				"sub_id", w.sub.ID, "drops", drops)
			w.close(true, moqt.StreamResetExcessiveLoad)
		}
	}
}

// lagging reports whether fwd waited in the queue longer than the §8 lag
// window allows.
func (w *subgroupWriter) lagging(fwd fwdObject) bool {
	return w.maxLag > 0 && time.Since(fwd.enqueuedAt) > w.maxLag
}

// expired reports whether fwd is older than its MAX_CACHE_DURATION (§12.3).
// Deviation: the age runs from when the relay finished reading the Object,
// not from "the beginning of the Object".
func expired(fwd fwdObject) bool {
	return fwd.maxCacheAge > 0 && time.Since(fwd.enqueuedAt) > fwd.maxCacheAge
}

// dropExpired handles an Object skipped by [subgroupWriter.expired]. A
// header-only stream may claim FIRST_OBJECT for it, so it is reset and the
// next Object opens a replay stream (§11.4.2).
func (w *subgroupWriter) dropExpired(hasWritten bool) {
	w.markIncomplete(moqt.StreamResetCancelled)
	if hasWritten || w.out == nil {
		return
	}
	if w.unbridge != nil {
		w.unbridge()
		w.unbridge = nil
	}
	w.closeOut(false, moqt.StreamResetCancelled)
}

// closeOut FINs or resets the current outbound stream and reports it closed
// to the subscription, whose PUBLISH_DONE waits on its streams (§10.12).
func (w *subgroupWriter) closeOut(fin bool, code moqt.StreamResetCode) {
	if w.out == nil {
		return
	}
	if fin {
		_ = w.out.Close()
	} else {
		w.out.Cancel(code)
	}
	w.dropOut()
}

// dropOut is closeOut for an outbound stream the session has already reset.
func (w *subgroupWriter) dropOut() {
	if w.out == nil {
		return
	}
	w.out = nil
	w.sub.StreamClosed()
}

// run drains the inbox onto outbound streams until close, then FINs or
// resets the stream from what close recorded. After a write failure it keeps
// draining, so publish never blocks.
func (w *subgroupWriter) run() {
	defer close(w.done)

	var (
		prevID      uint64
		hasWritten  bool
		writeFailed bool
	)

	// reopen resets the current outbound stream (if any) and opens a fresh
	// one, for the lazy first open and after a §11.4.3 gap. first sets the
	// §11.4.2 FIRST_OBJECT bit; otherwise the stream is a replay. All its
	// blocking I/O is bounded by w.ctx.
	reopen := func(first bool) bool {
		if w.unbridge != nil {
			w.unbridge()
			w.unbridge = nil
		}
		w.closeOut(false, w.resetCode())
		hdr := w.hdr
		hdr.ReplayingSubgroup = !first
		if !first && hdr.SubgroupIDMode == message.SubgroupIDImplicitFirstObject {
			// A replay stream's first object would imply the wrong ID.
			hdr.SubgroupIDMode = message.SubgroupIDExplicit
		}
		fresh, err := w.openCounted(hdr)
		if err != nil {
			w.log.Debug("fanout: OpenSubgroup (reopen) failed",
				"sub_id", w.sub.ID, "err", err.Error())
			return false
		}
		// §8: WithDeliveryTimeouts returns a copy; the bridge must cancel it.
		fresh = fresh.WithDeliveryTimeouts(w.pubTimeouts, w.subTimeouts)
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
		// Guard: an unreported stream would hold PUBLISH_DONE forever (§10.12).
		w.closeOut(false, moqt.StreamResetCancelled)
	}()

	// failWrites latches this writer broken and stops contributors
	// enqueueing. Only close, under sg.Mu, closes the inbox.
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
		if w.lagging(fwd) {
			w.log.Warn("fanout: subscriber exceeded MaxFanoutLag, terminating",
				"sub_id", w.sub.ID, "lag", time.Since(fwd.enqueuedAt).String())
			lagExceeded = true
			break
		}

		if writeFailed {
			continue
		}

		// Lazy first open, off sg.Mu (see openWriterForSub).
		if w.out == nil {
			if !reopen(fwd.first) {
				failWrites()
				continue
			}
		}

		// §11.4.3: only "the next Object" may go on an existing stream. Of
		// the draft's three ways to tell, this relay uses only "one greater
		// than the previous Object" (a choice) and reopens on any other gap.
		if hasWritten && fwd.absID != prevID+1 {
			w.metrics.SubgroupStreamReset(w.ref, w.hdr.SubgroupID, ResetCauseGap)
			if !reopen(fwd.first) {
				failWrites()
				continue
			}
		}

		// §12.3: "MUST NOT start forwarding" an expired Object. Checked after
		// the open above, which can block.
		if expired(fwd) {
			w.dropExpired(hasWritten)
			continue
		}

		// Re-encode ObjectIDDelta against this outbound stream (§11.4.2).
		out := *fwd.obj
		if !hasWritten {
			out.ObjectIDDelta = fwd.absID
		} else {
			out.ObjectIDDelta = fwd.absID - prevID - 1
		}
		// §8 measures OBJECT_DELIVERY_TIMEOUT from when the object was
		// received. Deviation: enqueuedAt is stamped after the whole object
		// was read, not at its first byte, so the timeout is lenient.
		if err := w.out.WriteObjectReceivedAt(fwd.enqueuedAt, &out); err != nil {
			// The stream is already reset with DELIVERY_TIMEOUT (§3.3.4);
			// resetting again would overwrite that code.
			if errors.Is(err, session.ErrDeliveryTimeout) {
				w.log.Debug("fanout: delivery timeout, abandoning subgroup stream",
					"sub_id", w.sub.ID, "group", w.hdr.GroupID,
					"subgroup", w.hdr.SubgroupID)
				w.metrics.SubgroupStreamReset(w.ref, w.hdr.SubgroupID, ResetCauseDeliveryTimeout)
				w.dropOut()
				failWrites()
				continue
			}
			w.log.Debug("fanout: WriteObject failed",
				"sub_id", w.sub.ID, "err", err.Error())
			w.metrics.SubgroupStreamReset(w.ref, w.hdr.SubgroupID, ResetCauseWriteError)
			w.closeOut(false, moqt.StreamResetInternalError)
			failWrites()
			continue
		}
		prevID = fwd.absID
		hasWritten = true
		// §11.4.3: a later reset still delivers what was written.
		w.out.MarkReliable()
	}

	w.dropsMu.Lock()
	dropCapped := w.maxDropsBeforeReset > 0 && w.drops > w.maxDropsBeforeReset
	inboundReset := w.inboundReset
	inboundResetCode := w.inboundResetCode
	incomplete, incompleteCode := w.incomplete, w.incompleteCode
	w.dropsMu.Unlock()

	if lagExceeded || dropCapped {
		// Slow reader: reset and terminate the subscription. §3.3.4:
		// TOO_FAR_BEHIND for the lag window, EXCESSIVE_LOAD for the drop cap.
		resetCode := moqt.StreamResetTooFarBehind
		cause := ResetCauseTooFarBehind
		if dropCapped && !lagExceeded {
			resetCode = moqt.StreamResetExcessiveLoad
			cause = ResetCauseExcessiveLoad
		}
		w.metrics.SubscriptionResetSlowReader(w.ref, cause)
		// Refuse further enqueues; only close may close the inbox.
		w.dropsMu.Lock()
		w.closed = true
		w.dropsMu.Unlock()
		w.closeOut(false, resetCode)

		// Cancel the request stream so handleSubscribe unregisters the sub.
		// Only if this writer ended it: otherwise a PUBLISH_DONE may be under
		// way, and the reset could discard it.
		if w.sub.Terminate() && w.sub.Stream != nil {
			w.sub.Stream.CancelRead(uint64(resetCode))
			w.sub.Stream.CancelWrite(uint64(resetCode))
		}
		return
	}

	if writeFailed {
		return
	}

	if w.out == nil {
		return
	}

	if inboundReset {
		// §11.4.3: "A relay might immediately reset the corresponding
		// downstream stream".
		w.metrics.SubgroupStreamReset(w.ref, w.hdr.SubgroupID, ResetCauseInboundReset)
		w.closeOut(false, inboundResetCode)
		return
	}

	if incomplete {
		// §11.4.3: FIN only after "all objects in a Subgroup".
		w.closeOut(false, incompleteCode)
		return
	}

	w.closeOut(true, 0)
}

// openCounted opens a subgroup stream, counting it for the §10.12 Stream
// Count; it fails once the subscription has terminated.
func (w *subgroupWriter) openCounted(hdr message.SubgroupHeader) (*session.OutgoingSubgroupStream, error) {
	if !w.sub.BeginStream() {
		return nil, errSubscriptionTerminated
	}
	out, err := w.sub.Session.OpenSubgroupContext(w.ctx, hdr)
	w.sub.EndStream(err == nil)
	return out, err
}

// applyPriority sets the §7.2 effective priority on the current outbound
// stream. It runs on each (re)open, so a SUBSCRIBER_PRIORITY change applies
// from the next stream.
func (w *subgroupWriter) applyPriority() {
	if w.out == nil {
		return
	}
	w.out.SetSendPriority(w.sub.EffectiveStreamPriority(
		w.hdr.PublisherPriority, w.hdr.GroupID, w.hdr.SubgroupID,
	))
}

// close closes the inbox, recording whether the writer ends its stream with
// a reset (and code) or a FIN. The first call wins. Queued objects are still
// written; a wedged writer is bounded by [joinWriters].
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
// configured.
const defaultWriterJoinTimeout = 5 * time.Second

// joinTimeout is the deadline for [joinWriters]: a healthy writer drains
// within MaxFanoutLag or terminates itself.
func (w *subgroupWriter) joinTimeout() time.Duration {
	if w.maxLag > 0 {
		return w.maxLag
	}
	return defaultWriterJoinTimeout
}

// joinWriters waits for every writer to finish after close. A writer wedged
// in a stream write never dequeues again, so at one shared deadline every
// still-running writer's I/O is cancelled: N stalled subscribers cost one
// timeout, not N.
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
