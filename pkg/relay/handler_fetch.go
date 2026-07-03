package relay

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// defaultUpstreamFetchTimeout bounds an upstream stitch FETCH when the
// downstream supplied no FILL_TIMEOUT. It keeps a fetch-capable upstream that
// nonetheless stalls (or never answers FETCH) from wedging the downstream
// handler: the stitch degrades to cache-only once it elapses.
const defaultUpstreamFetchTimeout = 5 * time.Second

// handleFetch implements FETCH (§9.4, §10.12): validate the requested range,
// reply FETCH_OK, open a FETCH_HEADER uni-stream, and serialise the cached
// objects in the requested group order. Gaps in the response stream are how the
// spec signals "objects do not exist" (§11.4.4, §3553).
//
// The below-floor portion of the range — objects the relay evicted or never
// cached — is stitched from an upstream FETCH when one is reachable; see
// [sessionHandler.stitchedFetchObjects]. Otherwise the relay emits what it has
// and leaves the rest as gaps, which §3553 lets the subscriber read as
// non-existence.
func (h *sessionHandler) handleFetch(ctx context.Context, req *session.Request, msg *message.Fetch) {
	if err := h.auth.AuthorizeFetch(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "Fetch", err)
		return
	}

	switch msg.FetchType {
	case message.FetchTypeStandalone:
		// fall through to standalone handling below
	case message.FetchTypeRelativeJoining, message.FetchTypeAbsoluteJoining:
		h.handleJoiningFetch(ctx, req, msg)
		return
	default:
		_ = req.RejectError(moqt.RequestNotSupported, "relay: unknown FETCH type")
		return
	}
	sf := msg.Standalone
	if sf == nil {
		_ = req.RejectError(moqt.RequestMalformedTrack, "relay: standalone FETCH missing payload")
		return
	}

	fullName := track.FullTrackName{Namespace: sf.Namespace, Name: sf.Name}
	entry, ok := h.tracks.Get(fullName.Key())
	if !ok {
		_ = req.RejectError(moqt.RequestDoesNotExist, "relay: track not known")
		return
	}

	largest, hasLargest := entry.GetLargest()
	if !hasLargest {
		// §3585-3587: "If no Objects have been published for the
		// track or Start Location is greater than the Largest Object
		// the publisher MUST return REQUEST_ERROR with error code
		// INVALID_RANGE."
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: no objects published")
		return
	}

	// §3576: "End Location MUST specify the same or a larger Location
	// than Start Location for Standalone and Absolute Joining Fetches."
	// For wire input message.Fetch.Validate already enforced this at parse
	// time (a violation is a session PROTOCOL_VIOLATION there); this check
	// answers locally-constructed requests with the gentler INVALID_RANGE.
	if message.FetchEndBeforeStart(sf.StartLocation, sf.EndLocation) {
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: end < start")
		return
	}

	// §3585: Start > Largest is INVALID_RANGE.
	if largest.Less(sf.StartLocation) {
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: start beyond largest object")
		return
	}

	order := fetchGroupOrder(msg.Parameters)
	fillTimeout := message.FillTimeoutFromParam(msg.Parameters)

	// The response EndLocation is fixed by the watermark (§3604-3605) and is
	// independent of which objects we end up streaming, so reply FETCH_OK
	// before doing any (possibly slow) upstream stitching.
	endLocation := capFetchEndLocation(sf.EndLocation, largest)
	if err := req.Reply(&message.FetchOK{
		EndLocation:     endLocation,
		TrackProperties: entry.GetProperties(),
	}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "FETCH_OK reply failed",
			slog.String("err", err.Error()))
		return
	}

	h.serveFetchObjects(ctx, req, "standalone", msg.RequestID, entry, fullName,
		sf.StartLocation, sf.EndLocation, order, fillTimeout)
}

// readFetchUpdates is the follow-up dispatch loop for an established FETCH:
// REQUEST_UPDATE (§10.9) routes to [sessionHandler.handleFetchUpdate]; any
// other follow-up is ignored. Scaffolding lives in [readRequestStream].
func (h *sessionHandler) readFetchUpdates(ctx context.Context, req *session.Request, out *session.OutgoingFetchStream) {
	readRequestStream(ctx, req.Stream, func(m message.Message) bool {
		if upd, ok := m.(*message.RequestUpdate); ok {
			// §10.2.2: an update may REGISTER/DELETE token aliases;
			// a cache fault there is session-fatal.
			if !h.handleFollowupTokens(ctx, upd) {
				return false
			}
			h.handleFetchUpdate(ctx, req, out, upd)
		}
		return true
	})
}

// handleFetchUpdate applies a REQUEST_UPDATE (§10.9) to an in-flight FETCH.
// A FETCH response is a finished snapshot by the time the data stream is
// FIN'd, so the relay has no live parameters to mutate — but it must still
// validate the update and answer with the single mandated REQUEST_OK /
// REQUEST_ERROR. Per §10.9, a FETCH whose REQUEST_UPDATE fails differs from
// a SUBSCRIBE: there is no PUBLISH_DONE for a FETCH, so the relay resets the
// FETCH data stream instead.
func (h *sessionHandler) handleFetchUpdate(
	ctx context.Context,
	req *session.Request,
	out *session.OutgoingFetchStream,
	upd *message.RequestUpdate,
) {
	if err := validateFetchUpdateParams(upd.Parameters); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "FETCH REQUEST_UPDATE parameter parse failed",
			slog.String("err", err.Error()))
		_ = req.Reply(&message.RequestError{
			ErrorCode:   moqt.RequestMalformedTrack,
			ErrorReason: err.Error(),
		})
		// §10.9: a failed FETCH update resets the FETCH data stream.
		out.Cancel(moqt.StreamResetInternalError)
		return
	}
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "FETCH REQUEST_UPDATE_OK write failed",
			slog.String("err", err.Error()))
	}
}

// validateFetchUpdateParams checks the parameters of a FETCH REQUEST_UPDATE.
// FETCH does not carry a Forward State (its response is a finished snapshot),
// so the only thing the relay validates here is the GROUP_ORDER enum (§10.2.8)
// — the same protocol-violation check installSubscribeParams applies.
func validateFetchUpdateParams(ps message.Parameters) error {
	if p, ok := ps.Find(message.ParamGroupOrder); ok {
		switch message.GroupOrder(p.Byte) {
		case message.GroupOrderAscending, message.GroupOrderDescending:
		default:
			return fmt.Errorf("invalid GROUP_ORDER value 0x%X (§10.2.8)", p.Byte)
		}
	}
	return nil
}

// joiningFetchWaitTimeout bounds how long a Joining FETCH waits for its
// referenced SUBSCRIBE to finish establishing (§10.12.2 buffering). A
// pipelined SUBSCRIBE + Joining FETCH can put the FETCH first, or the
// SUBSCRIBE may still be waiting on an upstream round trip.
const joiningFetchWaitTimeout = 5 * time.Second

// waitJoiningLocation returns the joining location registered for the given
// SUBSCRIBE Request ID, waiting up to [joiningFetchWaitTimeout] for it to
// appear — §10.12.2: a Joining Fetch referencing a subscription "that has
// not yet been established" is buffered "until either the Subscription is
// established or the request times out". Handlers run on their own
// goroutines, so blocking here holds up only this FETCH.
func (h *sessionHandler) waitJoiningLocation(
	ctx context.Context,
	joiningRequestID uint64,
) (joiningLocation, bool) {
	deadline := time.NewTimer(joiningFetchWaitTimeout)
	defer deadline.Stop()
	for {
		h.joinLocMu.RLock()
		jloc, ok := h.joinLocs[joiningRequestID]
		notify := h.joinLocNotify
		h.joinLocMu.RUnlock()
		if ok {
			return jloc, true
		}
		select {
		case <-notify:
			// A SUBSCRIBE registered; re-check whether it was ours.
		case <-deadline.C:
			return joiningLocation{}, false
		case <-ctx.Done():
			return joiningLocation{}, false
		}
	}
}

// handleJoiningFetch implements the Relative / Absolute Joining FETCH
// flows (§10.12.2). A Joining FETCH references an active SUBSCRIBE by
// Request ID; the relay recovers the associated Joining Location from
// [sessionHandler.joinLocs] (populated in handleSubscribe at SUBSCRIBE_OK
// time) and uses §10.12.2.1's rules to compute the response range:
//
//   - End Location = {Joining Location.Group, Joining Location.Object + 1}
//   - Absolute:  Start = {jf.JoiningStart, 0}
//   - Relative:  Start = {Joining Location.Group - jf.JoiningStart, 0}
//
// The relay then serves the range from the per-track Object Cache,
// reusing the standalone-FETCH stream writer ([streamFetchObjects]).
// Gaps in the cache surface as gaps in the response stream — same
// "evicted vs. never existed" caveat as standalone FETCH.
func (h *sessionHandler) handleJoiningFetch(ctx context.Context, req *session.Request, msg *message.Fetch) {
	jf := msg.Joining
	if jf == nil {
		_ = req.RejectError(moqt.RequestMalformedTrack, "relay: joining FETCH missing payload")
		return
	}

	jloc, ok := h.waitJoiningLocation(ctx, jf.JoiningRequestID)
	if !ok {
		// §10.12.2: "If a publisher receives a Joining Fetch with a
		// Request ID that does not correspond to a subscription in
		// the same session [...] it MUST return a REQUEST_ERROR
		// with error code INVALID_JOINING_REQUEST_ID."
		_ = req.RejectError(moqt.RequestInvalidJoiningID, "relay: no subscription for joining request ID")
		return
	}
	if !jloc.hasLargest {
		// §10.12.2: "If no Objects have been published for the
		// track the publisher MUST respond with a REQUEST_ERROR
		// with error code INVALID_RANGE."
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: no objects published at subscribe time")
		return
	}

	// §10.12.2.1: compute Start/End from the snapshot.
	var startLoc message.Location
	switch msg.FetchType {
	case message.FetchTypeAbsoluteJoining:
		startLoc = message.Location{Group: jf.JoiningStart, Object: 0}
	case message.FetchTypeRelativeJoining:
		if jf.JoiningStart > jloc.largest.Group {
			_ = req.RejectError(moqt.RequestInvalidRange, "relay: relative joining start exceeds largest group")
			return
		}
		startLoc = message.Location{Group: jloc.largest.Group - jf.JoiningStart, Object: 0}
	case message.FetchTypeStandalone:
		// Unreachable: standalone fetches do not use the joining-snapshot
		// path — this switch only runs for the two joining FetchTypes.
	}

	// End = {Joining Location.Group, Joining Location.Object + 1}. The
	// +1 might overflow when Object == MaxUint64; in practice this is
	// astronomically unlikely, but guard by falling back to the §10.12.1
	// "entire group" form {Group, 0}, whose inclusive end {Group, Max} is
	// exactly the joining location.
	endLoc := message.Location{Group: jloc.largest.Group, Object: jloc.largest.Object + 1}
	if jloc.largest.Object == math.MaxUint64 {
		endLoc = message.Location{Group: jloc.largest.Group, Object: 0}
	}

	if endLoc.Less(startLoc) {
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: joining start beyond largest object")
		return
	}

	entry, ok := h.tracks.Get(jloc.fullName.Key())
	if !ok {
		// The subscription's track entry was evicted between
		// SUBSCRIBE_OK and this FETCH — race with the last
		// upstream publisher disconnecting. The downstream
		// subscriber will see PUBLISH_DONE shortly; for this
		// FETCH we have nothing to serve.
		_ = req.RejectError(moqt.RequestDoesNotExist, "relay: track entry gone")
		return
	}

	order := fetchGroupOrder(msg.Parameters)

	if err := req.Reply(&message.FetchOK{
		EndLocation:     endLoc,
		TrackProperties: entry.GetProperties(),
	}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "joining FETCH_OK reply failed",
			slog.String("err", err.Error()))
		return
	}

	h.serveFetchObjects(ctx, req, "joining", msg.RequestID, entry, jloc.fullName,
		startLoc, endLoc, order, 0)
}

// fetchGroupOrder pulls the GROUP_ORDER parameter (§10.2.8) out of a
// FETCH's Parameters list. Defaults to ascending when omitted; the
// FETCH responder uses this to choose between ascending and descending
// traversal through the cache.
func fetchGroupOrder(ps message.Parameters) message.GroupOrder {
	p, ok := ps.Find(message.ParamGroupOrder)
	if !ok {
		return message.GroupOrderAscending
	}
	g := message.GroupOrder(p.Byte)
	if g == message.GroupOrderDescending {
		return g
	}
	return message.GroupOrderAscending
}

// inclusiveFetchEnd converts the protocol's exclusive-style EndLocation
// (§3622-3624: "the last Object, plus 1; or 0 to indicate the entire
// Group") into the cache's inclusive-end convention.
//
//   - EndLocation = {G, 0}        → inclusive end {G, MaxUint64}
//     (the entire group G).
//   - EndLocation = {G, N} (N>0)  → inclusive end {G, N-1}.
func inclusiveFetchEnd(end message.Location) message.Location {
	if end.Object == 0 {
		return message.Location{Group: end.Group, Object: math.MaxUint64}
	}
	return message.Location{Group: end.Group, Object: end.Object - 1}
}

// capFetchEndLocation implements §3628-3632: if the requested end is
// beyond {Largest.Group, Largest.Object + 1} we cap the response's
// EndLocation to that watermark+1. Otherwise echo the request's value.
//
// The comparison runs in inclusive space via [inclusiveFetchEnd]: the wire
// form "End Object 0 = the entire group" (§10.12.1) is numerically tiny but
// semantically huge, so comparing the raw exclusive form would fail to cap
// a whole-group request that extends past the watermark.
func capFetchEndLocation(requested, largest message.Location) message.Location {
	capped := message.Location{Group: largest.Group, Object: largest.Object + 1}
	// largest+1 might overflow when largest.Object == MaxUint64; in
	// practice that's astronomically unlikely, but the correct wire form
	// for "through the end of largest.Group" is precisely the §10.12.1
	// whole-group encoding {largest.Group, 0}.
	if largest.Object == math.MaxUint64 {
		capped = message.Location{Group: largest.Group, Object: 0}
	}
	if largest.Less(inclusiveFetchEnd(requested)) {
		return capped
	}
	return requested
}

// stitchedFetchObjects answers a FETCH range from the relay's cache, filling
// the below-floor portion the relay does not hold from an upstream FETCH when
// one is reachable (§9.4 upstream stitching).
//
// Everything below the cache's eviction floor (see
// [cache.ObjectCache.OldestRetained]) was evicted or never cached, so a gap
// there might still exist upstream whereas a gap at/above the floor is
// ground-truth non-existence. The handler splits the request at the floor,
// fetches [requestStart, floor) from an established upstream, and concatenates
// it with the cached part — the two are disjoint by Location, so the result is
// correctly ordered. With no FETCH-able upstream (or on error/timeout) it
// degrades to serving what the cache has. Upstream-fetched objects are NOT
// cached back: the FIFO ring is keyed by arrival, so old backfill would evict
// live objects.
func (h *sessionHandler) stitchedFetchObjects(
	ctx context.Context,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	requestStart message.Location,
	requestWireEnd message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
) []*cache.CachedObject {
	requestEndIncl := inclusiveFetchEnd(requestWireEnd)
	cacheObjs := entry.Cache.GetRange(requestStart, requestEndIncl, order)

	// Determine the inclusive upper bound of the below-floor sub-range the
	// relay cannot answer from cache.
	upEndIncl := requestEndIncl
	if floor, hasFloor := entry.Cache.OldestRetained(); hasFloor {
		pred, ok := fetchPredecessor(floor)
		if !ok {
			return cacheObjs // floor == {0,0}: nothing exists below it
		}
		if pred.Less(upEndIncl) {
			upEndIncl = pred
		}
	}
	if upEndIncl.Less(requestStart) {
		return cacheObjs // the request starts at/above the floor — no gap
	}

	up := h.pickFetchUpstream(entry)
	if up == nil {
		return cacheObjs // no reachable upstream — gaps stay gaps (§3553)
	}

	upstreamObjs := h.fetchUpstreamRange(
		ctx, up, fullName, requestStart, exclusiveFetchEnd(upEndIncl), order, fillTimeout,
	)
	if len(upstreamObjs) == 0 {
		return cacheObjs
	}
	return mergeFetchObjects(order, upstreamObjs, cacheObjs)
}

// pickFetchUpstream returns an Established, fetch-capable upstream on a
// different session the relay can issue a stitch FETCH to, or nil.
//
// Only upstreams the relay reached via an on-demand SUBSCRIBE (a relay/origin,
// marked FetchCapable in subscribeUpstream) are eligible: a directly-connected
// leaf publisher pushes live objects and is not expected to answer FETCH, so
// stitching to it would only stall. Skipping the requester's own session
// avoids a self-loop (mirrors subscribeUpstream's guard).
func (h *sessionHandler) pickFetchUpstream(entry *registry.TrackEntry) *registry.UpstreamSub {
	for _, u := range entry.CopyUpstream() {
		if u.FetchCapable && u.IsEstablished() && u.Session != nil && u.Session != h.sess {
			return u
		}
	}
	return nil
}

// fetchUpstreamRange issues a standalone FETCH for [start, wireEnd] on the
// upstream's session, awaits the response stream via the relay's fetch router,
// and returns the decoded objects (absence markers skipped). Any error, or a
// timeout bounded by fillTimeout, yields nil so the caller degrades to
// cache-only. The objects are returned in the requested group order because
// the upstream FETCH carries the same GROUP_ORDER parameter.
func (h *sessionHandler) fetchUpstreamRange(
	ctx context.Context,
	up *registry.UpstreamSub,
	fullName track.FullTrackName,
	start, wireEnd message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
) []*cache.CachedObject {
	params := message.Parameters{}
	if order == message.GroupOrderDescending {
		params = append(params, message.GroupOrderParam(message.GroupOrderDescending))
	}
	fmsg := &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     fullName.Namespace,
			Name:          fullName.Name,
			StartLocation: start,
			EndLocation:   wireEnd,
		},
		Parameters: params,
	}

	// Bound the upstream round-trip so a silent or non-FETCH-answering
	// upstream degrades to cache-only instead of wedging the downstream
	// handler. FILL_TIMEOUT, when present, is the subscriber's explicit
	// budget; otherwise fall back to a default.
	budget := fillTimeout
	if budget <= 0 {
		budget = defaultUpstreamFetchTimeout
	}
	fctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	stream, err := up.Session.Fetch(fctx, fmsg)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH failed",
			slog.String("err", err.Error()))
		return nil
	}
	defer stream.Close()

	// The upstream echoes our Request ID in the response's FETCH_HEADER, so
	// the body stream lands on the upstream session's data loop keyed by
	// fmsg.RequestID. Register after Fetch (the ID is only assigned there);
	// the router tolerates a response that races ahead of registration.
	ch, cleanup := h.fetch.Register(up.Session, fmsg.RequestID)
	defer cleanup()

	var fs *session.IncomingFetchStream
	select {
	case fs = <-ch:
	case <-fctx.Done():
		h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH response timed out")
		return nil
	}
	if fs == nil {
		return nil
	}

	var out []*cache.CachedObject
	for {
		obj, err := fs.ReadDecoded()
		if err != nil {
			break // io.EOF on clean FIN; a partial read keeps what arrived
		}
		if obj.EndOfNonExistentRange || obj.EndOfUnknownRange {
			continue // §11.4.4.2 absence markers carry no payload
		}
		// The §11.4.4.1 Datagram bit carries the original wire shape
		// across this relay hop, so the object is re-emitted downstream
		// with the same forwarding preference it was published with.
		// (Stitched objects are merged into the response only — they are
		// not written back into the cache.)
		pref := cache.ForwardingSubgroup
		if obj.Datagram {
			pref = cache.ForwardingDatagram
		}
		out = append(out, &cache.CachedObject{
			GroupID:           obj.GroupID,
			ObjectID:          obj.ObjectID,
			SubgroupID:        obj.SubgroupID,
			PublisherPriority: obj.PublisherPriority,
			ForwardingPref:    pref,
			Properties:        obj.Properties,
			Payload:           obj.Payload,
		})
	}
	return out
}

// mergeFetchObjects concatenates the below-floor (upstream) and at/above-floor
// (cache) slices in group order. The two are disjoint by Location and each is
// already sorted in order, so for ascending the lower range leads and for
// descending the higher (cache) range leads.
func mergeFetchObjects(order message.GroupOrder, lower, upper []*cache.CachedObject) []*cache.CachedObject {
	switch {
	case len(lower) == 0:
		return upper
	case len(upper) == 0:
		return lower
	}
	out := make([]*cache.CachedObject, 0, len(lower)+len(upper))
	if order == message.GroupOrderDescending {
		out = append(out, upper...)
		out = append(out, lower...)
	} else {
		out = append(out, lower...)
		out = append(out, upper...)
	}
	return out
}

// fetchPredecessor returns the Location immediately below loc in (group,
// object) order, and false when loc is {0, 0} (nothing precedes it). The
// object-underflow case rolls back to the end of the previous group.
func fetchPredecessor(loc message.Location) (message.Location, bool) {
	switch {
	case loc.Object > 0:
		return message.Location{Group: loc.Group, Object: loc.Object - 1}, true
	case loc.Group > 0:
		return message.Location{Group: loc.Group - 1, Object: math.MaxUint64}, true
	default:
		return message.Location{}, false
	}
}

// exclusiveFetchEnd is the inverse of [inclusiveFetchEnd]: it maps an inclusive
// end Location back to the protocol's exclusive wire EndLocation, so a relay
// can request an inclusive [start, incl] range from an upstream FETCH.
func exclusiveFetchEnd(incl message.Location) message.Location {
	if incl.Object == math.MaxUint64 {
		return message.Location{Group: incl.Group, Object: 0} // "entire group" form
	}
	return message.Location{Group: incl.Group, Object: incl.Object + 1}
}

// streamFetchObjects writes the cached objects to the FETCH response
// stream with §11.4.4 delta encoding:
//
//   - The first object includes both GroupIDDelta and ObjectIDDelta
//     flags; the values are absolute (§4460-4464).
//   - Subsequent objects in the same group use ObjectIDDelta only when
//     the gap is > 0 (a consecutive object omits the flag, the
//     subscriber reconstructs ObjectID = prior + 1).
//   - Subsequent objects in a different group set GroupIDDelta:
//     ascending → newGroup - priorGroup - 1, descending →
//     priorGroup - newGroup - 1 (§4466-4473). ObjectIDDelta is then the
//     absolute Object ID in the new group.
//   - Datagram-flavoured objects set bit 0x40 (§4486-4490); subscriber
//     ignores the subgroup bits.
func streamFetchObjects(out *session.OutgoingFetchStream, objs []*cache.CachedObject) (int, error) {
	var (
		written      int
		prevGroup    uint64
		prevObject   uint64
		prevPriority uint8
		havePrev     bool
		// Inferred from the ordering of the first vs second object.
		// Without a second object we don't need the direction.
		descending bool
	)

	for _, o := range objs {
		// §11.2.1.1: the Object Status field "is absent in Objects
		// delivered via a FETCH". Cached status markers describe absence,
		// so they are simply not serialized — the gap in Object IDs
		// conveys it.
		if o.IsStatusMarker() {
			continue
		}

		fo := &message.FetchObject{}

		switch {
		case !havePrev:
			// §4460-4464: first object MUST include both
			// GroupIDDelta and ObjectIDDelta flags; values are
			// absolute.
			fo.SerializationFlags |= message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta
			fo.GroupIDDelta = o.GroupID
			fo.ObjectIDDelta = o.ObjectID

		case o.GroupID != prevGroup:
			// Cross-group. Detect direction from the first such
			// transition: descending iff new group < prior.
			if !descending && o.GroupID < prevGroup {
				descending = true
			} else if descending && o.GroupID > prevGroup {
				// Direction reversed mid-stream — should
				// never happen because GetRange returns
				// stably sorted output, but if it did the
				// safest action is to abandon the optimised
				// delta encoding and reset the GroupIDDelta
				// using ascending convention.
				descending = false
			}
			fo.SerializationFlags |= message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta
			if descending {
				fo.GroupIDDelta = prevGroup - o.GroupID - 1
			} else {
				fo.GroupIDDelta = o.GroupID - prevGroup - 1
			}
			fo.ObjectIDDelta = o.ObjectID

		default:
			// Same group. Omit ObjectIDDelta when consecutive;
			// otherwise include it with the gap value.
			if o.ObjectID != prevObject+1 {
				fo.SerializationFlags |= message.FetchFlagObjectIDDelta
				fo.ObjectIDDelta = o.ObjectID - prevObject - 1
			}
		}

		switch o.ForwardingPref {
		case cache.ForwardingDatagram:
			// §4486-4490: bit 0x40 marks the object as a
			// Datagram-flavoured object; the subscriber ignores
			// the two subgroup bits.
			fo.SerializationFlags |= message.FetchFlagDatagram
		case cache.ForwardingSubgroup:
			// Subgroup: encode the SubgroupID explicitly. The
			// "prior + 0/1" subgroup modes are micro-optimisations
			// over the explicit form; we always emit explicit for
			// simplicity.
			fo.SerializationFlags = (fo.SerializationFlags &^ message.FetchFlagSubgroupIDMode) |
				uint64(message.FetchSubgroupIDExplicit)
			fo.SubgroupID = o.SubgroupID
		}

		// Publisher priority: emit when it differs from the prior
		// object (or on the first object).
		if !havePrev || o.PublisherPriority != prevPriority {
			fo.SerializationFlags |= message.FetchFlagPriority
			fo.PublisherPriority = o.PublisherPriority
		}

		if len(o.Properties) > 0 {
			fo.SerializationFlags |= message.FetchFlagProperties
			fo.Properties = o.Properties
		}

		fo.ObjectPayload = o.Payload

		if err := out.WriteObject(fo); err != nil {
			return written, err
		}
		written++

		prevGroup = o.GroupID
		prevObject = o.ObjectID
		prevPriority = o.PublisherPriority
		havePrev = true
	}

	return written, nil
}
