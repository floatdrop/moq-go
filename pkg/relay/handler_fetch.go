package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"slices"
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

// handleFetch implements FETCH (§9.4, §10.13): validate the requested range,
// reply FETCH_OK, open a FETCH_HEADER uni-stream, and serialise the cached
// objects in the requested group order. Gaps in the response stream are how
// the spec signals "objects do not exist" (§11.4.4).
//
// The below-floor portion of the range — objects the relay evicted or never
// cached — is stitched from an upstream FETCH when one is reachable; see
// [sessionHandler.stitchedFetchObjects]. Whatever no source could vouch for
// is covered by §11.4.4.2 End of Unknown Range markers, so a gap always means
// authoritative non-existence.
// trackKnown reports whether entry stands for a track the relay actually knows
// of. Bare existence does not say so: subscribeUpstreamOnSession creates the
// entry before the upstream round trip that would confirm the track, because
// it must be in place before the §11.1 Track Alias in SUBSCRIBE_OK can route
// (#85). Between those two points the entry describes a track nobody has
// vouched for yet.
//
// The distinction is visible on the wire. Answering a FETCH from such an entry
// falls through to the §10.13 "no Objects have been published" rule and
// returns INVALID_RANGE — "the range you asked for cannot be satisfied" —
// where §10.6 DOES_NOT_EXIST, "the track or namespace is not available at the
// publisher", is the truthful answer. A client deciding whether to retry, and
// with what, needs them kept apart.
//
// A watermark means a publisher has vouched for the track even if the
// subscription that carried it has since gone; a registered subscription means
// one is vouching for it now.
func trackKnown(entry *registry.TrackEntry) bool {
	if _, ok := entry.GetLargest(); ok {
		return true
	}
	// FETCH is not a hot path (see GetRange), so the copies are fine.
	return len(entry.CopyUpstream()) > 0 || len(entry.CopyDownstream()) > 0
}

func (h *sessionHandler) handleFetch(ctx context.Context, req *session.Request, msg *message.Fetch) {
	if err := h.auth.AuthorizeFetch(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "Fetch", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}
	entry, ok := h.tracks.Get(fullName.Key())
	if !ok || !trackKnown(entry) {
		_ = req.RejectError(moqt.RequestDoesNotExist, "relay: track not known")
		return
	}

	largest, hasLargest := entry.GetLargest()
	if !hasLargest {
		// §10.13: "If no Objects have been published for the track or Start
		// Location is greater than the Largest Object the publisher MUST
		// return REQUEST_ERROR with error code INVALID_RANGE."
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: no objects published")
		return
	}

	// draft-20 moved the FETCH range out of the message and into the
	// LOCATION_FILTER parameter (§5.1.2), inclusive at both ends. An absent
	// filter fetches the whole track up to Largest Object.
	filter, err := message.LocationFilterFromParam(msg.Parameters)
	if err != nil {
		_ = req.RejectError(moqt.RequestInvalidFilter, "relay: malformed LOCATION_FILTER")
		return
	}
	if filter == nil {
		filter = &message.LocationFilter{}
	}
	start := filter.Start(largest, hasLargest)

	// §10.13: Start > Largest is INVALID_RANGE.
	if largest.Less(start) {
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: start beyond largest object")
		return
	}

	// A 4-field filter can name an end below its own start (EndGroupDelta 0 with
	// EndObject < StartObject), which §5.1.2 does not itself forbid. Answering it
	// would put us in violation of §10.14 — "If End Location is smaller than the
	// Start Location in the corresponding FETCH the receiver MUST close the
	// session with a PROTOCOL_VIOLATION" — so one malformed FETCH would tear down
	// every other subscription on the session. Reject the request instead.
	if end, ok := filter.End(); ok && end.Less(start) {
		_ = req.RejectError(moqt.RequestInvalidRange, "relay: end before start")
		return
	}

	order := fetchGroupOrder(msg.Parameters)
	fillTimeout := resolveFillBudget(msg.Parameters)
	rangeFilters, ok := h.fetchRangeFilters(ctx, req, msg.Parameters)
	if !ok {
		return
	}

	// The response EndLocation is fixed by the watermark (§10.14) and is
	// independent of which objects we end up streaming, so reply FETCH_OK
	// before doing any (possibly slow) upstream stitching.
	endLocation := capFetchEndLocation(filter, largest)
	if err := req.Reply(&message.FetchOK{
		EndLocation:     endLocation,
		TrackProperties: entry.GetProperties(),
	}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "FETCH_OK reply failed",
			slog.String("err", err.Error()))
		return
	}

	// Serve (and account for) only the range FETCH_OK announced: everything
	// past the capped EndLocation is outside the response by definition, so
	// neither objects nor §11.4.4.2 unknown markers may reference it.
	h.serveFetchObjects(ctx, req, "fetch", msg.RequestID, entry, fullName,
		start, endLocation, order, fillTimeout, rangeFilters)
}

// resolveFillBudget reads FILL_TIMEOUT (§10.2.5) and resolves the "absent"
// case to the local default, so downstream a zero means only what §10.2.5 says
// it means: do not wait for upstream at all.
func resolveFillBudget(ps message.Parameters) time.Duration {
	if d, ok := message.FillTimeoutFromParamOK(ps); ok {
		return d
	}
	return defaultUpstreamFetchTimeout
}

// fetchRangeFilters parses and validates the §5.1.4 Range Filters on a FETCH's
// parameters against the negotiated MAX_FILTER_RANGES. On an invalid or
// over-limit filter it answers REQUEST_ERROR INVALID_FILTER (§10.6) and returns
// ok=false, so the caller aborts before replying FETCH_OK.
func (h *sessionHandler) fetchRangeFilters(
	ctx context.Context, req *session.Request, ps message.Parameters,
) (*message.RangeFilterSet, bool) {
	rf, err := message.RangeFiltersFromParams(ps)
	if err == nil && rf != nil {
		err = rf.Validate(h.sess.MaxFilterRanges())
	}
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "FETCH range filter rejected",
			slog.String("err", err.Error()))
		_ = req.RejectError(moqt.RequestInvalidFilter, err.Error())
		return nil, false
	}
	return rf, true
}

// readFetchUpdates is the follow-up dispatch loop for an established FETCH:
// REQUEST_UPDATE (§10.9) routes to [sessionHandler.handleFetchUpdate]; any
// other follow-up is ignored. Scaffolding lives in [readRequestStream].
func (h *sessionHandler) readFetchUpdates(ctx context.Context, req *session.Request, out *session.OutgoingFetchStream) {
	updates := h.sess.NewRequestUpdateLimiter()
	readRequestStream(ctx, req.Stream, func(m message.Message) bool {
		if upd, ok := m.(*message.RequestUpdate); ok {
			// §10.1: the update consumes a Request ID; a parity or
			// duplicate violation is session-fatal.
			if !h.handleFollowupRequestID(ctx, upd) {
				return false
			}
			// §10.3.1.7: enforce the per-stream MAX_REQUEST_UPDATES limit.
			if !h.handleRequestUpdateLimit(ctx, updates) {
				return false
			}
			// §10.2.2: an update may REGISTER/DELETE token aliases;
			// a cache fault there is session-fatal.
			if !h.handleFollowupTokens(ctx, upd) {
				return false
			}
			h.handleFetchUpdate(ctx, req, out, upd)
			updates.Responded()
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
// so the only thing the relay validates here is the GROUP_ORDER enum (§10.2.8).
//
// TODO(draft-19): §10.2.8 mandates a session-level PROTOCOL_VIOLATION for an
// out-of-range GROUP_ORDER; the SUBSCRIBE / SUBSCRIBE_TRACKS paths were
// promoted to close the session (see [checkGroupOrderParam]), but the FETCH
// paths (this one and the initial standalone/joining FETCH) still scope it to
// a REQUEST_ERROR pending the same promotion.
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

// capFetchEndLocation resolves a FETCH's end from its Location filter and
// caps it at Largest Object per §10.14: "This is the End Location from the
// FETCH request Location Filter parameter unless the requested range extends
// beyond Largest Object at the time the request was processed."
//
// draft-20 made both the request range and FETCH_OK's End Location inclusive
// (§5.1.2), so — unlike draft-19's "last Object plus 1, or 0 for the whole
// group" encoding — no exclusive/inclusive conversion is involved.
func capFetchEndLocation(filter *message.LocationFilter, largest message.Location) message.Location {
	end, ok := filter.End()
	if !ok || largest.Less(end) {
		// §5.1.2: "When they are omitted from a Fetch, the EndGroup and
		// EndObject are Largest Object."
		return largest
	}
	return end
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
// serves what the cache has and covers the below-floor remainder with a
// §11.4.4.2 End of Unknown Range marker, since a plain gap would falsely
// assert non-existence (§11.4.4). Upstream-fetched objects are NOT cached
// back: the FIFO ring is keyed by arrival, so old backfill would evict live
// objects.
func (h *sessionHandler) stitchedFetchObjects(
	ctx context.Context,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	requestStart message.Location,
	requestEndIncl message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
) []*cache.CachedObject {
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

	// GetRange and OldestRetained are two separate cache reads: an eviction
	// or TTL expiry between them can raise the floor above snapshot entries,
	// making the upstream sub-range [requestStart, upEndIncl] overlap the
	// snapshot. mergeFetchObjects relies on the two sources being disjoint
	// by Location (a duplicate would serialize a non-ascending Object ID),
	// so clip the snapshot to strictly above the sub-range.
	cacheObjs = slices.DeleteFunc(cacheObjs, func(o *cache.CachedObject) bool {
		return !upEndIncl.Less(message.Location{Group: o.GroupID, Object: o.ObjectID})
	})

	up := h.pickFetchUpstream(entry)
	if up == nil {
		// No reachable upstream: the below-floor sub-range has unknown
		// status, not ground-truth non-existence. A plain gap in a
		// FIN-terminated response asserts the latter (§11.4.4), so cover
		// the sub-range with an End of Unknown Range marker instead. This
		// is the unknown-status case, not the §10.2.5 budget case — nothing
		// timed out, we simply have no source to ask.
		return mergeFetchObjects(order,
			unknownWholeRange(requestStart, upEndIncl, order), cacheObjs)
	}

	upstreamObjs := h.fetchUpstreamRange(
		ctx, up, fullName, requestStart, upEndIncl, order, fillTimeout,
	)
	if len(upstreamObjs) == 0 {
		// A clean-FIN, uncapped, empty upstream response: the upstream
		// authoritatively asserted the whole sub-range non-existent, which
		// a plain gap encodes exactly. (Every unknown outcome returns at
		// least a marker element.)
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

// fetchUpstreamRange issues a standalone FETCH for the inclusive range
// [start, endIncl] on the upstream's session, awaits the response stream via
// the relay's fetch router, and returns the decoded objects in the requested
// group order (the upstream FETCH carries the same GROUP_ORDER parameter).
//
// The returned slice preserves what the upstream did and did not vouch for,
// so the downstream response stays truthful under §11.4.4's gap rule (a gap
// in a FIN-terminated response asserts non-existence):
//
//   - Upstream End of Unknown Range markers (§11.4.4.2, 0x10C) are kept as
//     [cache.CachedObject] marker elements and re-emitted downstream.
//   - End of Non-Existent Range markers (0x8C) are dropped: a plain gap in
//     our FIN-terminated response is the semantically equivalent encoding
//     (§9.1 lets relays re-represent missing ranges), and §11.4.4.2 prefers
//     it outside known/unknown splits.
//   - When the upstream vouches for less than the whole sub-range — FETCH
//     rejected, response timeout, a mid-stream error (no FIN, so its gaps
//     assert nothing), or a clean FIN whose FETCH_OK EndLocation was capped
//     below endIncl — the unvouched-for remainder is covered by an unknown
//     marker. The mid-stream-error and descending capped cases collapse to
//     "whole sub-range unknown": exact per-gap markers are inexpressible in
//     §11.4.4's delta encoding wherever the element after a marker would be
//     a same-group, lower-Object-ID transition.
func (h *sessionHandler) fetchUpstreamRange(
	ctx context.Context,
	up *registry.UpstreamSub,
	fullName track.FullTrackName,
	start, endIncl message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
) []*cache.CachedObject {
	unknownWhole := unknownWholeRange(start, endIncl, order)
	timedOutWhole := timedOutWholeRange(start, endIncl, order)

	// §10.2.5: "A value of 0 indicates the relay MUST NOT wait for upstream
	// delivery and MUST report any unavailable Objects as Timed-Out gaps."
	// fillTimeout arrives already resolved (see [resolveFillBudget]), so a zero
	// here is the subscriber's explicit 0, not an absent parameter.
	if fillTimeout == 0 {
		return timedOutWhole
	}

	params := message.Parameters{}
	if order == message.GroupOrderDescending {
		params = append(params, message.GroupOrderParam(message.GroupOrderDescending))
	}
	// §5.1.2: the range rides in LOCATION_FILTER. EndGroupDelta is delta-encoded
	// from the start group, and EndObject makes the end Object-precise.
	params = append(params, message.AbsoluteRangeObjectFilter(
		start, endIncl.Group-start.Group, endIncl.Object))
	fmsg := &message.Fetch{
		Namespace:  fullName.Namespace,
		Name:       fullName.Name,
		Parameters: params,
	}

	// Bound the upstream round-trip so a silent or non-FETCH-answering
	// upstream degrades to cache-plus-unknown-gap instead of wedging the
	// downstream handler. FILL_TIMEOUT, when present, is the subscriber's
	// explicit budget; otherwise fall back to a default.
	fctx, cancel := context.WithTimeout(ctx, fillTimeout)
	defer cancel()

	fr, err := up.Session.Fetch(fctx, fmsg)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH failed",
			slog.String("err", err.Error()))
		if fctx.Err() != nil {
			return timedOutWhole
		}
		return unknownWhole
	}
	defer fr.Close()

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
		return timedOutWhole
	}
	if fs == nil {
		return unknownWhole
	}
	// ReadDecoded needs the response's group order to resolve cross-group
	// deltas (§11.4.4.1); the upstream serves in the order our FETCH asked
	// for.
	fs.GroupOrder = order

	var (
		out      []*cache.CachedObject
		prevLoc  message.Location
		havePrev bool
	)
	for {
		obj, err := fs.ReadDecoded()
		if errors.Is(err, io.EOF) {
			break // clean FIN: the upstream's gaps are authoritative (§11.4.4)
		}
		if err != nil {
			// No FIN (or a FIN mid-object), so the gaps in what arrived
			// assert nothing; declare the whole sub-range unknown rather
			// than serve partial objects whose gaps would read as
			// non-existence.
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH stream failed mid-read",
				slog.String("err", err.Error()))
			return unknownWhole
		}
		if obj.EndOfNonExistentRange {
			// Dropped: a plain gap in our FIN-terminated response is the
			// semantically equivalent encoding (§9.1).
			continue
		}
		loc := message.Location{Group: obj.GroupID, Object: obj.ObjectID}
		if !upstreamFetchElemOK(loc, prevLoc, havePrev, start, endIncl, order,
			obj.EndOfUnknownRange || obj.EndOfTimedOutRange) {
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH element out of range or order",
				slog.Uint64("group", loc.Group), slog.Uint64("object", loc.Object))
			return unknownWhole
		}
		prevLoc, havePrev = loc, true
		if obj.EndOfUnknownRange {
			out = append(out, unknownRangeMarker(loc))
			continue
		}
		if obj.EndOfTimedOutRange {
			out = append(out, timedOutRangeMarker(loc))
			continue
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

	// A clean FIN asserts gaps only up to the FETCH_OK EndLocation (§11.4.4).
	// If the upstream capped it below our sub-range end (§10.13: End beyond
	// its Largest), the remainder has unknown status.
	if authEnd := fr.OK.EndLocation; authEnd.Less(endIncl) {
		if order == message.GroupOrderDescending {
			// The unknown remainder precedes every object in descending
			// stream order, and a leading marker cannot in general be
			// followed by a same-group object with a lower ID (see the
			// doc comment) — fall back to whole-sub-range unknown.
			return unknownWhole
		}
		out = append(out, unknownRangeMarker(endIncl))
	}
	return out
}

// upstreamFetchElemOK validates one kept element of an upstream FETCH
// response before it is re-serialized downstream. Every element must lie
// inside the requested sub-range [start, endIncl] — the merge with the
// cached part relies on Location disjointness — and an object must advance
// from the previous kept element the way §11.4.4's delta encoding can
// express: within a group, Object IDs strictly ascend; across groups, the
// Group ID moves in the response's order direction. Unknown-range markers
// carry absolute IDs and merely re-anchor the encoding, so only the range
// check applies to them. A violation means the upstream is nonconformant;
// trusting the element would corrupt the downstream delta stream (e.g. flip
// its group-direction inference), so the caller discards the response.
func upstreamFetchElemOK(
	loc, prev message.Location,
	havePrev bool,
	start, endIncl message.Location,
	order message.GroupOrder,
	isMarker bool,
) bool {
	if loc.Less(start) || endIncl.Less(loc) {
		return false
	}
	if isMarker || !havePrev {
		return true
	}
	if loc.Group == prev.Group {
		return prev.Object < loc.Object
	}
	if order == message.GroupOrderDescending {
		return loc.Group < prev.Group
	}
	return prev.Group < loc.Group
}

// unknownRangeMarker returns the serve-path element that streamFetchObjects
// serializes as a §11.4.4.2 End of Unknown Range (0x10C) marker at loc.
func unknownRangeMarker(loc message.Location) *cache.CachedObject {
	return &cache.CachedObject{
		GroupID:           loc.Group,
		ObjectID:          loc.Object,
		EndOfUnknownRange: true,
	}
}

// timedOutRangeMarker is [unknownRangeMarker] for the §10.2.5 case: the
// FILL_TIMEOUT budget ran out, so the Objects are reported as Timed-Out rather
// than unknown-status gaps.
func timedOutRangeMarker(loc message.Location) *cache.CachedObject {
	return &cache.CachedObject{
		GroupID:            loc.Group,
		ObjectID:           loc.Object,
		EndOfTimedOutRange: true,
	}
}

// unknownWholeRange declares the whole inclusive sub-range [start, endIncl]
// unknown with a single marker, positioned for the response's stream order.
// The marker Location is the range's far end in stream direction (endIncl
// when ascending, start when descending), so §11.4.4.2's "between the last
// serialized Object, if any, and this Location, inclusive" coverage spans
// the sub-range.
func unknownWholeRange(start, endIncl message.Location, order message.GroupOrder) []*cache.CachedObject {
	return wholeRange(unknownRangeMarker, start, endIncl, order)
}

// timedOutWholeRange is [unknownWholeRange] with the §11.4.4.2 End of
// Timed-Out Range marker, for when the FILL_TIMEOUT budget is what stopped us
// (§10.2.5) rather than an unreachable or unhelpful upstream.
func timedOutWholeRange(start, endIncl message.Location, order message.GroupOrder) []*cache.CachedObject {
	return wholeRange(timedOutRangeMarker, start, endIncl, order)
}

// wholeRange covers [start, endIncl] with a single marker built by mark. The
// marker names the far end of the range in delivery order, since §11.4.4.2
// markers cover everything from the previous element up to their own Location.
func wholeRange(
	mark func(message.Location) *cache.CachedObject,
	start, endIncl message.Location,
	order message.GroupOrder,
) []*cache.CachedObject {
	if order == message.GroupOrderDescending {
		return []*cache.CachedObject{mark(start)}
	}
	return []*cache.CachedObject{mark(endIncl)}
}

// mergeFetchObjects merges the below-floor (upstream) and at/above-floor
// (cache) slices in group order. The two are disjoint by Location and each is
// already sorted in order, so for ascending the lower range leads and for
// descending the higher (cache) range leads.
//
// Descending needs one more step: within a group, Object IDs always ascend
// (§11.4.3), and §11.4.4's delta encoding cannot express a same-group
// transition to a lower Object ID — so when the eviction floor splits a
// group across the two sources, the seam group's runs must be spliced into
// one contiguous ascending run, upstream part (lower Object IDs) first.
// Plain concatenation would put the cache's high-object run before the
// upstream's low-object run of the same group and serialize a wrapped
// delta. Unknown-range markers interleaved with the seam run's objects move
// with them (their coverage and delta re-anchoring stay as the upstream
// meant them); a marker-only prefix — the whole-sub-range unknown marker,
// whose coverage spans everything below the cache — stays after it.
func mergeFetchObjects(order message.GroupOrder, lower, upper []*cache.CachedObject) []*cache.CachedObject {
	switch {
	case len(lower) == 0:
		return upper
	case len(upper) == 0:
		return lower
	}
	out := make([]*cache.CachedObject, 0, len(lower)+len(upper))
	if order != message.GroupOrderDescending {
		out = append(out, lower...)
		out = append(out, upper...)
		return out
	}

	// The only group the two sources can share is the cache's lowest
	// (upper's last element) — the floor group. splice is the length of
	// lower's leading seam-group run, markers included: an interleaved
	// upstream 0x10C marker belongs with its neighbouring objects (its
	// coverage and the delta re-anchoring stay exactly as the upstream
	// meant them, and every spliced Location is below the cache's seam
	// objects). A prefix with no objects at all is NOT spliced — that is
	// the whole-sub-range unknown marker, whose coverage spans everything
	// below the cache and must stay after it.
	seamG := upper[len(upper)-1].GroupID
	splice, seamHasObject := 0, false
	for splice < len(lower) && lower[splice].GroupID == seamG {
		seamHasObject = seamHasObject || !lower[splice].IsRangeMarker()
		splice++
	}
	if !seamHasObject {
		splice = 0
	}
	// cut is where upper's trailing seam-group run starts (the cache never
	// holds unknown-range markers, so a plain group comparison suffices).
	cut := len(upper)
	for cut > 0 && upper[cut-1].GroupID == seamG {
		cut--
	}
	out = append(out, upper[:cut]...)
	out = append(out, lower[:splice]...)
	out = append(out, upper[cut:]...)
	out = append(out, lower[splice:]...)
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

// streamFetchObjects writes the cached objects to the FETCH response
// stream with §11.4.4 delta encoding:
//
//   - The first object includes both GroupIDDelta and ObjectIDDelta
//     flags; the values are absolute (§11.4.4.1).
//   - Subsequent objects in the same group use ObjectIDDelta only when
//     the gap is > 0 (a consecutive object omits the flag, the
//     subscriber reconstructs ObjectID = prior + 1).
//   - Subsequent objects in a different group set GroupIDDelta:
//     ascending → newGroup - priorGroup - 1, descending →
//     priorGroup - newGroup - 1 (§11.4.4.1). ObjectIDDelta is then the
//     absolute Object ID in the new group.
//   - Datagram-flavoured objects set bit 0x40 (§11.4.4.1); subscriber
//     ignores the subgroup bits.
//   - [cache.CachedObject.EndOfUnknownRange] elements serialize as §11.4.4.2
//     End of Unknown Range markers (0x10C) with absolute Group/Object IDs,
//     and become the prior Location for the delta encoding of what follows.
//
// The returned count is the number of real objects written (markers are
// serialized but not counted — they carry no payload).
func streamFetchObjects(out *session.OutgoingFetchStream, objs []*cache.CachedObject) (int, error) {
	var (
		written      int
		prevGroup    uint64
		prevObject   uint64
		prevPriority uint8
		// havePrev: a prior Group/Object ID exists — a real object or a
		// §11.4.4.2 End-of-Range marker. haveActual: a real object was
		// written — only then do a prior Subgroup ID / Priority exist
		// (mirror of ReadDecoded's decHavePrev / decHaveActual).
		havePrev   bool
		haveActual bool
		// Inferred from the ordering of the first vs second object.
		// Without a second object we don't need the direction.
		descending bool
	)

	for _, o := range objs {
		if o.IsRangeMarker() {
			// §11.4.4.2 End of Unknown / Timed-Out Range: the Group/Object ID fields
			// carry the absolute range boundary, and the marker becomes
			// the prior Location for subsequent delta encoding — but not
			// a prior *actual* object, so the next object still spells
			// out its Priority (and never references the prior Subgroup).
			flags := uint64(message.FetchEndOfRangeGroup)
			if o.EndOfTimedOutRange {
				flags = message.FetchEndOfTimedOutRange
			}
			if err := out.WriteObject(&message.FetchObject{
				SerializationFlags: flags,
				GroupIDDelta:       o.GroupID,
				ObjectIDDelta:      o.ObjectID,
			}); err != nil {
				return written, err
			}
			prevGroup, prevObject = o.GroupID, o.ObjectID
			havePrev = true
			continue
		}

		// §11.2.1.1: the Object Status field "is absent in Objects
		// delivered via a FETCH". Cached status markers describe absence,
		// so they are simply not serialized — their knowledge still reaches
		// the fetcher: the marker bumped the LARGEST_OBJECT watermark on
		// ingest, FETCH_OK's EndLocation extends through it
		// (capFetchEndLocation), and §11.4.4's gap rule makes the trailing
		// gap of a FIN-terminated response authoritative non-existence.
		// Emitting End of Non-Existent Range (0x8C) instead would be
		// redundant: §11.4.4.2 reserves it for splitting non-serialized
		// ranges into known-non-existent and unknown parts.
		if o.IsStatusMarker() {
			continue
		}

		fo := &message.FetchObject{}

		switch {
		case !havePrev:
			// §11.4.4.1: first object MUST include both
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
			// Same group. §11.4.4 cannot express a non-ascending Object ID
			// here — the delta only ever adds. The inputs are sorted and
			// seam-spliced (mergeFetchObjects), so hitting this is an
			// internal invariant violation; fail rather than emit a wrapped
			// delta the subscriber must treat as a session-fatal overflow.
			if o.ObjectID <= prevObject {
				return written, fmt.Errorf(
					"relay: fetch serialization order violation: {%d,%d} after {%d,%d}",
					o.GroupID, o.ObjectID, prevGroup, prevObject)
			}
			// Omit ObjectIDDelta when consecutive; otherwise include it
			// with the gap value.
			if o.ObjectID != prevObject+1 {
				fo.SerializationFlags |= message.FetchFlagObjectIDDelta
				fo.ObjectIDDelta = o.ObjectID - prevObject - 1
			}
		}

		switch o.ForwardingPref {
		case cache.ForwardingDatagram:
			// §11.4.4.1: bit 0x40 marks the object as a
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

		// Publisher priority: emit when it differs from the prior actual
		// object's — or when there is none (the first object, and the
		// first object after a leading marker, §11.4.4.2).
		if !haveActual || o.PublisherPriority != prevPriority {
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
		haveActual = true
	}

	return written, nil
}
