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
// handler: once it elapses, the cache is served with the unknown Locations
// marked Timed-Out.
const defaultUpstreamFetchTimeout = 5 * time.Second

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

// handleFetch implements FETCH (§10.13): validate the requested range, reply
// FETCH_OK, open a FETCH_HEADER uni-stream, and serialise the cached objects
// in the requested group order. Gaps in the response stream are how the spec
// signals "objects do not exist" (§10.13), so what the cache cannot vouch for
// is asked of an upstream FETCH when one is reachable, or covered by §11.4.4.2
// End of Range markers; see [sessionHandler.stitchedFetchObjects].
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
	// filter fetches the whole track up to Largest Object. AcceptRequest has
	// validated it (see [message.Parameters.CheckScope]).
	filter, _ := message.LocationFilterFromParam(msg.Parameters)
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
	var properties []byte
	if includeProperties(msg.Parameters) { // §10.2.21
		properties = entry.GetProperties()
	}
	if err := req.Reply(&message.FetchOK{
		EndLocation:     endLocation,
		TrackProperties: properties,
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
// other follow-up is ignored. On the requester's FIN the relay FINs back
// (§3.3.2).
func (h *sessionHandler) readFetchUpdates(ctx context.Context, req *session.Request) {
	updates := h.sess.NewRequestUpdateLimiter()
	fin := readRequestStream(ctx, h.sess, req.Stream, func(m message.Message) bool {
		if h.isPeerStateNotify(m) {
			return false
		}
		if upd, ok := m.(*message.RequestUpdate); ok {
			// §10.2.1: out-of-scope parameters are session-fatal.
			if h.sess.CheckPeerParams(message.ScopeUpdateFetch, upd) != nil {
				return false
			}
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
			if _, ok := h.handleFollowupTokens(ctx, upd); !ok {
				return false
			}
			h.handleFetchUpdate(ctx, req)
			updates.Responded()
		}
		return true
	})
	if fin {
		_ = req.Stream.Close()
	}
}

// handleFetchUpdate answers a REQUEST_UPDATE (§10.9) to an in-flight FETCH
// with REQUEST_OK: the in-scope parameters have nothing to change on a
// finished snapshot.
func (h *sessionHandler) handleFetchUpdate(ctx context.Context, req *session.Request) {
	if err := req.Reply(&message.RequestOK{}); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "FETCH REQUEST_UPDATE_OK write failed",
			slog.String("err", err.Error()))
	}
}

// fetchGroupOrder is a FETCH's GROUP_ORDER (§10.2.8): Ascending when omitted.
// AcceptRequest has closed the session on a value outside {1, 2}.
func fetchGroupOrder(ps message.Parameters) message.GroupOrder {
	if p, ok := ps.Find(message.ParamGroupOrder); ok {
		return message.GroupOrder(p.Byte)
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

// stitchedFetchObjects answers a FETCH range [start, end] from the relay's
// cache, asking an upstream about the Locations whose status it does not know
// (§10.13: "If it encounters an object in the requested range that is not
// cached and has unknown status, the relay MUST pause subsequent delivery
// until it has confirmed the object's status upstream"). See fetch_ranges.go
// for what the relay knows.
//
// With a fetch-capable upstream, one FETCH covers the span from the first
// unknown Location to the last, within the FILL_TIMEOUT budget (§10.2.5). Its
// Objects fill the holes, and what it marks unknown or timed out stays so where
// the relay does not know better; the cached Objects are served either way,
// and what the relay knows does not exist stays a gap. With no such upstream, or
// when its FETCH fails or times out, the unknown Locations are marked End of
// Unknown or Timed-Out Range (§11.4.4.2) and the cached Objects served: the
// relay can "indicate the range of unknown Objects and continue serving other
// known Objects" (§10.13). Upstream-fetched objects are NOT cached back: the
// FIFO ring is keyed by arrival, so old backfill would evict live objects.
//
// A non-nil refusal (see fetchUpstreamRange) means the track must not be
// forwarded; no objects are returned.
func (h *sessionHandler) stitchedFetchObjects(
	ctx context.Context,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	start, end message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
) (objs []*cache.CachedObject, refusal error) {
	// An expired Object (§12.3) is not returned: its status is unknown.
	cached := entry.Cache.GetRange(start, end, message.GroupOrderAscending)
	unknown := unknownIn(entry, cached, start, end)
	if len(unknown) == 0 {
		return fetchElements(cached, nil, nil, order), nil
	}
	up := h.pickFetchUpstream(entry)
	if up == nil {
		return fetchElements(cached, unknown, nil, order), nil
	}
	span := registry.LocRange{Lo: unknown[0].Lo, Hi: unknown[len(unknown)-1].Hi}
	ans, refusal := h.fetchUpstreamRange(ctx, up, fullName, span, order, fillTimeout)
	if errors.Is(refusal, session.ErrMalformedTrack) {
		h.endMalformedTrack(ctx, entry, up.Session, refusal)
	}
	if refusal != nil {
		return nil, refusal
	}
	switch ans.failed {
	case upstreamUnknown:
		return fetchElements(cached, unknown, nil, order), nil
	case upstreamTimedOut:
		return fetchElements(cached, nil, unknown, order), nil
	case upstreamAnswered:
	}

	// The upstream answered for the span: its Objects fill the holes, and
	// under its FIN the rest does not exist, except what it marked unknown or
	// timed out and the relay has no signal for either.
	have := make(map[message.Location]bool, len(cached))
	for _, o := range cached {
		have[message.Location{Group: o.GroupID, Object: o.ObjectID}] = true
	}
	merged := cached
	for _, o := range ans.objs {
		if !have[message.Location{Group: o.GroupID, Object: o.ObjectID}] {
			merged = append(merged, o)
		}
	}
	return fetchElements(merged, intersect(ans.unknown, unknown), intersect(ans.timedOut, unknown), order), nil
}

// intersect returns the Locations both a and b hold, each a set of disjoint
// ranges.
func intersect(a, b []registry.LocRange) []registry.LocRange {
	var out []registry.LocRange
	for _, x := range a {
		for _, y := range b {
			lo, hi := x.Lo, x.Hi
			if lo.Less(y.Lo) {
				lo = y.Lo
			}
			if y.Hi.Less(hi) {
				hi = y.Hi
			}
			if !hi.Less(lo) {
				out = append(out, registry.LocRange{Lo: lo, Hi: hi})
			}
		}
	}
	return out
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
		if u.FetchCapable && u.IsEstablished() && u.Session != nil && u.Session != h.sess &&
			!peerSentGoaway(u.Session) {
			return u
		}
	}
	return nil
}

// upstreamFailure is how an upstream FETCH failed to answer at all.
type upstreamFailure uint8

const (
	upstreamAnswered upstreamFailure = iota
	// upstreamUnknown: refused, reset, malformed or out of order; nothing it
	// sent is vouched for.
	upstreamUnknown
	// upstreamTimedOut: the FILL_TIMEOUT budget ran out (§10.2.5).
	upstreamTimedOut
)

// upstreamAnswer is an upstream FETCH response for a span, as Location ranges:
// its Objects; the parts its End of Unknown and Timed-Out Range markers
// covered, and any past a capped FETCH_OK End Location; every other Location
// of the span, a gap under a clean FIN, is known not to exist (§10.13).
type upstreamAnswer struct {
	objs              []*cache.CachedObject
	unknown, timedOut []registry.LocRange
	failed            upstreamFailure
}

// fetchUpstreamRange issues a standalone FETCH for span on the upstream's
// session, awaits the response stream via the relay's fetch router, and reads
// it into an upstreamAnswer. End of Non-Existent Range markers need no
// record: under a clean FIN a gap already says so.
//
// It returns a refusal instead when the track MUST NOT be forwarded: a
// FETCH_OK with unacceptable Track Properties (§2.5.1), or a response Object
// that makes the track malformed (§2.4.2, wrapping [session.ErrMalformedTrack]).
func (h *sessionHandler) fetchUpstreamRange(
	ctx context.Context,
	up *registry.UpstreamSub,
	fullName track.FullTrackName,
	span registry.LocRange,
	order message.GroupOrder,
	fillTimeout time.Duration,
) (ans upstreamAnswer, refusal error) {
	// §10.2.5: an explicit 0 means "MUST NOT wait for upstream delivery"
	// (fillTimeout is already resolved, see [resolveFillBudget]).
	if fillTimeout == 0 {
		return upstreamAnswer{failed: upstreamTimedOut}, nil
	}

	params := message.Parameters{}
	if order == message.GroupOrderDescending {
		params = append(params, message.GroupOrderParam(message.GroupOrderDescending))
	}
	// §5.1.2: the range rides in LOCATION_FILTER. EndGroupDelta is delta-encoded
	// from the start group, and EndObject makes the end Object-precise.
	params = append(params, message.AbsoluteRangeObjectFilter(
		span.Lo, span.Hi.Group-span.Lo.Group, span.Hi.Object))
	fmsg := &message.Fetch{
		Namespace:  fullName.Namespace,
		Name:       fullName.Name,
		Parameters: params,
	}

	// Bound the upstream round-trip so a silent or non-FETCH-answering
	// upstream degrades to cache-plus-marked-unknown instead of wedging the
	// downstream handler. FILL_TIMEOUT, when present, is the subscriber's
	// explicit budget; otherwise fall back to a default.
	fctx, cancel := context.WithTimeout(ctx, fillTimeout)
	defer cancel()

	fr, err := up.Session.Fetch(fctx, fmsg)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH failed",
			slog.String("err", err.Error()))
		// §2.5.1: Session.Fetch has cancelled it; the caller resets the
		// downstream stream.
		if isTrackPropertiesErr(err) {
			return upstreamAnswer{}, err
		}
		if fctx.Err() != nil {
			return upstreamAnswer{failed: upstreamTimedOut}, nil
		}
		return upstreamAnswer{failed: upstreamUnknown}, nil
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
		return upstreamAnswer{failed: upstreamTimedOut}, nil
	}
	if fs == nil {
		return upstreamAnswer{failed: upstreamUnknown}, nil
	}
	// ReadDecoded needs the response's group order to resolve cross-group
	// deltas (§11.4.4.1); the upstream serves in the order our FETCH asked
	// for.
	fs.GroupOrder = order
	// §10.2.5: the budget covers the response too; when it runs out, what
	// has arrived is kept and the rest reported Timed-Out.
	defer context.AfterFunc(fctx, func() { fs.Cancel(moqt.StreamResetCancelled) })()

	var prev *message.Location
	for {
		obj, err := fs.ReadDecoded()
		if errors.Is(err, io.EOF) {
			break // clean FIN: the upstream's gaps are authoritative (§10.13)
		}
		if errors.Is(err, session.ErrMalformedTrack) {
			// §2.4.2: fr.Close (deferred) cancels the fetch; the caller
			// resets the downstream stream.
			fs.Cancel(moqt.StreamResetMalformedTrack)
			return upstreamAnswer{}, err
		}
		if err != nil && fctx.Err() != nil {
			// Without a FIN its gaps assert nothing (§10.13): all of the span
			// it did not send or mark is Timed-Out.
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH response timed out mid-read")
			known := slices.Concat(ans.unknown, ans.timedOut)
			for _, o := range ans.objs {
				loc := message.Location{Group: o.GroupID, Object: o.ObjectID}
				known = append(known, registry.LocRange{Lo: loc, Hi: loc})
			}
			ans.timedOut = append(ans.timedOut, uncovered(span.Lo, span.Hi, known)...)
			return ans, nil
		}
		if err != nil {
			// No FIN (or a FIN mid-object), so the gaps in what arrived
			// assert nothing.
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH stream failed mid-read",
				slog.String("err", err.Error()))
			return upstreamAnswer{failed: upstreamUnknown}, nil
		}
		loc := message.Location{Group: obj.GroupID, Object: obj.ObjectID}
		// §10.14: nothing past its own End Location.
		if !upstreamFetchElemOK(loc, prev, span, order) || fr.OK.EndLocation.Less(loc) {
			h.log.LogAttrs(ctx, slog.LevelDebug, "upstream FETCH element out of range or order",
				slog.Uint64("group", loc.Group), slog.Uint64("object", loc.Object))
			return upstreamAnswer{failed: upstreamUnknown}, nil
		}
		switch {
		case obj.EndOfUnknownRange:
			ans.unknown = append(ans.unknown, streamCovered(prev, loc, span, order)...)
		case obj.EndOfTimedOutRange:
			ans.timedOut = append(ans.timedOut, streamCovered(prev, loc, span, order)...)
		case obj.EndOfNonExistentRange:
		default:
			// The §11.4.4.1 Datagram bit carries the original wire shape
			// across this relay hop.
			pref := cache.ForwardingSubgroup
			if obj.Datagram {
				pref = cache.ForwardingDatagram
			}
			ans.objs = append(ans.objs, &cache.CachedObject{
				GroupID:           obj.GroupID,
				ObjectID:          obj.ObjectID,
				SubgroupID:        obj.SubgroupID,
				PublisherPriority: obj.PublisherPriority,
				ForwardingPref:    pref,
				Properties:        obj.Properties,
				Payload:           obj.Payload,
			})
		}
		prev = &loc
	}

	// A clean FIN asserts gaps only up to the FETCH_OK End Location (§10.13).
	// If the upstream capped it below the span (§10.13: End beyond its
	// Largest), what lies past it has unknown status.
	// No element lay past it, and Session.Fetch refused one before the span.
	if authEnd := fr.OK.EndLocation; authEnd.Less(span.Hi) {
		next, _ := locSucc(authEnd) // below span.Hi, so it has one
		ans.unknown = append(ans.unknown, registry.LocRange{Lo: next, Hi: span.Hi})
	}
	return ans, nil
}

// upstreamFetchElemOK validates one element of an upstream FETCH response for
// span before it is re-served downstream: it lies inside span, and after the
// previous element prev (nil for the first) in the order the response carries
// them (see [streamCompare]), as §11.4.4's delta encoding requires. A
// violation means the upstream is nonconformant; trusting the element would
// corrupt the downstream stream, so the caller discards the response.
func upstreamFetchElemOK(
	loc message.Location,
	prev *message.Location,
	span registry.LocRange,
	order message.GroupOrder,
) bool {
	if loc.Less(span.Lo) || span.Hi.Less(loc) {
		return false
	}
	return prev == nil || streamCompare(*prev, loc, order) < 0
}

// streamCovered returns, as Location ranges, what an End of Range marker at at
// covers in a response to a FETCH of span in order: the Locations after the
// previous element prev (nil for the first) up to at, in the order the
// response carries them (see fetch_ranges.go). at is after prev.
func streamCovered(
	prev *message.Location,
	at message.Location,
	span registry.LocRange,
	order message.GroupOrder,
) []registry.LocRange {
	if order != message.GroupOrderDescending {
		from := span.Lo
		if prev != nil {
			from, _ = locSucc(*prev) // at is after prev, so it has one
		}
		return []registry.LocRange{{Lo: from, Hi: at}}
	}
	// Descending: Group g of span carries Objects lo(g) through hi(g).
	lo := func(g uint64) uint64 {
		if g == span.Lo.Group {
			return span.Lo.Object
		}
		return 0
	}
	hi := func(g uint64) uint64 {
		if g == span.Hi.Group {
			return span.Hi.Object
		}
		return math.MaxUint64
	}
	var from message.Location
	switch {
	case prev == nil:
		from = message.Location{Group: span.Hi.Group, Object: lo(span.Hi.Group)}
	case prev.Object < hi(prev.Group):
		from = message.Location{Group: prev.Group, Object: prev.Object + 1}
	default:
		from = message.Location{Group: prev.Group - 1, Object: lo(prev.Group - 1)}
	}
	if from.Group == at.Group {
		return []registry.LocRange{{Lo: from, Hi: at}}
	}
	out := []registry.LocRange{{Lo: message.Location{Group: at.Group, Object: lo(at.Group)}, Hi: at}}
	if from.Group-at.Group > 1 {
		out = append(out, registry.LocRange{
			Lo: message.Location{Group: at.Group + 1},
			Hi: message.Location{Group: from.Group - 1, Object: math.MaxUint64},
		})
	}
	return append(out, registry.LocRange{Lo: from, Hi: message.Location{Group: from.Group, Object: hi(from.Group)}})
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
//   - Subsequent objects in the same group omit ObjectIDDelta when
//     consecutive (the subscriber reconstructs ObjectID = prior + 1);
//     otherwise ObjectIDDelta = ObjectID - prior, with no +1 unlike the
//     §11.4.2 subgroup rule (§11.4.4.1).
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
func streamFetchObjects(
	out *session.OutgoingFetchStream,
	objs []*cache.CachedObject,
	expired func(*cache.CachedObject) bool,
) (int, error) {
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
		// §12.3: a slow reader can hold the stream until a cached Object
		// expires; mark it unknown, since a gap asserts non-existence.
		if !o.IsRangeMarker() && !o.IsStatusMarker() && expired != nil && expired(o) {
			o = &cache.CachedObject{GroupID: o.GroupID, ObjectID: o.ObjectID, EndOfUnknownRange: true}
		}
		if o.IsRangeMarker() {
			// §11.4.4.2 End of Unknown / Timed-Out Range: the Group/Object ID fields
			// carry the absolute range boundary, and the marker becomes
			// the prior Location for subsequent delta encoding — but not
			// a prior *actual* object, so the next object still spells
			// out its Priority (and never references the prior Subgroup).
			flags := uint64(message.FetchEndOfUnknownRange)
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
			// here — the delta only ever adds. The inputs are sorted in
			// stream order (fetchElements), so hitting this is an
			// internal invariant violation; fail rather than emit a wrapped
			// delta the subscriber must treat as a session-fatal overflow.
			if o.ObjectID <= prevObject {
				return written, fmt.Errorf(
					"relay: fetch serialization order violation: {%d,%d} after {%d,%d}",
					o.GroupID, o.ObjectID, prevGroup, prevObject)
			}
			// §11.4.4.1: no +1, unlike the §11.4.2 subgroup rule.
			if o.ObjectID != prevObject+1 {
				fo.SerializationFlags |= message.FetchFlagObjectIDDelta
				fo.ObjectIDDelta = o.ObjectID - prevObject
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
