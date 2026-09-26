package relay

import (
	"cmp"
	"math"
	"slices"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// A FETCH response asserts what it does not carry: "Any gaps in the Group and
// Object IDs in the response stream indicate objects that do not exist"
// (§10.13), and "All signals that an Object does not exist are
// authoritative" (§2.1). The relay knows an Object does not exist only from a
// signal — a Prior Group or Object ID Gap (§12.8, §12.9), a Group's or the
// Track's end (§11.2.1.1, §11.4.2), or an upstream's FETCH response — never from a gap in
// what it happened to receive: "A gap in the observed Object IDs does not by
// itself convey any information about the skipped Objects" (§2.1). So a
// response is built from the requested range classified into Objects, known
// absent Locations, and the rest, whose status is unknown and is either asked
// of an upstream (§10.13) or marked with an End of Unknown Range (§11.4.4.2).
//
// Interpretation: an End of Range marker covers "Locations between the last
// serialized Object, if any, and this Location" (§11.4.4.2) in the order the
// response carries them: Groups in its Group Order, Object IDs ascending
// within a Group. In Descending order that is not Location order.

// locSucc returns the Location right after l, and false when l is the last.
func locSucc(l message.Location) (message.Location, bool) {
	switch {
	case l.Object < math.MaxUint64:
		return message.Location{Group: l.Group, Object: l.Object + 1}, true
	case l.Group < math.MaxUint64:
		return message.Location{Group: l.Group + 1}, true
	}
	return message.Location{}, false
}

// uncovered returns the parts of [start, end] no range in known covers, in
// ascending order. It sorts known.
func uncovered(start, end message.Location, known []registry.LocRange) []registry.LocRange {
	slices.SortFunc(known, func(a, b registry.LocRange) int { return a.Lo.Compare(b.Lo) })
	var out []registry.LocRange
	cur := start
	for _, k := range known {
		if k.Hi.Less(cur) {
			continue
		}
		if end.Less(k.Lo) {
			break
		}
		if cur.Less(k.Lo) {
			pred, _ := fetchPredecessor(k.Lo) // k.Lo > cur, so it has one
			out = append(out, registry.LocRange{Lo: cur, Hi: pred})
		}
		next, ok := locSucc(k.Hi)
		if !ok || end.Less(next) {
			return out
		}
		cur = next
	}
	return append(out, registry.LocRange{Lo: cur, Hi: end})
}

// knownFromCache is what objs, Objects the cache holds, establish: each
// Object's Location; from an END_OF_GROUP or END_OF_TRACK status, the rest of
// its Group or of the Track (§11.2.1.1); and the gaps its Prior Group and
// Object ID Gaps announce (§12.8, §12.9). [message.CheckObjectProperties]
// rejected a gap above an Object's ID on receipt.
func knownFromCache(objs []*cache.CachedObject) []registry.LocRange {
	out := make([]registry.LocRange, 0, len(objs))
	for _, o := range objs {
		loc := message.Location{Group: o.GroupID, Object: o.ObjectID}
		r := registry.LocRange{Lo: loc, Hi: loc}
		switch o.Status {
		case message.ObjectStatusEndOfGroup:
			r.Hi.Object = math.MaxUint64
		case message.ObjectStatusEndOfTrack:
			r.Hi = message.Location{Group: math.MaxUint64, Object: math.MaxUint64}
		}
		out = append(out, r)
		gaps := message.ObjectPriorGaps(o.Properties)
		if gaps.HasObject && gaps.Object > 0 {
			out = append(out, registry.LocRange{
				Lo: message.Location{Group: o.GroupID, Object: o.ObjectID - gaps.Object},
				Hi: message.Location{Group: o.GroupID, Object: o.ObjectID - 1},
			})
		}
		if gaps.HasGroup && gaps.Group > 0 {
			out = append(out, registry.LocRange{
				Lo: message.Location{Group: o.GroupID - gaps.Group},
				Hi: message.Location{Group: o.GroupID - 1, Object: math.MaxUint64},
			})
		}
	}
	return out
}

// unknownIn returns the Locations of [start, end] whose status the relay does
// not know: neither among objs, the cached Objects of the range, nor known
// absent from them or entry's ledger.
func unknownIn(
	entry *registry.TrackEntry,
	objs []*cache.CachedObject,
	start, end message.Location,
) []registry.LocRange {
	return uncovered(start, end, append(knownFromCache(objs), entry.KnownAbsent()...))
}

// streamCompare orders Locations as a FETCH response in order carries them
// (§10.13): Groups in the Group Order, Object IDs ascending within a Group.
func streamCompare(a, b message.Location, order message.GroupOrder) int {
	if order == message.GroupOrderDescending && a.Group != b.Group {
		return cmp.Compare(b.Group, a.Group)
	}
	return a.Compare(b)
}

// runEnds returns where each part of r a response in order carries without
// interruption ends, in stream order. Ascending, r is one run. Descending, a
// range across Groups is up to three: the start of its highest Group, the
// Groups strictly between, and the rest of its lowest Group.
func runEnds(r registry.LocRange, order message.GroupOrder) []message.Location {
	if order != message.GroupOrderDescending || r.Lo.Group == r.Hi.Group {
		return []message.Location{r.Hi}
	}
	ends := []message.Location{r.Hi}
	if r.Hi.Group-r.Lo.Group > 1 {
		ends = append(ends, message.Location{Group: r.Lo.Group + 1, Object: math.MaxUint64})
	}
	return append(ends, message.Location{Group: r.Lo.Group, Object: math.MaxUint64})
}

// fetchElements orders objs for a response in order and adds, at the end of
// each run of unknown and timedOut Locations, an End of Unknown or Timed-Out
// Range marker (§11.4.4.2), so that a plain gap in the response only ever
// covers Locations known not to exist. unknown and timedOut must be disjoint
// and hold no Object of objs. A marker's run starts after the previous
// element, so it may also cover known-absent Locations: weaker, never false.
func fetchElements(
	objs []*cache.CachedObject,
	unknown, timedOut []registry.LocRange,
	order message.GroupOrder,
) []*cache.CachedObject {
	elems := slices.Clone(objs)
	for _, r := range unknown {
		for _, at := range runEnds(r, order) {
			elems = append(elems, unknownRangeMarker(at))
		}
	}
	for _, r := range timedOut {
		for _, at := range runEnds(r, order) {
			elems = append(elems, timedOutRangeMarker(at))
		}
	}
	slices.SortStableFunc(elems, func(a, b *cache.CachedObject) int {
		return streamCompare(
			message.Location{Group: a.GroupID, Object: a.ObjectID},
			message.Location{Group: b.GroupID, Object: b.ObjectID},
			order)
	})
	// A marker's coverage starts after the previous element, so a marker
	// right after one of its own kind covers both: keep only the later.
	out := elems[:0]
	for i, e := range elems {
		if i+1 < len(elems) && e.IsRangeMarker() && elems[i+1].IsRangeMarker() &&
			e.EndOfTimedOutRange == elems[i+1].EndOfTimedOutRange {
			continue
		}
		out = append(out, e)
	}
	return out
}
