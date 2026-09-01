package relay

import (
	"context"
	"log/slog"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// maybeServeFill opens and serves a fill fetch stream for a subscription when
// the SUBSCRIBE or REQUEST_UPDATE carried FILL_PARAMETERS (§5.1.3), which is
// draft-20's replacement for the Joining FETCH.
//
// requestID is the Request ID of the message that asked for the fill — the
// SUBSCRIBE's for an initial fill, the REQUEST_UPDATE's for a later one — and
// it is what the FETCH_HEADER carries, so a subscription can have several fill
// fetch streams open at once, each named by its own Request ID.
//
// It returns an error only for a malformed FILL_PARAMETERS, which the caller
// MUST turn into a session-level PROTOCOL_VIOLATION (§10.2.15). Everything
// else is best-effort: §5.1.3.1 has no REQUEST_ERROR for a fill, so a failure
// is signalled by resetting the stream, and the subscription itself is
// unaffected either way.
func (h *sessionHandler) maybeServeFill(
	ctx context.Context,
	sub *registry.DownstreamSub,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	requestID uint64,
	ps message.Parameters,
) error {
	inner, requested, err := message.FillParametersFromParam(ps)
	if err != nil {
		return err
	}
	if !requested {
		return nil
	}

	// From here the peer has asked for a fill, so §5.1.3.1's failure signal
	// applies to everything that can still go wrong: "Because there is no
	// REQUEST_ERROR associated with a fill fetch stream, the publisher signals a
	// fill failure by resetting the stream; it MUST open a fill fetch stream and
	// reset it immediately after the FETCH_HEADER if necessary."
	fail := func(err error) error {
		h.resetFillStream(ctx, requestID)
		return err
	}

	// §5.1.3.1: "A publisher opens a fill fetch stream when it processes a
	// SUBSCRIBE or REQUEST_UPDATE that carries FILL_PARAMETERS while Forward
	// State is 1." FILL_PARAMETERS arriving while paused opens nothing, and a
	// later unpause does not retroactively open one.
	if sub.ForwardState() != 1 {
		return nil
	}

	// The fill range is evaluated with Fetch rules (§5.1.2), so it never
	// extends past Largest Object. With nothing published there is nothing to
	// fill.
	largest, hasLargest := entry.GetLargest()
	if !hasLargest {
		return nil
	}

	// §5.1.3: the fill range comes from the LOCATION_FILTER inside
	// FILL_PARAMETERS, falling back to the subscription's own filter, and to
	// the whole track when neither is present.
	filter, err := message.LocationFilterFromParam(inner)
	if err != nil {
		return fail(err)
	}
	if filter == nil {
		filter = sub.GetFilter()
	}
	if filter == nil {
		filter = &message.LocationFilter{}
	}

	start := filter.Start(largest, hasLargest)
	end := capFetchEndLocation(filter, largest)
	// §5.1.3: "If the fill range is empty, or starts after Largest Object, the
	// publisher does not open a fill fetch stream."
	if largest.Less(start) || end.Less(start) {
		return nil
	}

	// §10.2.15: a parameter omitted from FILL_PARAMETERS keeps the value it
	// has for the subscription, so the inner list only carries the overrides.
	order := message.GroupOrder(sub.GroupOrder)
	if p, ok := inner.Find(message.ParamGroupOrder); ok {
		order = message.GroupOrder(p.Byte)
	}
	fillTimeout := resolveFillBudget(inner)

	rangeFilters, err := message.RangeFiltersFromParams(inner)
	if err == nil && rangeFilters != nil {
		err = rangeFilters.Validate(h.sess.MaxFilterRanges())
	}
	if err != nil {
		return fail(err)
	}

	h.relayGo(func() {
		h.serveFill(ctx, requestID, entry, fullName, start, end, order, fillTimeout, rangeFilters)
	})
	return nil
}

// TODO(draft-20): §5.1.3.1 also requires "When the subscription is cancelled,
// the publisher MUST reset any open fill fetch streams." That needs a watchdog
// resetting the stream on ctx cancellation mid-write, which is a concurrency
// change worth landing with -race coverage — i.e. with the test slice.

// serveFill writes one fill fetch stream and closes it. §5.1.3.1: the FIN is
// what signals the fill is complete, and because a fill has no REQUEST_ERROR
// of its own, a failure is signalled by resetting the stream —
// [sessionHandler.streamFetchRange] does that on a write error.
func (h *sessionHandler) serveFill(
	ctx context.Context,
	requestID uint64,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	start, end message.Location,
	order message.GroupOrder,
	fillTimeout time.Duration,
	rangeFilters *message.RangeFilterSet,
) {
	h.log.LogAttrs(ctx, slog.LevelDebug, "serving fill fetch stream",
		slog.Uint64("request_id", requestID),
		slog.Uint64("start_group", start.Group),
		slog.Uint64("end_group", end.Group))

	h.streamFetchRange(ctx, "fill", requestID, entry, fullName,
		start, end, order, fillTimeout, rangeFilters)
}

// resetFillStream signals a fill failure the only way §5.1.3.1 allows: open the
// fill fetch stream and reset it immediately after the FETCH_HEADER. Without
// it the subscriber cannot tell a failed fill from the legitimate "fill range
// is empty, so no stream" case (§5.1.3), and waits forever.
func (h *sessionHandler) resetFillStream(ctx context.Context, requestID uint64) {
	out, err := h.sess.OpenFetchStream(message.FetchHeader{RequestID: requestID})
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "could not open fill stream to reset it",
			slog.Uint64("request_id", requestID), slog.String("err", err.Error()))
		return
	}
	out.Cancel(moqt.StreamResetInternalError)
}
