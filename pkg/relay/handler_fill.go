package relay

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// maybeServeFill opens and serves a fill fetch stream for a subscription when
// the SUBSCRIBE or REQUEST_UPDATE carried FILL_PARAMETERS (§5.1.3).
//
// requestID is the Request ID of the message that asked for the fill; the
// FETCH_HEADER carries it, so one subscription can have several fills open.
//
// A failure resets the fill stream, leaves the subscription unaffected, and
// is returned for the log. AcceptRequest has closed the session on a malformed
// FILL_PARAMETERS (§10.2.15; see [message.Parameters.CheckScope]).
func (h *sessionHandler) maybeServeFill(
	ctx context.Context,
	sub *registry.DownstreamSub,
	entry *registry.TrackEntry,
	fullName track.FullTrackName,
	requestID uint64,
	ps message.Parameters,
) error {
	inner, requested, _ := message.FillParametersFromParam(ps)
	if !requested {
		return nil
	}

	// §5.1.3.1: from here a failure MUST open the fill stream and reset it.
	fail := func(err error) error {
		h.resetFillStream(ctx, sub, requestID)
		return err
	}

	// §5.1.3.1: only "while Forward State is 1"; a later unpause does not
	// open one retroactively.
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
	filter, _ := message.LocationFilterFromParam(inner)
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

	// §5.1.3: the fill "inherits the subscription's parameters". A filter
	// type inside FILL_PARAMETERS overrides that type as a REQUEST_UPDATE
	// would (§5.1.4); the other types are inherited.
	rangeFilters := sub.GetRangeFilters()
	if slices.ContainsFunc(inner, func(p message.Parameter) bool { return message.IsRangeFilterParam(p.Type) }) {
		var err error
		rangeFilters, err = rangeFilters.Update(inner)
		if err == nil && rangeFilters != nil {
			err = rangeFilters.Validate(h.sess.MaxFilterRanges())
		}
		if err != nil {
			return fail(err)
		}
	}

	h.relayGo(func() {
		// §5.1.3.1: "When the subscription is cancelled, the publisher MUST
		// reset any open fill fetch streams".
		fillCtx, cancel := context.WithCancelCause(ctx)
		defer cancel(nil)
		defer context.AfterFunc(sub.Cancelled(), func() { cancel(errRequestCancelled) })()
		h.serveFill(fillCtx, sub, requestID, entry, fullName, start, end, order, fillTimeout, rangeFilters)
	})
	return nil
}

// serveFill writes one fill fetch stream; the FIN signals completion
// (§5.1.3.1), and [sessionHandler.streamFetchRange] resets it on a write error.
func (h *sessionHandler) serveFill(
	ctx context.Context,
	sub *registry.DownstreamSub,
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

	h.streamFetchRange(ctx, "fill", sub, requestID, entry, fullName,
		start, end, order, fillTimeout, rangeFilters)
}

// resetFillStream signals a fill failure the only way §5.1.3.1 allows: open the
// fill fetch stream and reset it right after the FETCH_HEADER. Otherwise the
// subscriber cannot tell it from an empty fill range, which opens no stream.
func (h *sessionHandler) resetFillStream(ctx context.Context, sub *registry.DownstreamSub, requestID uint64) {
	out, err := openFillOrFetchStream(h.sess, sub, requestID)
	if err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "could not open fill stream to reset it",
			slog.Uint64("request_id", requestID), slog.String("err", err.Error()))
		return
	}
	out.Cancel(moqt.StreamResetInternalError)
	sub.StreamClosed()
}

// errSubscriptionTerminated reports a subgroup or fill stream not opened
// because its subscription already ended: §10.12 forbids streams after
// PUBLISH_DONE.
var errSubscriptionTerminated = errors.New("relay: subscription terminated before the stream opened")

// openFillOrFetchStream opens a FETCH_HEADER stream. A fill fetch stream
// counts toward sub's §10.12 PUBLISH_DONE Stream Count; a standalone FETCH
// response passes a nil sub.
func openFillOrFetchStream(
	sess *session.Session,
	sub *registry.DownstreamSub,
	requestID uint64,
) (*session.OutgoingFetchStream, error) {
	if sub == nil {
		return sess.OpenFetchStream(message.FetchHeader{RequestID: requestID})
	}
	if !sub.BeginStream() {
		return nil, errSubscriptionTerminated
	}
	out, err := sess.OpenFetchStream(message.FetchHeader{RequestID: requestID})
	sub.EndStream(err == nil)
	return out, err
}
