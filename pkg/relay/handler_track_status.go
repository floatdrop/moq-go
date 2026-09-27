package relay

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// trackStatusTimeout, when set by a test, replaces trackStatusUpstreamTimeout.
var trackStatusTimeout atomic.Int64

// trackStatusUpstreamTimeout bounds a forwarded TRACK_STATUS's upstream round
// trip, as FILL_TIMEOUT's default bounds a stitch FETCH's (§13.6): a
// candidate that does not answer within it counts as TIMEOUT.
const trackStatusUpstreamTimeout = defaultUpstreamFetchTimeout

// handleTrackStatus implements TRACK_STATUS (§10.15): a metadata-only query for
// a track's Properties and existence, which the relay "treats ... identically
// as if it had received a SUBSCRIBE", without creating a subscription. The
// reply is TRACK_STATUS_OK (a REQUEST_OK, [message.TrackStatusOK]) carrying
// what the relay's SUBSCRIBE_OK would: the Track Properties and §10.2.17
// LARGEST_OBJECT.
//
// A track with an Established subscription is answered from the track
// registry. Otherwise, as SUBSCRIBE would go upstream, TRACK_STATUS is
// forwarded to every candidate SUBSCRIBE would try (§10.15: relays "MAY
// forward TRACK_STATUS to one or more publishers"; see
// [sessionHandler.trackStatusUpstream]), and their answers are combined as a
// SUBSCRIBE_OK's would be: the entry's Track Properties if it has any, else
// the first answer's (§9.6), and the largest LARGEST_OBJECT of all, the
// entry's included (§10.2.17). If none answers OK, the refusal is the one
// SUBSCRIBE would give.
func (h *sessionHandler) handleTrackStatus(ctx context.Context, req *session.Request, msg *message.TrackStatus) {
	if err := h.auth.AuthorizeTrackStatus(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "TrackStatus", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}
	var (
		properties []byte
		largest    message.Location
		hasLargest bool
	)
	entry, known := h.tracks.Get(fullName.Key())
	if known {
		properties = entry.GetProperties()
		largest, hasLargest = entry.GetLargest()
	}
	if !known || !hasEstablishedUpstream(entry) {
		// §13.1: a forwarded TRACK_STATUS holds upstream requests open, so
		// it counts against the subscription cap while it does.
		if !h.limiter.acquireSub() {
			h.rejectExcessiveLoad(ctx, req, "subscription")
			return
		}
		oks, err := h.trackStatusUpstream(ctx, req, fullName)
		h.limiter.releaseSub()
		if len(oks) == 0 {
			h.rejectTrackStatus(ctx, req, err)
			return
		}
		properties, largest, hasLargest = mergeTrackStatus(oks, properties, largest, hasLargest)
	}

	reply := &message.TrackStatusOK{}
	// §10.2.21: INCLUDE_PROPERTIES=0 empties the Track Properties only.
	if includeProperties(msg.Parameters) {
		reply.TrackProperties = properties
	}
	// §10.2.17: LARGEST_OBJECT only once Objects have been published.
	if hasLargest {
		reply.Parameters = message.Parameters{message.LargestObjectParam(largest.Group, largest.Object)}
	}
	// AcceptTrackStatus FINs after the reply (§10.15).
	if err := req.AcceptTrackStatus(reply); err != nil {
		h.log.LogAttrs(ctx, slog.LevelDebug, "TRACK_STATUS_OK write failed",
			slog.String("err", err.Error()))
	}
}

// mergeTrackStatus combines TRACK_STATUS_OKs with what the relay already
// knows of the track, as a SUBSCRIBE_OK's would be: its Track Properties if
// any, else the first answer's (§9.6), and the largest LARGEST_OBJECT
// (§10.2.17).
func mergeTrackStatus(
	oks []*message.TrackStatusOK,
	properties []byte,
	largest message.Location,
	hasLargest bool,
) ([]byte, message.Location, bool) {
	for _, ok := range oks {
		if len(properties) == 0 {
			properties = ok.TrackProperties
		}
		p, found := ok.Parameters.Find(message.ParamLargestObject)
		if l := (message.Location{Group: p.Group, Object: p.Object}); found && (!hasLargest || largest.Less(l)) {
			largest, hasLargest = l, true
		}
	}
	return properties, largest, hasLargest
}

// rejectTrackStatus answers a TRACK_STATUS no candidate accepted: with the
// refusal SUBSCRIBE would give for err (see [upstreamRejection]), or
// DOES_NOT_EXIST when there was no candidate.
func (h *sessionHandler) rejectTrackStatus(ctx context.Context, req *session.Request, err error) {
	rej := &session.RequestRejectedError{Code: moqt.RequestDoesNotExist, Reason: "relay: track not known"}
	if err != nil {
		rej = upstreamRejection(err)
		rej.Reason = "relay: no upstream for track: " + err.Error()
	}
	if werr := req.Reject(rej); werr != nil && !errors.Is(werr, context.Canceled) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "TRACK_STATUS reject write failed",
			slog.String("err", werr.Error()))
	}
}

// trackStatusUpstream forwards TRACK_STATUS for fullName, concurrently, to
// every candidate SUBSCRIBE would try (see [sessionHandler.subscribeUpstream]):
// each local publisher of a covering namespace and each relay Discovery
// resolves. It returns their TRACK_STATUS_OKs in that order and, when there
// are none, the highest-ranked refusal ([candidateErrRank]), nil when there
// was no candidate.
//
// trackStatusUpstreamTimeout bounds the whole forwarding, resolving the
// Discovery candidates included, and it all ends when the requester cancels
// (STOP_SENDING). A draining candidate is sent nothing
// (§10.4). Relay policy, as for SUBSCRIBE and FETCH: the requester's own
// session is skipped while a TRACK_STATUS for the track to it is in flight,
// since this request may be that one routed back, and a second would loop
// (§6.2).
func (h *sessionHandler) trackStatusUpstream(
	ctx context.Context,
	req *session.Request,
	fullName track.FullTrackName,
) ([]*message.TrackStatusOK, error) {
	timeout := trackStatusUpstreamTimeout
	if d := time.Duration(trackStatusTimeout.Load()); d > 0 {
		timeout = d
	}
	upCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer context.AfterFunc(req.Stream.Context(), cancel)()

	key := fullName.Key()
	var lastErr error
	fail := func(err error) {
		if candidateErrRank(err) > candidateErrRank(lastErr) {
			lastErr = err
		}
	}
	var candidates []*session.Session
	seen := map[*session.Session]bool{}
	add := func(sess *session.Session) {
		if seen[sess] {
			return
		}
		seen[sess] = true
		switch {
		case goingAway(sess):
			fail(errGoingAway)
		case sess == h.sess && h.tracks.RequestPending(message.TypeTrackStatus, sess, key):
		default:
			candidates = append(candidates, sess)
		}
	}
	for _, pub := range h.names.MatchPublishers(fullName.Namespace) {
		add(pub.Session)
	}
	remotes, draining := h.upstreams.resolveUpstreams(upCtx, fullName.Namespace)
	if draining {
		fail(errGoingAway)
	}
	for _, remote := range remotes {
		add(remote)
	}

	oks := make([]*message.TrackStatusOK, len(candidates))
	errs := make([]error, len(candidates))
	var wg sync.WaitGroup
	for i, sess := range candidates {
		done := h.tracks.BeginRequest(message.TypeTrackStatus, sess, key)
		wg.Go(func() {
			defer done()
			ts, err := sess.TrackStatus(upCtx, &message.TrackStatus{Namespace: fullName.Namespace, Name: fullName.Name})
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					err = &session.RequestRejectedError{
						Code:   moqt.RequestTimeout,
						Reason: "relay: TRACK_STATUS timed out",
					}
				}
				errs[i] = err
				return
			}
			_ = ts.Close()
			oks[i] = ts.OK
		})
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			fail(err)
		}
	}
	return slices.DeleteFunc(oks, func(ok *message.TrackStatusOK) bool { return ok == nil }), lastErr
}
