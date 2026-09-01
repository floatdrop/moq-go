package relay

import (
	"context"
	"errors"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// handleTrackStatus implements TRACK_STATUS (§10.15): a metadata-only query for
// a track's Properties and existence, answered without creating a subscription
// or round-tripping upstream. The reply is REQUEST_OK (aliased as
// [message.TrackStatusOK]) carrying the same Track Properties block SUBSCRIBE_OK
// would, plus §10.2.17 LARGEST_OBJECT when objects have been forwarded.
//
// It answers from the track registry when metadata exists, falls back to an
// empty TRACK_STATUS_OK when only the namespace is advertised locally, and
// otherwise rejects with [moqt.RequestDoesNotExist].
func (h *sessionHandler) handleTrackStatus(ctx context.Context, req *session.Request, msg *message.TrackStatus) {
	if err := h.auth.AuthorizeTrackStatus(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "TrackStatus", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}
	entry, known := h.tracks.Get(fullName.Key())
	// Answer TRACK_STATUS_OK for any entry with metadata to surface: Properties
	// or a §10.2.17 LargestObject watermark. Either field alone is useful.
	var (
		largest    message.Location
		hasLargest bool
	)
	if known {
		largest, hasLargest = entry.GetLargest()
	}
	hasProperties := known && len(entry.GetProperties()) > 0
	if known && (hasProperties || hasLargest) {
		reply := &message.TrackStatusOK{}
		if hasProperties {
			reply.TrackProperties = entry.GetProperties()
		}
		if hasLargest {
			// §10.2.17: omit LARGEST_OBJECT when no objects have
			// been observed; emit it (and the watermark) otherwise.
			reply.Parameters = append(reply.Parameters,
				message.LargestObjectParam(largest.Group, largest.Object))
		}
		if err := req.Reply(reply); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "TRACK_STATUS_OK write failed",
				slog.String("err", err.Error()))
		}
		// TRACK_STATUS is a one-shot RPC; the spec does not keep the
		// stream open for further messages (cf. §10.15). FIN the send
		// side now.
		_ = req.Stream.Close()
		return
	}

	// No local track entry with properties. Check the namespace
	// registry — if a publisher has advertised the namespace, the track
	// at least *might* exist, so we reply with an empty Properties block.
	// No LARGEST_OBJECT either: nothing has been observed.
	if len(h.names.MatchPublishers(msg.Namespace)) > 0 {
		if err := req.Reply(&message.TrackStatusOK{}); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "TRACK_STATUS_OK (empty) write failed",
				slog.String("err", err.Error()))
		}
		_ = req.Stream.Close()
		return
	}

	if err := req.RejectError(moqt.RequestDoesNotExist, "relay: track not known"); err != nil &&
		!errors.Is(err, context.Canceled) {
		h.log.LogAttrs(ctx, slog.LevelDebug, "TRACK_STATUS reject write failed",
			slog.String("err", err.Error()))
	}
}
