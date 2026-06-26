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

// handleTrackStatus implements TRACK_STATUS (§10.14). It is a metadata-only
// query: the caller wants the track's Properties (and, implicitly, its
// existence) without creating a subscription. Per the spec the response is
// REQUEST_OK (aliased as [message.TrackStatusOK]) carrying the same Track
// Properties block that SUBSCRIBE_OK would.
//
// Flow:
//
//  1. Authorize.
//  2. Look up the track in [registry.TrackRegistry]. If found and a publisher has
//     populated Properties (handlePublish / subscribeUpstream both do
//     this), reply TRACK_STATUS_OK with those bytes plus a §10.2.11
//     LARGEST_OBJECT parameter sourced from the entry's watermark when
//     any object has been forwarded.
//  3. Otherwise check the [registry.NamespaceRegistry] — if a local publisher has
//     advertised the namespace, reply TRACK_STATUS_OK with empty
//     Properties (and no LARGEST_OBJECT). The caller learns the track
//     exists but the relay has no metadata to forward without issuing an
//     upstream SUBSCRIBE.
//  4. If neither is true, reject with [moqt.RequestDoesNotExist].
//
// The handler deliberately does NOT issue an upstream TRACK_STATUS on
// demand. TRACK_STATUS is a status query, not a subscription — the
// answer the relay can give without round-tripping upstream is sufficient
// for callers that just want to know "does this track exist?".
func (h *sessionHandler) handleTrackStatus(ctx context.Context, req *session.Request, msg *message.TrackStatus) {
	if err := h.auth.AuthorizeTrackStatus(ctx, h.sess, msg); err != nil {
		h.rejectAuth(ctx, req, "TrackStatus", err)
		return
	}

	fullName := track.FullTrackName{Namespace: msg.Namespace, Name: msg.Name}
	entry, known := h.tracks.Get(fullName.Key())
	// The relay can answer TRACK_STATUS_OK for any entry it has metadata
	// to surface: either Properties (captured from the upstream publisher
	// in PUBLISH / SUBSCRIBE_OK) or a §10.2.11 LargestObject watermark
	// (the fanout advances this on every forwarded object). Either field
	// — even on its own — is useful to the caller.
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
			// §10.2.11: omit LARGEST_OBJECT when no objects have
			// been observed; emit it (and the watermark) otherwise.
			reply.Parameters = append(reply.Parameters,
				message.LargestObjectParam(largest.Group, largest.Object))
		}
		if err := req.Reply(reply); err != nil {
			h.log.LogAttrs(ctx, slog.LevelDebug, "TRACK_STATUS_OK write failed",
				slog.String("err", err.Error()))
		}
		// TRACK_STATUS is a one-shot RPC; the spec does not keep the
		// stream open for further messages (cf. §10.14). FIN the send
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
