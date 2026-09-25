package relay

import (
	"context"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// endMalformedTrack is the relay's response to a malformed track (§2.4.2)
// detected in an Object that src sent on entry's track. As a relay it "MUST
// immediately terminate downstream subscriptions with PUBLISH_DONE [...] with
// Status Code MALFORMED_TRACK": every downstream subscription, whichever
// upstream fed it, since the track is malformed. As a subscriber it "MUST
// cancel any corresponding subscription or fetches for that Track from that
// publisher": the upstream subscriptions on src only, so a redundant publisher
// (§9.5) keeps serving later subscribers. The Object is never cached — every
// caller detects it before the cache is written.
//
// The downstreams are terminated first: cancelling an upstream makes its owner
// unregister it, which terminates downstreams with the upstream's own code,
// and the first termination wins.
//
// Downstream fetch streams already serving the track are not reset: the relay
// does not track them per track.
func (h *sessionHandler) endMalformedTrack(
	ctx context.Context,
	entry *registry.TrackEntry,
	src *session.Session,
	cause error,
) {
	h.log.LogAttrs(ctx, slog.LevelInfo, "malformed track: ending it downstream and cancelling the upstream",
		slog.String("name", string(entry.FullName.Name)), slog.String("err", cause.Error()))
	for _, sub := range entry.CopyDownstream() {
		sub.TerminateWithPublishDone(moqt.PublishDoneMalformedTrack, "relay: malformed track")
	}
	for _, up := range entry.CopyUpstream() {
		if up.Session == src {
			up.Cancel(moqt.StreamResetMalformedTrack)
		}
	}
}
