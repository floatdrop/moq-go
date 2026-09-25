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
// The terminated subscriptions' open subgroup streams are reset with
// MALFORMED_TRACK now: PUBLISH_DONE waits for them (§10.12), and one left
// open by an idle upstream stream would hold it back indefinitely.
//
// The downstreams are terminated before the upstream is cancelled:
// cancelling makes its owner unregister it, which terminates downstreams with
// the upstream's own code, and the first termination wins.
//
// Cancelling the upstream uses MALFORMED_TRACK too. §3.3.4 defines it for "A
// relay publisher detected that the track was malformed", the downstream
// direction; saying why the relay cancels is this relay's choice under "SHOULD
// use a relevant error code", where CANCELLED would say less.
//
// Downstream fetch streams already serving the track are not reset: the relay
// does not track them per track.
func (h *sessionHandler) endMalformedTrack(
	ctx context.Context,
	entry *registry.TrackEntry,
	src *session.Session,
	cause error,
) {
	downstream := entry.CopyDownstream()
	for _, sub := range downstream {
		sub.TerminateWithPublishDone(moqt.PublishDoneMalformedTrack, "relay: malformed track")
	}
	h.resetWriters(entry, downstream)

	cancelled := 0
	for _, up := range entry.CopyUpstream() {
		if up.Session == src {
			up.Cancel(moqt.StreamResetMalformedTrack)
			cancelled++
		}
	}
	// Once per upstream at Info; a publisher that keeps sending (datagrams,
	// other streams) after the cancel is logged at Debug.
	level := slog.LevelDebug
	if cancelled > 0 {
		level = slog.LevelInfo
	}
	h.log.LogAttrs(ctx, level, "malformed track: ended it downstream, cancelled the upstream",
		slog.String("name", string(entry.FullName.Name)), slog.String("err", cause.Error()))
}

// resetWriters resets the open subgroup writers of subs on entry with
// MALFORMED_TRACK. Each writer's slot is set to nil, which also keeps the
// joiner scan from reopening one for the same Subgroup. The writers drain in
// the background, joined by the handler's wait group.
func (h *sessionHandler) resetWriters(entry *registry.TrackEntry, subs []*registry.DownstreamSub) {
	if len(subs) == 0 {
		return
	}
	var ws []*subgroupWriter
	for _, sg := range entry.CopySubgroups() {
		set, _ := sg.Set.(*subgroupWriterSet)
		sg.Mu.Lock()
		for _, sub := range subs {
			if w := set.writers[sub]; w != nil {
				w.close(true, moqt.StreamResetMalformedTrack)
				set.writers[sub] = nil
				ws = append(ws, w)
			}
		}
		sg.Mu.Unlock()
	}
	if len(ws) > 0 {
		h.spawn(func() { joinWriters(ws) })
	}
}
