package relay

import (
	"context"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// endMalformedTrack handles a malformed track (§2.4.2) detected in an Object
// src sent on entry's track: every downstream subscription ends with
// PUBLISH_DONE MALFORMED_TRACK, and only the upstreams on src are cancelled,
// so a redundant publisher (§9.3) keeps serving. Callers never cache the
// Object. Open subgroup streams are reset now, since PUBLISH_DONE waits for
// them (§10.12). Downstream fetch streams are not reset.
//
// Downstreams are terminated before the upstream is cancelled: the first
// termination wins, and the upstream's teardown would use its own code.
//
// Interpretation: the upstream cancel also uses MALFORMED_TRACK, which §3.3.4
// defines for the downstream direction.
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
	// Info once per upstream; later detections are Debug.
	level := slog.LevelDebug
	if cancelled > 0 {
		level = slog.LevelInfo
	}
	h.log.LogAttrs(ctx, level, "malformed track: ended it downstream, cancelled the upstream",
		slog.String("name", string(entry.FullName.Name)), slog.String("err", cause.Error()))
}

// resetWriters resets the open subgroup writers of subs on entry with
// MALFORMED_TRACK. The nil slot keeps the joiner scan from reopening one.
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
