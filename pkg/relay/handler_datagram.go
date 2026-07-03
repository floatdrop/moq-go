package relay

import (
	"context"
	"errors"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// runDatagramLoop is the datagram fanout entry point. It pulls
// [message.ObjectDatagram]s off the session and forwards each to every
// downstream subscriber whose §5.1.2 filter passes, with the Track Alias
// remapped to that subscriber's per-session outbound alias.
//
// Per §11.3 a datagram is a fire-and-forget delivery — the underlying
// transport drops oversized or unschedulable datagrams without notification.
// The loop therefore swallows per-send failures: there is no slow-reader
// escalation analogous to the subgroup path, and there is no
// stream-lifecycle propagation — each datagram is its own self-contained
// §11.4.3-style "stream".
//
// Termination:
//
//   - Transport-level errors from [session.Session.ReceiveDatagram]
//     (session closed, ctx cancelled, PROTOCOL_VIOLATION on parse) end
//     the loop and propagate to [sessionHandler.run]'s aggregator.
//   - Per-datagram lookup misses (unknown Track Alias, evicted track entry)
//     drop the datagram silently — §11.3 explicitly permits this.
func (h *sessionHandler) runDatagramLoop(ctx context.Context) error {
	for {
		d, err := h.sess.ReceiveDatagram(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return err
		}
		h.handleDatagram(ctx, d)
	}
}

// handleDatagram is the per-datagram fanout. It mirrors [runFanout]'s
// per-object body but with a flat structure: no per-subscriber writer
// goroutine, no §11.4.3 stream-lifecycle bookkeeping, no ObjectIDDelta
// re-encoding (datagrams carry an absolute Object ID, §11.3.1).
func (h *sessionHandler) handleDatagram(ctx context.Context, d *message.ObjectDatagram) {
	key, ok := h.sess.LookupInboundTrackAlias(d.TrackAlias)
	if !ok {
		// §11.3: an unknown Track Alias MAY be dropped or briefly buffered for
		// reordering against the establishing control message. We drop.
		h.log.LogAttrs(ctx, slog.LevelDebug, "datagram: unknown inbound Track Alias",
			slog.Uint64("alias", d.TrackAlias))
		return
	}

	entry, ok := h.tracks.Get(key)
	if !ok {
		h.log.LogAttrs(ctx, slog.LevelDebug, "datagram: track entry gone",
			slog.Uint64("alias", d.TrackAlias))
		return
	}

	// §2.1 dedup across redundant upstream publishers, same ledger as the
	// subgroup path (handler_fanout): the first copy of {GroupID, ObjectID}
	// wins; later copies from peer upstreams are dropped so each subscriber
	// sees the object exactly once — and the loser neither re-caches nor
	// re-bumps the watermark.
	if !entry.ClaimDelivered(d.GroupID, d.ObjectID) {
		return
	}

	// §10.2.11: a forwarded datagram counts towards the track's
	// LARGEST_OBJECT watermark just like a subgroup object does.
	entry.UpdateLargest(message.Location{Group: d.GroupID, Object: d.ObjectID})

	// Cache via the per-track ObjectCache. The cache retains the payload +
	// properties BY REFERENCE (see cache.PutDatagram); ReceiveDatagram
	// hands out caller-owned buffers, so nothing here mutates them after
	// the Put.
	entry.Cache.PutDatagram(d)

	downstream := entry.CopyDownstream()
	for _, sub := range downstream {
		if !sub.IsEstablished() {
			continue
		}
		// One lock acquisition folds the §9.2 Forward-State gate and the
		// §5.1.2 filter test, exactly like the subgroup fanout: a paused
		// subscription (Forward State 0) receives no datagrams. There is
		// no per-datagram stream to reset, so the groupExhausted signal
		// is irrelevant here.
		forward, _ := sub.ForwardDecision(d.GroupID, d.ObjectID)
		if !forward {
			continue
		}
		// Re-encode the datagram with the subscriber's outbound
		// Track Alias. Per §9.7 the relay does not modify any other
		// object fields — Type, Group ID, Object ID, Priority,
		// Properties, Status, Payload all forward verbatim.
		out := *d
		out.TrackAlias = sub.TrackAlias
		if err := sub.Session.SendDatagram(&out); err != nil {
			// Per §11.3 datagrams may be dropped silently when
			// the transport can't deliver them; treat send errors
			// the same way and log at Debug for postmortem.
			h.log.LogAttrs(ctx, slog.LevelDebug, "datagram: SendDatagram failed",
				slog.Uint64("sub_id", sub.ID),
				slog.String("err", err.Error()))
		}
	}
}
