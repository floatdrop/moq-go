package relay

import (
	"context"
	"errors"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay/cache"
	"github.com/floatdrop/moq-go/pkg/relay/internal/registry"
)

// runDatagramLoop forwards each received [message.ObjectDatagram] to the
// downstream subscribers, until a session-level receive error, which it
// returns. Send failures and lookup misses drop the datagram (§11.2); a
// malformed track is ended (§12.1) and the loop reads on.
func (h *sessionHandler) runDatagramLoop(ctx context.Context) error {
	for {
		d, err := h.sess.ReceiveDatagram(ctx)
		if errors.Is(err, session.ErrMalformedTrack) {
			// Per-track, not per-session: end that track and read on.
			if in, ok := h.sess.LookupInboundTrack(d.TrackAlias); ok {
				if entry, ok := h.tracks.Get(in.Key); ok {
					h.endMalformedTrack(ctx, entry, h.sess, err)
				}
			}
			continue
		}
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return ctx.Err()
			}
			return err
		}
		h.handleDatagram(ctx, d)
	}
}

// handleDatagram is the per-datagram counterpart of [runFanout]'s
// per-object body.
func (h *sessionHandler) handleDatagram(ctx context.Context, d *message.ObjectDatagram) {
	in, ok := h.sess.LookupInboundTrack(d.TrackAlias)
	if !ok {
		// §3.1.3.1: an unknown Track Alias MAY be dropped.
		h.log.LogAttrs(ctx, slog.LevelDebug, "datagram: unknown inbound Track Alias",
			slog.Uint64("alias", d.TrackAlias))
		return
	}

	entry, ok := h.tracks.Get(in.Key)
	if !ok {
		h.log.LogAttrs(ctx, slog.LevelDebug, "datagram: track entry gone",
			slog.Uint64("alias", d.TrackAlias))
		return
	}

	// §11.2.1: resolve the inherited DEFAULT_PUBLISHER_PRIORITY (§10.4) for
	// the cache and PRIORITY_FILTER; the forwarded Type still omits the byte.
	if d.HasDefaultPriority() {
		d.PublisherPriority = in.DefaultPublisherPriority
	}

	// §7.3: the first copy of {GroupID, ObjectID} wins, unless an announced
	// gap says it does not exist (§2.1.2, §7.1).
	info := registry.ObjectInfo{
		Group:      d.GroupID,
		Object:     d.ObjectID,
		Datagram:   true,
		Priority:   d.PublisherPriority,
		Status:     d.ObjectStatus,
		EndOfGroup: d.HasEndOfGroup(),
		Gaps:       message.ObjectPriorGaps(d.Properties),
	}
	claim, err := entry.ClaimDelivered(info)
	if err != nil {
		h.endMalformedTrack(ctx, entry, h.sess, err)
		return
	}
	// An aged-out datagram is dropped like a redundant one: it has no stream
	// to reset (§7.4 covers multi-object streams).
	if claim != registry.ClaimFresh {
		if err := recordRedundant(entry, info, &cache.CachedObject{
			GroupID:           d.GroupID,
			ObjectID:          d.ObjectID,
			PublisherPriority: d.PublisherPriority,
			ForwardingPref:    cache.ForwardingDatagram,
			Status:            d.ObjectStatus,
			Properties:        d.Properties,
			Payload:           d.ObjectPayload,
		}); err != nil {
			h.endMalformedTrack(ctx, entry, h.sess, err)
		}
		return
	}

	// The cache keeps the buffers by reference; nothing mutates them after.
	entry.Cache.PutDatagram(d, in.MaxCacheDuration, in.HasMaxCacheDuration)

	// §9.20.17. After the cache, so a FETCH that sees the new Largest Object
	// finds the datagram there: a datagram arrives whole, so its first byte
	// and its last are the same moment (§3.1.4).
	entry.UpdateLargest(message.Location{Group: d.GroupID, Object: d.ObjectID})

	downstream := entry.CopyDownstream()
	for _, sub := range downstream {
		// A datagram belongs to no Subgroup (§2.2), and §3.3.2 does not say
		// how a SUBGROUP_FILTER treats one; the relay filters it as Subgroup 0.
		if sub.ForwardDecision(d.GroupID, d.ObjectID, 0, d.PublisherPriority, d.Properties) != registry.Forward {
			continue
		}
		// §7.8: only the Track Alias changes, bar the exception below.
		out := *d
		out.TrackAlias = sub.TrackAlias
		// A subscriber without Track Properties (§9.20.21) cannot inherit
		// DEFAULT_PUBLISHER_PRIORITY, so the priority is written out.
		if !sub.IncludesProperties() {
			out.Type &^= message.DatagramDefaultPriorityBit
		}
		// §9.9: PUBLISH_DONE waits for a send in progress, and none starts
		// after it.
		if !sub.BeginDatagram() {
			continue
		}
		err := sub.Session.SendDatagram(&out)
		sub.EndDatagram()
		if err != nil {
			// §11.2: datagrams may be dropped.
			h.log.LogAttrs(ctx, slog.LevelDebug, "datagram: SendDatagram failed",
				slog.Uint64("sub_id", sub.ID),
				slog.String("err", err.Error()))
		}
	}
}
