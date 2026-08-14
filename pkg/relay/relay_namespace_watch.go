package relay

import (
	"context"
	"log/slog"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// runNamespaceWatch consumes [discovery.DiscoveryStore.WatchNamespaces] and
// forwards namespaces advertised by *other* relays to this relay's local
// SUBSCRIBE_NAMESPACE holders. It is the consume-side mirror of the advertise
// side in [registry.NamespaceRegistry]: that publishes local PUBLISH_NAMESPACE into the
// store; this reflects remote advertisements back out as NAMESPACE /
// NAMESPACE_DONE so a downstream subscriber discovers namespaces served
// elsewhere in the deployment — and can then SUBSCRIBE, which the on-demand
// cross-relay path resolves via FindNamespace.
//
// It runs as a single relay-level goroutine started in [Relay.Start] (only when
// Discovery is configured) and returns when ctx is cancelled or the store
// closes its watch channel.
//
// The watch yields an initial snapshot before following live changes (see
// [discovery.DiscoveryStore.WatchNamespaces]), so this goroutine observes
// namespaces advertised before it started, not just later ones. The remaining
// limitation is downstream of here: it starts once in [Relay.Start] and
// reflects each event only to the SUBSCRIBE_NAMESPACE holders registered at the
// moment it arrives, so a subscriber that registers later is not back-filled
// with already-advertised namespaces. That subscriber still discovers them on
// demand — its SUBSCRIBE resolves via FindNamespace — so this is a
// reflection-latency gap, not a correctness one.
func (r *Relay) runNamespaceWatch(ctx context.Context) {
	ch, err := r.cfg.Discovery.WatchNamespaces(ctx)
	if err != nil {
		r.log.LogAttrs(ctx, slog.LevelWarn, "discovery: WatchNamespaces failed",
			slog.String("err", err.Error()))
		return
	}
	r.log.LogAttrs(ctx, slog.LevelDebug, "discovery namespace watch started")
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			r.forwardNamespaceEvent(ctx, ev)
		}
	}
}

// forwardNamespaceEvent reflects one remote namespace event to local
// SUBSCRIBE_NAMESPACE holders whose prefix matches.
//
// Own-relay events are skipped: [sessionHandler.handlePublishNamespace] already
// forwards a local PUBLISH_NAMESPACE to matching subscribers, so re-forwarding
// the same advertisement from the watch would duplicate the NAMESPACE message.
// SUBSCRIBE_TRACKS holders (WantsTracks) are skipped too — they receive
// forwarded PUBLISH messages, not NAMESPACE, and a relay cannot synthesize a
// remote PUBLISH from a namespace advertisement alone.
func (r *Relay) forwardNamespaceEvent(ctx context.Context, ev discovery.NamespaceEvent) {
	if ev.Info.RelayAddr == r.cfg.RelayAddr {
		return // our own advertisement — already forwarded locally
	}
	ns := ev.Info.Prefix
	for _, sub := range r.names.MatchSubscribers(ns) {
		if sub.WantsTracks {
			continue
		}
		// Reuse the same suffix-stripping helpers handlePublishNamespace uses
		// so the wire form is identical whether the namespace is local or
		// remote (§10.16 NAMESPACE and §10.17 NAMESPACE_DONE both carry only the
		// bytes beyond the subscriber prefix).
		var err error
		switch ev.Op {
		case discovery.OpPublish:
			err = sub.WriteMessage(namespaceMessageFor(ns, sub.Prefix))
		case discovery.OpUnpublish:
			err = sub.WriteMessage(namespaceDoneMessageFor(ns, sub.Prefix))
		default:
			continue
		}
		if err != nil {
			r.log.LogAttrs(ctx, slog.LevelDebug, "discovery NAMESPACE forward failed",
				slog.String("op", ev.Op.String()),
				slog.String("err", err.Error()))
		}
	}
}
