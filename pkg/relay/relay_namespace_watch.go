package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

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
// namespaces advertised before it started, not just later ones. Each event is
// recorded in the namespace registry, which also seeds a SUBSCRIBE_NAMESPACE
// holder that registers later. A watch that fails to start is retried, and one
// whose channel closes is restarted, its snapshot replacing the remote state.
// An event the store drops (MemoryStore does, for a slow consumer) is not
// recovered until the next restart.
func (r *Relay) runNamespaceWatch(ctx context.Context) {
	backoff := namespaceWatchBackoffInitial
	for first := true; ; first = false {
		if ctx.Err() != nil {
			return // shutting down: the watch closing is not a failure
		}
		ch, err := r.cfg.Discovery.WatchNamespaces(ctx)
		if errors.Is(err, discovery.ErrClosed) {
			return // "After Close all methods return ErrClosed"
		}
		if err != nil {
			r.log.LogAttrs(ctx, slog.LevelWarn, "discovery: WatchNamespaces failed",
				slog.String("err", err.Error()), slog.Duration("retry_in", backoff))
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(2*backoff, namespaceWatchBackoffCap)
			continue
		}
		backoff = namespaceWatchBackoffInitial
		if !first {
			// A restarted watch begins with a fresh snapshot; drop what the
			// old one reported so namespaces withdrawn in between do not
			// linger. Subscribers see NAMESPACE_DONE then NAMESPACE again
			// for every remote namespace that still exists.
			r.names.ResetRemote()
		}
		r.log.LogAttrs(ctx, slog.LevelDebug, "discovery namespace watch started")
		if !r.consumeNamespaceWatch(ctx, ch) {
			return
		}
	}
}

// consumeNamespaceWatch forwards events from ch until it closes, reporting
// true, or ctx is cancelled, reporting false.
func (r *Relay) consumeNamespaceWatch(ctx context.Context, ch <-chan discovery.NamespaceEvent) bool {
	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-ch:
			if !ok {
				return true
			}
			r.forwardNamespaceEvent(ctx, ev)
		}
	}
}

// The watch restarts after these delays, doubling, when it fails to start.
const (
	namespaceWatchBackoffInitial = 100 * time.Millisecond
	namespaceWatchBackoffCap     = 10 * time.Second
)

// forwardNamespaceEvent records one remote namespace event in the namespace
// registry, which announces it to local SUBSCRIBE_NAMESPACE holders whose
// prefix matches, counted together with local publishers of the same
// namespace (§10.18: NAMESPACE_DONE is per namespace).
//
// Own-relay events are skipped: the registry already counts this relay's local
// PUBLISH_NAMESPACE registrations, which are what it advertised.
func (r *Relay) forwardNamespaceEvent(_ context.Context, ev discovery.NamespaceEvent) {
	if ev.Info.RelayAddr == r.cfg.RelayAddr {
		return // our own advertisement — already counted locally
	}
	switch ev.Op {
	case discovery.OpPublish:
		r.names.RemoteNamespace(ev.Info.Prefix, ev.Info.RelayAddr, true)
	case discovery.OpUnpublish:
		r.names.RemoteNamespace(ev.Info.Prefix, ev.Info.RelayAddr, false)
	}
}
