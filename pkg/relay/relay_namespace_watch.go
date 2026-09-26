package relay

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// runNamespaceWatch consumes [discovery.DiscoveryStore.WatchNamespaces] and
// records namespaces advertised by *other* relays in the namespace registry,
// which announces them to local SUBSCRIBE_NAMESPACE holders. It runs as one
// goroutine started in [Relay.Start] when Discovery is configured, until ctx
// is cancelled or the store is closed.
//
// A watch that fails to start is retried with backoff; one whose channel
// closes (the store's signal that this consumer fell behind) is restarted,
// and its snapshot reconciled via [registry.NamespaceRegistry.ReplaceRemote].
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
		r.log.LogAttrs(ctx, slog.LevelDebug, "discovery namespace watch started", slog.Bool("restart", !first))
		if !r.consumeNamespaceWatch(ctx, ch) {
			return
		}
	}
}

// consumeNamespaceWatch applies events from ch until it closes, reporting
// true, or ctx is cancelled, reporting false. The snapshot is applied as a
// whole at OpSnapshotDone, so a watch that ends before then applies nothing.
func (r *Relay) consumeNamespaceWatch(ctx context.Context, ch <-chan discovery.NamespaceEvent) bool {
	var (
		snapshot []discovery.NamespaceInfo
		synced   bool
	)
	for {
		select {
		case <-ctx.Done():
			return false
		case ev, ok := <-ch:
			if !ok {
				return true
			}
			switch {
			case synced:
				r.forwardNamespaceEvent(ctx, ev)
			case ev.Op == discovery.OpSnapshotDone:
				r.names.ReplaceRemote(snapshot)
				snapshot, synced = nil, true
			case ev.Op == discovery.OpPublish && ev.Info.RelayAddr != r.cfg.RelayAddr:
				// Own-relay advertisements are counted locally.
				snapshot = append(snapshot, ev.Info)
			}
		}
	}
}

// The watch restarts after these delays, doubling, when it fails to start.
const (
	namespaceWatchBackoffInitial = 100 * time.Millisecond
	namespaceWatchBackoffCap     = 10 * time.Second
)

// forwardNamespaceEvent records one remote namespace event in the namespace
// registry, counted together with local publishers of the same namespace
// (§10.18: NAMESPACE_DONE is per namespace). Own-relay events are skipped.
func (r *Relay) forwardNamespaceEvent(_ context.Context, ev discovery.NamespaceEvent) {
	if ev.Info.RelayAddr == r.cfg.RelayAddr {
		return // our own advertisement — already counted locally
	}
	switch ev.Op {
	case discovery.OpPublish:
		r.names.RemoteNamespace(ev.Info.Prefix, ev.Info.RelayAddr, true)
	case discovery.OpUnpublish:
		r.names.RemoteNamespace(ev.Info.Prefix, ev.Info.RelayAddr, false)
	case discovery.OpSnapshotDone:
		// Handled by consumeNamespaceWatch.
	}
}
