package nats

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// WatchTracks delivers the current track advertisements as an OpPublish
// snapshot, then streams every subsequent Publish/Unpublish observed on the
// bucket. The returned channel closes when ctx is cancelled or the store closes.
//
// The snapshot→follow handoff is gapless: the KV watcher replays each matching
// key's current value, marks the end of the snapshot with a nil sentinel, then
// follows live from the same subscription, so nothing between the two is missed
// or duplicated. Snapshot events are delivered with a blocking, cancellable send
// (nothing back-pressures the source yet); after the snapshot, delivery is
// non-blocking and drops on a full buffer with a logged warning, per the
// slow-consumer contract. The liveness heartbeat's unchanged re-Puts are
// suppressed (see [Store.watchPump]).
func (s *Store) WatchTracks(ctx context.Context) (<-chan discovery.TrackEvent, error) {
	return startWatch(ctx, s, watchCodec[discovery.TrackEvent]{
		filter:    trackFilter,
		publish:   s.trackEvent(discovery.OpPublish),
		unpublish: s.trackEvent(discovery.OpUnpublish),
	})
}

// WatchNamespaces — same snapshot-then-follow contract as [Store.WatchTracks],
// over namespace events.
func (s *Store) WatchNamespaces(ctx context.Context) (<-chan discovery.NamespaceEvent, error) {
	return startWatch(ctx, s, watchCodec[discovery.NamespaceEvent]{
		filter:    nsFilter,
		publish:   s.namespaceEvent(discovery.OpPublish),
		unpublish: s.namespaceEvent(discovery.OpUnpublish),
	})
}

// watchCodec adapts the generic pump to a concrete event type: filter is the
// subject subtree to watch; publish decodes a stored value into an event with
// the given Op for a live/snapshot Put; unpublish decodes the last-seen value
// (KV delete markers carry no payload) into a removal event.
type watchCodec[T any] struct {
	filter    string
	publish   func(value []byte) (T, bool)
	unpublish func(value []byte) (T, bool)
}

// trackEvent returns a decoder that wraps a stored track value in a TrackEvent
// with op. An undecodable value is logged and skipped.
func (s *Store) trackEvent(op discovery.Op) func([]byte) (discovery.TrackEvent, bool) {
	return func(value []byte) (discovery.TrackEvent, bool) {
		info, err := decodeTrack(value)
		if err != nil {
			s.log.Warn("nats discovery: undecodable track", "op", op.String(), "err", err)
			return discovery.TrackEvent{}, false
		}
		return discovery.TrackEvent{Op: op, Info: info}, true
	}
}

// namespaceEvent — see [Store.trackEvent].
func (s *Store) namespaceEvent(op discovery.Op) func([]byte) (discovery.NamespaceEvent, bool) {
	return func(value []byte) (discovery.NamespaceEvent, bool) {
		info, err := decodeNamespace(value)
		if err != nil {
			s.log.Warn("nats discovery: undecodable namespace", "op", op.String(), "err", err)
			return discovery.NamespaceEvent{}, false
		}
		return discovery.NamespaceEvent{Op: op, Info: info}, true
	}
}

// startWatch creates the underlying KV watcher synchronously — so every event
// after this returns is captured, with no window between the caller receiving
// the channel and the watch taking effect — then drives the delivery pump in the
// background. The channel closes when ctx is cancelled or the store is closed.
func startWatch[T any](ctx context.Context, s *Store, c watchCodec[T]) (<-chan T, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	wctx, cancel := context.WithCancel(ctx)
	w, err := s.kv.Watch(wctx, c.filter)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("nats discovery: watch %s: %w", c.filter, err)
	}
	out := make(chan T, s.bufferSize)
	go watchPump(wctx, s, cancel, w, out, c)
	return out, nil
}

// watchPump drains one watcher onto out. It keeps a per-key cache of the last
// value it delivered, which serves two purposes: it suppresses the liveness
// heartbeat's unchanged re-Puts (a Put whose value equals the cached one emits
// nothing), and it supplies the pre-removal value for OpUnpublish, since
// JetStream KV delete/expiry markers carry no payload. It closes out, stops the
// watcher, and cancels the watch ctx on return.
func watchPump[T any](
	ctx context.Context,
	s *Store,
	cancel context.CancelFunc,
	w jetstream.KeyWatcher,
	out chan T,
	c watchCodec[T],
) {
	defer close(out)
	defer cancel()
	defer w.Stop() //nolint:errcheck // teardown of a watcher we are done with

	seen := make(map[string]string) // KV key -> last delivered value
	snapshotDone := false
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case e, ok := <-w.Updates():
			if !ok {
				return // watcher closed (Stop or connection loss)
			}
			if e == nil {
				snapshotDone = true // end-of-initial-values sentinel
				continue
			}
			ev, emit := decodeEvent(seen, e, c)
			if !emit {
				continue
			}
			if !deliver(ctx, s, out, ev, snapshotDone, c.filter) {
				return
			}
		}
	}
}

// decodeEvent turns one raw KV entry into an event to emit, updating seen. A Put
// whose value is unchanged from the cache (a heartbeat) yields emit=false. A
// delete/purge marker for a key never seen also yields emit=false — there is
// nothing to retract.
func decodeEvent[T any](seen map[string]string, e jetstream.KeyValueEntry, c watchCodec[T]) (T, bool) {
	var zero T
	switch e.Operation() {
	case jetstream.KeyValuePut:
		v := string(e.Value())
		if prev, ok := seen[e.Key()]; ok && prev == v {
			return zero, false // unchanged re-Put (heartbeat) — suppress
		}
		seen[e.Key()] = v
		return c.publish(e.Value())
	case jetstream.KeyValueDelete, jetstream.KeyValuePurge:
		prev, ok := seen[e.Key()]
		if !ok {
			return zero, false // never delivered a value for this key
		}
		delete(seen, e.Key())
		return c.unpublish([]byte(prev))
	}
	return zero, false
}

// deliver sends one event. During the snapshot it blocks until the consumer
// accepts it or the watch is torn down (losing a snapshot event would leave the
// consumer's view incomplete, and blocking is safe because nothing
// back-pressures the source yet). After the snapshot it is non-blocking, dropping
// on a full buffer with a warning per the slow-consumer contract. Returns false
// if the watch ended mid-send, signalling the pump to stop.
func deliver[T any](ctx context.Context, s *Store, out chan T, ev T, snapshotDone bool, filter string) bool {
	if !snapshotDone {
		select {
		case out <- ev:
			return true
		case <-ctx.Done():
			return false
		case <-s.done:
			return false
		}
	}
	select {
	case out <- ev:
	default:
		s.log.WarnContext(ctx, "nats discovery: dropped event on slow watcher", "filter", filter)
	}
	return true
}
