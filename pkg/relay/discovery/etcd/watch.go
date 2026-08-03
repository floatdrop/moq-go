package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// WatchTracks delivers the current track advertisements under this store's
// track prefix as an OpPublish snapshot, then streams every subsequent
// Publish/Unpublish the etcd cluster observes there. The returned channel
// closes when ctx is cancelled or the store is closed.
//
// The snapshot→follow handoff is gapless: the snapshot is read at a single etcd
// revision and the follow Watch starts at exactly the next revision (WithRev),
// so no event between the two is missed or duplicated. Snapshot events are
// delivered in full (a blocking, cancellable send — the follow Watch is not yet
// being read, so nothing back-pressures etcd); after the snapshot, delivery is
// non-blocking per the slow-consumer contract, dropping with a logged warning
// rather than stalling the etcd watch loop.
func (s *Store) WatchTracks(ctx context.Context) (<-chan discovery.TrackEvent, error) {
	return startWatch(ctx, s, watchCodec[discovery.TrackEvent]{
		dir:      s.root + "t/",
		snapshot: s.trackSnapshot,
		event:    s.trackEvent,
	})
}

// WatchNamespaces — same snapshot-then-follow contract as [Store.WatchTracks],
// over namespace events.
func (s *Store) WatchNamespaces(ctx context.Context) (<-chan discovery.NamespaceEvent, error) {
	return startWatch(ctx, s, watchCodec[discovery.NamespaceEvent]{
		dir:      s.root + "n/",
		snapshot: s.namespaceSnapshot,
		event:    s.namespaceEvent,
	})
}

// watchCodec adapts the generic snapshot-then-follow machinery to a concrete
// event type: dir is the key subtree to snapshot and follow; snapshot turns a
// stored value into an OpPublish event; event turns a raw watch event into a
// publish/unpublish event.
type watchCodec[T any] struct {
	dir      string
	snapshot func(value []byte) (T, bool)
	event    func(ev *clientv3.Event) (T, bool)
}

// startWatch opens the out channel, binds a cancellable child context, and
// drives the snapshot-then-follow [pump] on it in the background. The channel
// closes when ctx is cancelled or the store is closed.
func startWatch[T any](ctx context.Context, s *Store, c watchCodec[T]) (<-chan T, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	out := make(chan T, s.bufferSize)
	wctx, cancel := context.WithCancel(ctx)
	go pump(wctx, s, cancel, out, c)
	return out, nil
}

// pump implements the snapshot-then-follow watch shared by WatchTracks and
// WatchNamespaces. It reads the current subtree at one revision, emits it as
// OpPublish events, then follows from exactly the next revision (WithRev) so the
// handoff is gapless. Snapshot delivery blocks (nothing back-pressures etcd
// yet); follow delivery is non-blocking and drops on a full buffer per the
// slow-consumer contract. It closes out and cancels the watch ctx on return.
func pump[T any](ctx context.Context, s *Store, cancel context.CancelFunc, out chan T, c watchCodec[T]) {
	defer close(out)
	defer cancel()

	resp, err := s.cli.Get(ctx, c.dir, clientv3.WithPrefix())
	if err != nil {
		s.log.WarnContext(ctx, "etcd discovery: watch snapshot failed", "dir", c.dir, "err", err)
		return
	}
	for _, kv := range resp.Kvs {
		ev, ok := c.snapshot(kv.GetValue())
		if !ok {
			continue
		}
		if !sendSnapshot(ctx, s.done, out, ev) {
			return
		}
	}

	wch := s.cli.Watch(ctx, c.dir,
		clientv3.WithPrefix(), clientv3.WithPrevKV(), clientv3.WithRev(resp.Header.GetRevision()+1))
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case wresp, ok := <-wch:
			if !ok || wresp.Canceled {
				return
			}
			for _, ev := range wresp.Events {
				e, ok := c.event(ev)
				if !ok {
					continue
				}
				select {
				case out <- e:
				default:
					s.log.WarnContext(ctx, "etcd discovery: dropped event on slow watcher", "dir", c.dir)
				}
			}
		}
	}
}

// sendSnapshot delivers one initial-snapshot event, blocking until the consumer
// accepts it or the watch is torn down (ctx cancelled / store closed). Snapshot
// events must not be dropped — losing one would leave the consumer's view of
// current state incomplete — and blocking here is safe because the follow Watch
// is not yet being read, so nothing back-pressures etcd. Returns false if the
// watch ended before delivery, signalling the pump to stop.
func sendSnapshot[T any](ctx context.Context, done <-chan struct{}, out chan T, ev T) bool {
	select {
	case out <- ev:
		return true
	case <-ctx.Done():
		return false
	case <-done:
		return false
	}
}

// trackSnapshot decodes one stored track value into an OpPublish snapshot
// event; an undecodable value is logged and skipped.
func (s *Store) trackSnapshot(value []byte) (discovery.TrackEvent, bool) {
	info, err := decodeTrack(value)
	if err != nil {
		s.log.Warn("etcd discovery: undecodable track in snapshot", "err", err)
		return discovery.TrackEvent{}, false
	}
	return discovery.TrackEvent{Op: discovery.OpPublish, Info: info}, true
}

// namespaceSnapshot — see [Store.trackSnapshot].
func (s *Store) namespaceSnapshot(value []byte) (discovery.NamespaceEvent, bool) {
	info, err := decodeNamespace(value)
	if err != nil {
		s.log.Warn("etcd discovery: undecodable namespace in snapshot", "err", err)
		return discovery.NamespaceEvent{}, false
	}
	return discovery.NamespaceEvent{Op: discovery.OpPublish, Info: info}, true
}

// trackEvent converts a raw etcd watch event into a discovery.TrackEvent. A PUT
// decodes the current value; a DELETE decodes PrevKv (populated by WithPrevKV)
// since the storage key is a one-way hash of the track and cannot itself be
// reversed to a FullTrackName. Undecodable events are logged and skipped.
func (s *Store) trackEvent(ev *clientv3.Event) (discovery.TrackEvent, bool) {
	switch ev.Type {
	case clientv3.EventTypePut:
		info, err := decodeTrack(ev.Kv.GetValue())
		if err != nil {
			s.log.Warn("etcd discovery: undecodable track put", "err", err)
			return discovery.TrackEvent{}, false
		}
		return discovery.TrackEvent{Op: discovery.OpPublish, Info: info}, true
	case clientv3.EventTypeDelete:
		if ev.PrevKv == nil {
			return discovery.TrackEvent{}, false
		}
		info, err := decodeTrack(ev.PrevKv.GetValue())
		if err != nil {
			s.log.Warn("etcd discovery: undecodable track delete", "err", err)
			return discovery.TrackEvent{}, false
		}
		return discovery.TrackEvent{Op: discovery.OpUnpublish, Info: info}, true
	}
	return discovery.TrackEvent{}, false
}

func (s *Store) namespaceEvent(ev *clientv3.Event) (discovery.NamespaceEvent, bool) {
	switch ev.Type {
	case clientv3.EventTypePut:
		info, err := decodeNamespace(ev.Kv.GetValue())
		if err != nil {
			s.log.Warn("etcd discovery: undecodable namespace put", "err", err)
			return discovery.NamespaceEvent{}, false
		}
		return discovery.NamespaceEvent{Op: discovery.OpPublish, Info: info}, true
	case clientv3.EventTypeDelete:
		if ev.PrevKv == nil {
			return discovery.NamespaceEvent{}, false
		}
		info, err := decodeNamespace(ev.PrevKv.GetValue())
		if err != nil {
			s.log.Warn("etcd discovery: undecodable namespace delete", "err", err)
			return discovery.NamespaceEvent{}, false
		}
		return discovery.NamespaceEvent{Op: discovery.OpUnpublish, Info: info}, true
	}
	return discovery.NamespaceEvent{}, false
}
