package etcd

import (
	"context"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// WatchTracks streams every track Publish/Unpublish the etcd cluster observes
// under this store's track prefix. The returned channel closes when ctx is
// cancelled or the store is closed. Per the slow-consumer contract, delivery is
// non-blocking: a full buffer drops the event with a logged warning rather than
// stalling the etcd watch loop.
func (s *Store) WatchTracks(ctx context.Context) (<-chan discovery.TrackEvent, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	out := make(chan discovery.TrackEvent, s.bufferSize)
	wctx, cancel := context.WithCancel(ctx)
	wch := s.cli.Watch(wctx, s.root+"t/", clientv3.WithPrefix(), clientv3.WithPrevKV())
	go s.pumpTracks(wctx, cancel, wch, out)
	return out, nil
}

// WatchNamespaces — same contract as [Store.WatchTracks], over namespace events.
func (s *Store) WatchNamespaces(ctx context.Context) (<-chan discovery.NamespaceEvent, error) {
	if err := s.notClosed(); err != nil {
		return nil, err
	}
	out := make(chan discovery.NamespaceEvent, s.bufferSize)
	wctx, cancel := context.WithCancel(ctx)
	wch := s.cli.Watch(wctx, s.root+"n/", clientv3.WithPrefix(), clientv3.WithPrevKV())
	go s.pumpNamespaces(wctx, cancel, wch, out)
	return out, nil
}

func (s *Store) pumpTracks(
	ctx context.Context,
	cancel context.CancelFunc,
	wch clientv3.WatchChan,
	out chan discovery.TrackEvent,
) {
	defer close(out)
	defer cancel()
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok || resp.Canceled {
				return
			}
			for _, ev := range resp.Events {
				te, ok := s.trackEvent(ev)
				if !ok {
					continue
				}
				select {
				case out <- te:
				default:
					s.log.WarnContext(ctx, "etcd discovery: dropped track event on slow watcher", "op", te.Op.String())
				}
			}
		}
	}
}

func (s *Store) pumpNamespaces(
	ctx context.Context,
	cancel context.CancelFunc,
	wch clientv3.WatchChan,
	out chan discovery.NamespaceEvent,
) {
	defer close(out)
	defer cancel()
	for {
		select {
		case <-s.done:
			return
		case <-ctx.Done():
			return
		case resp, ok := <-wch:
			if !ok || resp.Canceled {
				return
			}
			for _, ev := range resp.Events {
				ne, ok := s.namespaceEvent(ev)
				if !ok {
					continue
				}
				select {
				case out <- ne:
				default:
					s.log.WarnContext(
						ctx,
						"etcd discovery: dropped namespace event on slow watcher",
						"op",
						ne.Op.String(),
					)
				}
			}
		}
	}
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
