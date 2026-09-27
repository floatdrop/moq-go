package registry

import (
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// arrivals wakes the waiters on a key once a publisher arrives for it. It is
// guarded by the owning registry's mutex.
type arrivals[K comparable] map[K]*arrival

// arrival is the waiters on one key: ch closes when a publisher arrives.
type arrival struct {
	ch      chan struct{}
	ns      wire.TrackNamespace // for [arrivals.notifyCoveredLocked]
	waiters int
}

// waitLocked registers a waiter on k and returns its channel and the stop
// that unregisters it under mu, the owning registry's mutex.
func (a *arrivals[K]) waitLocked(k K, ns wire.TrackNamespace, mu sync.Locker) (<-chan struct{}, func()) {
	if *a == nil {
		*a = make(arrivals[K])
	}
	w := (*a)[k]
	if w == nil {
		w = &arrival{ch: make(chan struct{}), ns: ns}
		(*a)[k] = w
	}
	w.waiters++
	return w.ch, func() {
		mu.Lock()
		defer mu.Unlock()
		if w.waiters--; w.waiters == 0 && (*a)[k] == w {
			delete(*a, k)
		}
	}
}

// notifyLocked wakes the waiters on k.
func (a arrivals[K]) notifyLocked(k K) {
	if w := a[k]; w != nil {
		close(w.ch)
		delete(a, k)
	}
}

// notifyCoveredLocked wakes the waiters whose namespace ns is a prefix of.
func (a arrivals[K]) notifyCoveredLocked(ns wire.TrackNamespace) {
	for k, w := range a {
		if w.ns.HasPrefix(ns) {
			close(w.ch)
			delete(a, k)
		}
	}
}
