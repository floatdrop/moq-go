package registry

import (
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// A SUBSCRIBE_NAMESPACE subscriber's view of the namespaces announced to it.
//
// §10.18: NAMESPACE_DONE is per namespace, not per source, so each subscriber
// counts the sources (local PUBLISH_NAMESPACEs and Discovery's remote relays)
// of each namespace under its prefix: NAMESPACE on a count's 0→1,
// NAMESPACE_DONE on its 1→0. Counts change only under the registry lock, which
// also orders the messages; [SubscriberEntry.RunWriter] sends them, so no
// stream write happens under the lock.

// remoteNamespace is one namespace Discovery reports other relays advertise.
type remoteNamespace struct {
	ns     wire.TrackNamespace
	relays map[string]struct{}
}

// namespaceSources returns every namespace under prefix with its number of
// sources, keyed by wire key. The caller holds r.mu.
func (r *NamespaceRegistry) namespaceSources(
	prefix wire.TrackNamespace,
) (map[string]int, map[string]wire.TrackNamespace) {
	counts := make(map[string]int)
	names := make(map[string]wire.TrackNamespace)
	for _, p := range r.publishers {
		if p.announced && p.Namespace.HasPrefix(prefix) {
			k := namespaceWireKey(p.Namespace)
			counts[k]++
			names[k] = p.Namespace
		}
	}
	for k, rn := range r.remote {
		if rn.ns.HasPrefix(prefix) {
			counts[k] += len(rn.relays)
			names[k] = rn.ns
		}
	}
	return counts, names
}

// addSourceLocked records one more source of ns for every SUBSCRIBE_NAMESPACE
// subscriber whose prefix covers it, announcing ns to those that had none. The
// caller holds r.mu.
func (r *NamespaceRegistry) addSourceLocked(ns wire.TrackNamespace) {
	k := namespaceWireKey(ns)
	for _, s := range r.subscribers {
		prefix := s.Prefix()
		if s.WantsTracks || !ns.HasPrefix(prefix) {
			continue
		}
		s.announced[k]++
		if s.announced[k] == 1 {
			s.enqueue(namespaceMessage(ns, prefix))
		}
	}
}

// removeSourceLocked drops one source of ns for every subscriber counting it,
// sending NAMESPACE_DONE to those left with none. The caller holds r.mu.
func (r *NamespaceRegistry) removeSourceLocked(ns wire.TrackNamespace) {
	k := namespaceWireKey(ns)
	for _, s := range r.subscribers {
		if s.WantsTracks || s.announced[k] == 0 {
			continue
		}
		s.announced[k]--
		if s.announced[k] == 0 {
			delete(s.announced, k)
			s.enqueue(namespaceDoneMessage(ns, s.Prefix()))
		}
	}
}

// RemoteNamespace records that the relay at relayAddr started (published) or
// stopped advertising ns, and announces the change to SUBSCRIBE_NAMESPACE
// subscribers. A repeated report changes nothing.
func (r *NamespaceRegistry) RemoteNamespace(ns wire.TrackNamespace, relayAddr string, published bool) {
	k := namespaceWireKey(ns)
	r.mu.Lock()
	defer r.mu.Unlock()
	rn := r.remote[k]
	if published {
		if rn == nil {
			rn = &remoteNamespace{ns: ns, relays: make(map[string]struct{})}
			r.remote[k] = rn
		}
		if _, have := rn.relays[relayAddr]; have {
			return
		}
		rn.relays[relayAddr] = struct{}{}
		r.addSourceLocked(ns)
		return
	}
	if rn == nil {
		return
	}
	if _, have := rn.relays[relayAddr]; !have {
		return
	}
	delete(rn.relays, relayAddr)
	if len(rn.relays) == 0 {
		delete(r.remote, k)
	}
	r.removeSourceLocked(ns)
}

// UpdatePrefix applies a TRACK_NAMESPACE_PREFIX update (§10.9.2) to e and
// queues ok, the update's REQUEST_OK, in order with e's other messages.
//
// For a SUBSCRIBE_NAMESPACE, namespaces no longer covered are done before ok
// (suffixes relative to the old prefix) and newly covered ones announced after
// it, relative to the new one. A SUBSCRIBE_TRACKS only changes which later
// PUBLISHes match.
func (r *NamespaceRegistry) UpdatePrefix(e *SubscriberEntry, prefix wire.TrackNamespace, ok message.Message) {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := e.Prefix()
	e.prefix.Store(&prefix)
	if e.WantsTracks {
		e.enqueue(ok)
		return
	}
	counts, names := r.namespaceSources(prefix)
	_, oldNames := r.namespaceSources(old)
	for k := range e.announced {
		if _, still := counts[k]; !still {
			e.enqueue(namespaceDoneMessage(oldNames[k], old))
		}
	}
	e.enqueue(ok)
	for k, ns := range names {
		if _, had := e.announced[k]; !had {
			e.enqueue(namespaceMessage(ns, prefix))
		}
	}
	e.announced = counts
}

// Enqueue queues m on e's stream behind every message already queued, for
// replies that must keep their order relative to NAMESPACE / NAMESPACE_DONE.
func (e *SubscriberEntry) Enqueue(m message.Message) {
	e.push(m, false)
}

// Finish queues m as the last message of the request, after which the writer
// FINs the stream (§10.9.1, for a failed REQUEST_UPDATE). The owner then waits
// for [SubscriberEntry.WriterDone] and unregisters e.
func (e *SubscriberEntry) Finish(m message.Message) {
	e.push(m, true)
}

func (e *SubscriberEntry) enqueue(m message.Message) { e.push(m, false) }

// maxQueuedMessages and maxUnsentWait bound a namespace subscription's queue
// (§10.19: a blocked response stream "MAY" be reset). The relay counts a
// stream as blocked when at least maxQueuedMessages are unsent and the oldest
// has waited longer than maxUnsentWait. A slow subscriber still draining a
// large seed can therefore be reset. The check runs only when a message is
// queued. The same bound holds a SUBSCRIBE_TRACKS stream's PUBLISH_SKIPPEDs.
const (
	maxQueuedMessages = 1024
	maxUnsentWait     = time.Second
)

// queuedMessage is a message waiting for RunWriter, with its (monotonic)
// queue time. A nil m is the finish marker.
type queuedMessage struct {
	m  message.Message
	at time.Time
}

// push appends m, then the finish marker when last. Nothing is queued once
// the request is finishing or its stream failed. A push to a blocked stream
// resets both halves with EXCESSIVE_LOAD instead, which unblocks a stuck write
// and ends the request's reader.
func (e *SubscriberEntry) push(m message.Message, last bool) {
	now := time.Now()
	e.outMu.Lock()
	if e.stopped {
		e.outMu.Unlock()
		return
	}
	if e.blockedLocked(now) {
		e.stopped = true
		e.outbox = nil
		e.outMu.Unlock()
		e.Stream.CancelWrite(uint64(moqt.StreamResetExcessiveLoad))
		e.Stream.CancelRead(uint64(moqt.StreamResetExcessiveLoad))
		return
	}
	e.outbox = append(e.outbox, queuedMessage{m: m, at: now})
	if last {
		e.outbox = append(e.outbox, queuedMessage{at: now})
		e.stopped = true
	}
	e.outMu.Unlock()
	select {
	case e.outReady <- struct{}{}:
	default:
	}
}

// blockedLocked reports whether at least maxQueuedMessages are unsent and the
// oldest has waited longer than maxUnsentWait. e.outMu must be held.
func (e *SubscriberEntry) blockedLocked(now time.Time) bool {
	unsent := len(e.outbox)
	oldest := time.Time{}
	if !e.writing.at.IsZero() {
		unsent++
		oldest = e.writing.at
	} else if len(e.outbox) > 0 {
		oldest = e.outbox[0].at
	}
	return unsent >= maxQueuedMessages && now.Sub(oldest) > maxUnsentWait
}

// RunWriter sends e's queued messages in order until e is unregistered, the
// request finishes, a write fails, or the queue bound resets the stream. Its
// owner runs it once, for the subscription's lifetime. After a failed write it
// also stops reading the stream, so a peer's STOP_SENDING-only cancel (§3.3.3)
// ends the subscription.
func (e *SubscriberEntry) RunWriter() {
	defer close(e.writerDone)
	for {
		select {
		case <-e.closed:
			return
		case <-e.outReady:
		}
		for {
			e.outMu.Lock()
			if len(e.outbox) == 0 {
				stopped := e.stopped
				e.outMu.Unlock()
				if stopped {
					// Only the queue bound's reset gets here; the owner may
					// be waiting on WriterDone.
					return
				}
				break
			}
			q := e.outbox[0]
			e.outbox[0] = queuedMessage{}
			e.outbox = e.outbox[1:]
			e.writing = q
			e.outMu.Unlock()
			if q.m == nil { // the finish marker
				_ = e.Stream.Close()
				return
			}
			err := e.write(q.m)
			e.outMu.Lock()
			e.writing = queuedMessage{}
			if err != nil {
				e.stopped = true
				e.outbox = nil
				e.outMu.Unlock()
				e.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
				return
			}
			e.outMu.Unlock()
		}
	}
}

// WriterDone is closed once RunWriter has returned.
func (e *SubscriberEntry) WriterDone() <-chan struct{} { return e.writerDone }

// PublishSkipped queues a PUBLISH_SKIPPED (§10.21) for the track (ns, name) on
// a SUBSCRIBE_TRACKS subscriber, its suffix relative to the prefix in force at
// that point of the stream. It reports false, queuing nothing, when an update
// moved the prefix off ns.
func (r *NamespaceRegistry) PublishSkipped(e *SubscriberEntry, ns wire.TrackNamespace, name []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	prefix := e.Prefix()
	if !ns.HasPrefix(prefix) {
		return false
	}
	e.enqueue(&message.PublishSkipped{TrackNamespaceSuffix: suffixAfter(ns, prefix), TrackName: name})
	return true
}

// ReplaceRemote makes the remote namespaces exactly those in ads, for a
// Discovery watch's snapshot (see [discovery.DiscoveryStore.WatchNamespaces]).
// Only differences reach subscribers. Sources are added before any is removed,
// so a namespace whose only advertising relay changed is not done and
// announced again.
func (r *NamespaceRegistry) ReplaceRemote(ads []discovery.NamespaceInfo) {
	want := make(map[string]*remoteNamespace)
	for _, ad := range ads {
		k := namespaceWireKey(ad.Prefix)
		w := want[k]
		if w == nil {
			w = &remoteNamespace{ns: ad.Prefix, relays: make(map[string]struct{})}
			want[k] = w
		}
		w.relays[ad.RelayAddr] = struct{}{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for k, w := range want {
		rn := r.remote[k]
		if rn == nil {
			rn = &remoteNamespace{ns: w.ns, relays: make(map[string]struct{})}
			r.remote[k] = rn
		}
		for addr := range w.relays {
			if _, have := rn.relays[addr]; !have {
				rn.relays[addr] = struct{}{}
				r.addSourceLocked(rn.ns)
			}
		}
	}
	for k, rn := range r.remote {
		w := want[k]
		for addr := range rn.relays {
			if w != nil {
				if _, keep := w.relays[addr]; keep {
					continue
				}
			}
			delete(rn.relays, addr)
			r.removeSourceLocked(rn.ns)
		}
		if len(rn.relays) == 0 {
			delete(r.remote, k)
		}
	}
}

// namespaceMessage is the NAMESPACE announcing ns to a subscriber of prefix
// (§10.17).
func namespaceMessage(ns, prefix wire.TrackNamespace) *message.Namespace {
	return &message.Namespace{TrackNamespaceSuffix: suffixAfter(ns, prefix)}
}

// namespaceDoneMessage is the NAMESPACE_DONE counterpart (§10.18).
func namespaceDoneMessage(ns, prefix wire.TrackNamespace) *message.NamespaceDone {
	return &message.NamespaceDone{TrackNamespaceSuffix: suffixAfter(ns, prefix)}
}

func suffixAfter(ns, prefix wire.TrackNamespace) wire.TrackNamespace {
	return append(wire.TrackNamespace(nil), ns[len(prefix):]...)
}
