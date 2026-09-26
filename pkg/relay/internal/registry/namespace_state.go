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
// §10.18: NAMESPACE_DONE says the publisher stops "serving new subscriptions
// for tracks within the provided Track Namespace", so it is per namespace, not
// per source: two publishers of one namespace announce it once, and it is done
// when the last one leaves. §10.19: "The publisher MUST NOT send NAMESPACE_DONE
// for a namespace suffix before the corresponding NAMESPACE."
//
// Each subscriber therefore counts the sources — local PUBLISH_NAMESPACE
// registrations and remote relays reported by Discovery — of each namespace
// under its prefix. The counts change only under the registry lock, which
// also orders the messages the changes produce: NAMESPACE on a count's 0→1,
// NAMESPACE_DONE on its 1→0. Messages go to the subscriber's outbox, and one
// writer ([SubscriberEntry.RunWriter]) sends them in that order, so no stream
// write happens under the lock.

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
// stopped advertising ns, as Discovery reports it, and announces the change to
// SUBSCRIBE_NAMESPACE subscribers like a local PUBLISH_NAMESPACE would. A
// repeated report changes nothing.
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
// For a SUBSCRIBE_NAMESPACE the announced set is reconciled to the new prefix:
// namespaces it no longer covers are done before ok (their suffixes are
// relative to the old prefix), and namespaces it newly covers are announced
// after ok, relative to the new one — "NAMESPACE and NAMESPACE_DONE messages
// following the REQUEST_OK will contain Track Namespace suffixes relative to
// the updated prefix". A SUBSCRIBE_TRACKS only changes which later PUBLISHes
// match; "Updating the prefix of a SUBSCRIBE_TRACKS has no effect on existing
// subscriptions".
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
	// e.announced holds exactly the namespaces with sources under the old
	// prefix, so that view names each one to be done.
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
// replies that must keep their order relative to NAMESPACE / NAMESPACE_DONE
// (a REQUEST_UPDATE's REQUEST_OK, §10.9).
func (e *SubscriberEntry) Enqueue(m message.Message) {
	e.push(m, false)
}

// Finish queues m as the last message of the request, after which the writer
// FINs the stream: "When a REQUEST_UPDATE fails for a SUBSCRIBE_NAMESPACE,
// SUBSCRIBE_TRACKS or PUBLISH_NAMESPACE, the responder MUST close the bidi
// stream (see Section 3.3.2)". That ends the request: the owner waits for
// [SubscriberEntry.WriterDone] and then unregisters e.
func (e *SubscriberEntry) Finish(m message.Message) {
	e.push(m, true)
}

func (e *SubscriberEntry) enqueue(m message.Message) { e.push(m, false) }

// maxQueuedMessages bounds how many messages a namespace subscription may have
// waiting to be sent. §10.19: "If the publisher is unable to send NAMESPACE or
// NAMESPACE_DONE messages in a timely manner because the SUBSCRIBE_NAMESPACE
// response stream is blocked by flow control, the publisher MAY reset the
// SUBSCRIBE_NAMESPACE response stream." The relay treats a stream as blocked
// when its writer has been stuck in one write for maxBlockedWrite while at
// least maxQueuedMessages wait behind it; a burst (seeding a subscription, a
// prefix update, a Discovery resync) queues many messages at once but keeps
// the writer moving, so it does not count. The same rule holds a
// SUBSCRIBE_TRACKS stream's PUBLISH_SKIPPEDs.
const (
	maxQueuedMessages = 1024
	maxBlockedWrite   = time.Second
)

// push appends m, then the finish marker (a nil message) when last. Nothing
// is queued once the request is finishing or its stream failed. A push to a
// blocked stream (see maxQueuedMessages) resets it with EXCESSIVE_LOAD instead
// — both halves, which also unblocks the stuck write and ends the request's
// reader, so the owner unregisters e.
func (e *SubscriberEntry) push(m message.Message, last bool) {
	e.outMu.Lock()
	if e.stopped {
		e.outMu.Unlock()
		return
	}
	if len(e.outbox) >= maxQueuedMessages && e.writeBlocked() {
		e.stopped = true
		e.outbox = nil
		e.outMu.Unlock()
		e.Stream.CancelWrite(uint64(moqt.StreamResetExcessiveLoad))
		e.Stream.CancelRead(uint64(moqt.StreamResetExcessiveLoad))
		return
	}
	e.outbox = append(e.outbox, m)
	if last {
		e.outbox = append(e.outbox, nil)
		e.stopped = true
	}
	e.outMu.Unlock()
	select {
	case e.outReady <- struct{}{}:
	default:
	}
}

// RunWriter sends e's queued messages in order until e is unregistered, the
// request finishes, or a write fails. Its owner runs it once, for the
// subscription's lifetime. After a failed write it also stops reading the
// stream, so the request's reader returns and the owner unregisters e: that is
// how a peer's STOP_SENDING-only cancel (§3.3.3) ends the subscription.
func (e *SubscriberEntry) RunWriter() {
	defer close(e.writerDone)
	for {
		select {
		case <-e.closed:
			return
		case <-e.outReady:
		}
		e.outMu.Lock()
		batch := e.outbox
		e.outbox = nil
		e.outMu.Unlock()
		for _, m := range batch {
			if m == nil { // the finish marker
				_ = e.Stream.Close()
				return
			}
			e.writeSince.Store(time.Now().UnixNano())
			err := e.write(m)
			e.writeSince.Store(0)
			if err != nil {
				e.outMu.Lock()
				e.stopped = true
				e.outbox = nil
				e.outMu.Unlock()
				e.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
				return
			}
		}
	}
}

// writeBlocked reports whether RunWriter has been stuck in one write for
// maxBlockedWrite.
func (e *SubscriberEntry) writeBlocked() bool {
	since := e.writeSince.Load()
	return since != 0 && time.Since(time.Unix(0, since)) > maxBlockedWrite
}

// WriterDone is closed once RunWriter has returned: after the FIN that
// [SubscriberEntry.Finish] asked for, a failed write, or unregistration.
func (e *SubscriberEntry) WriterDone() <-chan struct{} { return e.writerDone }

// PublishSkipped queues a PUBLISH_SKIPPED (§10.21) for the track (ns, name) on
// a SUBSCRIBE_TRACKS subscriber, its suffix relative to the prefix in force at
// that point of the stream, so it cannot disagree with a queued prefix
// update's REQUEST_OK. It reports false, queuing nothing, when an update moved
// the prefix off ns.
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
// Only differences reach subscribers: a namespace some relay still advertises
// causes nothing, one no longer advertised is done, one newly advertised is
// announced. Sources are added before any is removed, so a namespace whose
// only advertising relay changed is not done and announced again.
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

// namespaceMessage is the NAMESPACE announcing ns to a subscriber of prefix:
// only the fields after the prefix (§10.17).
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
