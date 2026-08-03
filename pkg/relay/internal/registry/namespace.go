package registry

import (
	"context"
	"log/slog"
	"slices"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// PublisherEntry records a single PUBLISH_NAMESPACE advertisement received
// from a publisher (or upstream relay). The relay holds onto the bidi
// Stream because §6.2 / §10.15 require the same stream to stay open for the
// lifetime of the advertisement — that's also where REQUEST_OK / REQUEST_ERROR
// and the eventual cancellation FIN flow.
type PublisherEntry struct {
	// Namespace is the exact tuple the publisher advertised (§2.4.1).
	Namespace wire.TrackNamespace

	// Session is the MOQT session that owns the PUBLISH_NAMESPACE.
	Session *session.Session

	// Stream is the bidi request stream the PUBLISH_NAMESPACE arrived on.
	// The session handler reads further control messages from it and is
	// the owner that closes/cancels it on teardown.
	Stream session.Stream
}

// SubscriberEntry records a single SUBSCRIBE_NAMESPACE or SUBSCRIBE_TRACKS
// announcement received from a subscriber (or downstream relay). §6.1 says
// these are open-ended subscriptions to a *prefix*: the relay must echo any
// matching PUBLISH_NAMESPACE / PUBLISH back to the subscriber as long as the
// subscription is alive.
type SubscriberEntry struct {
	// Prefix is the namespace prefix the subscriber asked to be notified
	// about. A zero-field prefix means "all namespaces" (§6.1).
	Prefix wire.TrackNamespace

	// Session is the MOQT session that owns the SUBSCRIBE_NAMESPACE /
	// SUBSCRIBE_TRACKS.
	Session *session.Session

	// Stream is the bidi request stream the subscription arrived on. The
	// session handler writes NAMESPACE / NAMESPACE_DONE / PUBLISH
	// messages back through this stream when matching publishers appear
	// or vanish. All such writes MUST go through [SubscriberEntry.WriteMessage]
	// (guarded by writeMu) — the stream is written from several goroutines
	// (every publisher's PUBLISH_NAMESPACE / PUBLISH handler and the
	// relay-level Discovery namespace watcher), and the underlying
	// session.Stream does not serialise concurrent Writes.
	Stream session.Stream

	// writeMu serialises concurrent control-message writes to Stream. A
	// single control message is several stream Writes (frame header + body),
	// so without this two interleaving Marshal calls would corrupt the wire
	// framing (and race the underlying QUIC stream). It guards only writes —
	// it is independent of the owning [NamespaceRegistry]'s mutex.
	writeMu sync.Mutex

	// WantsTracks distinguishes SUBSCRIBE_TRACKS (true: forward PUBLISH
	// messages for matching tracks) from SUBSCRIBE_NAMESPACE (false: only
	// NAMESPACE / NAMESPACE_DONE). The two share a registry entry because
	// they share the prefix-matching semantics — the session handler
	// dispatches on this flag.
	WantsTracks bool

	// Forward and GroupOrder carry the FORWARD (§10.2.17) and GROUP_ORDER
	// (§10.2.8) parameters from the SUBSCRIBE_TRACKS, which §10.19.1 copies
	// onto every PUBLISH the subscription triggers. Set once at registration
	// (never mutated), so reads in the PUBLISH fanout need no lock. They are
	// meaningful only when WantsTracks: Forward defaults to true (FORWARD
	// omitted or 1); GroupOrder is 0 when omitted (the publisher's default
	// applies) or the validated Ascending/Descending value.
	Forward    bool
	GroupOrder byte
}

// WriteMessage serialises one control message onto the subscriber's request
// stream. NAMESPACE / NAMESPACE_DONE / PUBLISH_SKIPPED are written to a single
// SubscriberEntry from multiple goroutines — the subscriber's own session
// handler, every publisher's PUBLISH_NAMESPACE / PUBLISH handler, and the
// relay-level Discovery namespace watcher — so the write is taken under writeMu
// to keep one message's frames contiguous on the wire.
func (e *SubscriberEntry) WriteMessage(m message.Message) error {
	e.writeMu.Lock()
	defer e.writeMu.Unlock()
	return message.Marshal(e.Stream, m)
}

// NamespaceRegistry maintains the relay's view of who advertises which
// namespaces and who has subscribed to which prefixes (§9.5 / §6.1).
//
// Two slices, two queries:
//
//   - publishers, populated by [Register…] / drained by [Unregister…],
//     queried by [MatchPublishers] when an inbound SUBSCRIBE arrives and
//     the relay needs to find upstream(s) for its track.
//   - subscribers, queried by [MatchSubscribers] when an inbound
//     PUBLISH_NAMESPACE / PUBLISH arrives and the relay needs to forward
//     notifications downstream.
//
// Linear scans for both queries are intentional: namespace cardinality is
// far lower than per-track cardinality and these matches are not on the
// object fanout hot path. If profiling later shows otherwise, swapping in a
// trie behind the same API is a contained change.
//
// Cross-instance namespace advertisement is delegated to the optional
// [discovery.DiscoveryStore]; when configured, the registry mirrors
// publish / unpublish events into it.
type NamespaceRegistry struct {
	mu          sync.RWMutex
	publishers  []*PublisherEntry
	subscribers []*SubscriberEntry

	// pubCount refs each distinct namespace by its wire-encoded key.
	// Used to coalesce Discovery PublishNamespace / UnpublishNamespace
	// calls: only the 0→1 and 1→0 transitions fire events, so two
	// publishers advertising the same namespace from the same relay
	// produce one Discovery entry, not two.
	pubCount map[string]int

	// discovery / relayAddr / log mirror [TrackRegistry] — see those
	// docs. nil discovery means "do not advertise"; failures log at
	// Warn and are not propagated.
	discovery discovery.DiscoveryStore
	relayAddr string
	log       *slog.Logger
}

// NamespaceRegistryOption tweaks a [NamespaceRegistry] at construction.
type NamespaceRegistryOption func(*NamespaceRegistry)

// WithNamespaceDiscovery installs a [discovery.DiscoveryStore] for
// cross-instance namespace advertisement. relayAddr is stamped into
// every [discovery.NamespaceInfo] this registry emits.
func WithNamespaceDiscovery(d discovery.DiscoveryStore, relayAddr string) NamespaceRegistryOption {
	return func(r *NamespaceRegistry) {
		r.discovery = d
		r.relayAddr = relayAddr
	}
}

// WithNamespaceRegistryLogger sets the logger used for Discovery
// warnings.
func WithNamespaceRegistryLogger(l *slog.Logger) NamespaceRegistryOption {
	return func(r *NamespaceRegistry) { r.log = l }
}

// NewNamespaceRegistry constructs an empty registry.
func NewNamespaceRegistry(opts ...NamespaceRegistryOption) *NamespaceRegistry {
	r := &NamespaceRegistry{
		pubCount: make(map[string]int),
		log:      slog.Default(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// RegisterPublisher records a publisher's PUBLISH_NAMESPACE. The returned
// pointer is the canonical record; callers should keep it for the eventual
// [NamespaceRegistry.UnregisterPublisher] call rather than rebuilding it.
//
// Duplicate registrations from the same session for the same namespace are
// not deduplicated — §9.3 explicitly permits a relay to see PUBLISH_NAMESPACE
// for the same namespace from multiple publishers, and even a single session
// can in principle re-advertise after withdrawing. Callers that want
// duplicate-suppression policy enforce it at the request-handler layer.
func (r *NamespaceRegistry) RegisterPublisher(
	ns wire.TrackNamespace,
	sess *session.Session,
	stream session.Stream,
) *PublisherEntry {
	entry := &PublisherEntry{Namespace: ns, Session: sess, Stream: stream}
	key := namespaceWireKey(ns)
	r.mu.Lock()
	r.publishers = append(r.publishers, entry)
	r.pubCount[key]++
	if r.pubCount[key] == 1 {
		// Under r.mu, so publish/unpublish reach the Discovery store in
		// exactly the order pubCount crossed the 0 boundary — see
		// [NamespaceRegistry.unpublishNamespaceFromDiscovery].
		r.publishNamespaceToDiscovery(ns)
	}
	r.mu.Unlock()
	return entry
}

// UnregisterPublisher removes a previously registered publisher entry. The
// caller passes the exact pointer that RegisterPublisher returned — this
// avoids any ambiguity when the same (session, namespace) pair has multiple
// concurrent registrations.
//
// Returns true if the entry was found and removed.
func (r *NamespaceRegistry) UnregisterPublisher(entry *PublisherEntry) bool {
	r.mu.Lock()
	before := len(r.publishers)
	r.publishers = slices.DeleteFunc(r.publishers, func(e *PublisherEntry) bool {
		return e == entry
	})
	removed := len(r.publishers) < before
	if removed {
		key := namespaceWireKey(entry.Namespace)
		r.pubCount[key]--
		if r.pubCount[key] <= 0 {
			delete(r.pubCount, key)
			// Under r.mu — see
			// [NamespaceRegistry.unpublishNamespaceFromDiscovery].
			r.unpublishNamespaceFromDiscovery(entry.Namespace)
		}
	}
	r.mu.Unlock()
	return removed
}

// RegisterSubscriber records a subscriber's SUBSCRIBE_NAMESPACE (when
// wantsTracks is false) or SUBSCRIBE_TRACKS (when true). forward and groupOrder
// carry the SUBSCRIBE_TRACKS FORWARD/GROUP_ORDER passthrough values (§10.19.1)
// and are ignored unless wantsTracks. Returns the canonical pointer for use
// with [NamespaceRegistry.UnregisterSubscriber].
func (r *NamespaceRegistry) RegisterSubscriber(
	prefix wire.TrackNamespace,
	sess *session.Session,
	stream session.Stream,
	wantsTracks bool,
	forward bool,
	groupOrder byte,
) *SubscriberEntry {
	entry := &SubscriberEntry{
		Prefix:      prefix,
		Session:     sess,
		Stream:      stream,
		WantsTracks: wantsTracks,
		Forward:     forward,
		GroupOrder:  groupOrder,
	}
	r.mu.Lock()
	r.subscribers = append(r.subscribers, entry)
	r.mu.Unlock()
	return entry
}

// UnregisterSubscriber removes a previously registered subscriber entry.
// Returns true if the entry was found and removed.
func (r *NamespaceRegistry) UnregisterSubscriber(entry *SubscriberEntry) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	before := len(r.subscribers)
	r.subscribers = slices.DeleteFunc(r.subscribers, func(e *SubscriberEntry) bool {
		return e == entry
	})
	return len(r.subscribers) < before
}

// RemoveSession removes every publisher and subscriber entry owned by sess.
// This is the bulk-cleanup path session handlers take when the underlying
// transport dies — they cannot iterate the registry themselves without
// risking a stale view, so the registry does it under its own lock.
//
// Returns the number of publisher entries and subscriber entries removed,
// in that order. The split lets callers distinguish what Discovery
// unpublish calls were warranted by the cleanup.
func (r *NamespaceRegistry) RemoveSession(sess *session.Session) (publishers, subscribers int) {
	r.mu.Lock()
	beforeP := len(r.publishers)

	// Collect the namespaces this session owned so we can decrement
	// pubCount under the same lock — without it we'd lose the
	// "last publisher leaving the relay" signal that Discovery needs.
	var toUnadvertise []wire.TrackNamespace
	for _, e := range r.publishers {
		if e.Session != sess {
			continue
		}
		key := namespaceWireKey(e.Namespace)
		r.pubCount[key]--
		if r.pubCount[key] <= 0 {
			delete(r.pubCount, key)
			toUnadvertise = append(toUnadvertise, e.Namespace)
		}
	}
	r.publishers = slices.DeleteFunc(r.publishers, func(e *PublisherEntry) bool {
		return e.Session == sess
	})

	beforeS := len(r.subscribers)
	r.subscribers = slices.DeleteFunc(r.subscribers, func(e *SubscriberEntry) bool {
		return e.Session == sess
	})

	// Capture the final lengths under the lock; reading them after
	// Unlock races with concurrent RemoveSession calls.
	pubsRemoved := beforeP - len(r.publishers)
	subsRemoved := beforeS - len(r.subscribers)
	for _, ns := range toUnadvertise {
		// Under r.mu — see
		// [NamespaceRegistry.unpublishNamespaceFromDiscovery].
		r.unpublishNamespaceFromDiscovery(ns)
	}
	r.mu.Unlock()
	return pubsRemoved, subsRemoved
}

// publishNamespaceToDiscovery advertises ns to the Discovery store
// (best-effort). See [TrackRegistry.publishTrackToDiscovery] for the
// rationale around synchronous calls + log-and-swallow errors.
func (r *NamespaceRegistry) publishNamespaceToDiscovery(ns wire.TrackNamespace) {
	if r.discovery == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), discoveryCallTimeout)
	defer cancel()
	if err := r.discovery.PublishNamespace(ctx, discovery.NamespaceInfo{
		Prefix:    ns,
		RelayAddr: r.relayAddr,
	}); err != nil {
		r.log.Warn("discovery: PublishNamespace failed", "err", err.Error(), "namespace", ns)
	}
}

// unpublishNamespaceFromDiscovery is the counterpart called when the last
// publisher of a namespace leaves.
//
// The caller MUST hold r.mu. Both this and [publishNamespaceToDiscovery]
// run under the registry lock so the store receives publish/unpublish in
// exactly the order pubCount crossed 0 — a late unpublish issued after
// releasing r.mu could race a concurrent RegisterPublisher's publish and
// erase the re-advertised namespace's record. The Discovery call is bounded
// by [discoveryCallTimeout] — and the interface requires backends to honor
// ctx deadlines — so the lock hold is bounded too.
func (r *NamespaceRegistry) unpublishNamespaceFromDiscovery(ns wire.TrackNamespace) {
	if r.discovery == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), discoveryCallTimeout)
	defer cancel()
	if err := r.discovery.UnpublishNamespace(ctx, ns, r.relayAddr); err != nil {
		r.log.Warn("discovery: UnpublishNamespace failed", "err", err.Error(), "namespace", ns)
	}
}

// namespaceWireKey serialises a wire.TrackNamespace into a canonical
// byte string suitable for use as a map key. The same trick the
// discovery package uses; we re-implement here to keep the relay
// package free of an internal dependency in case the discovery
// package's helper ever becomes test-only.
func namespaceWireKey(ns wire.TrackNamespace) string {
	w := wire.NewWriter(nil)
	w.TrackNamespace(ns)
	return string(w.Bytes())
}

// MatchPublishers returns every publisher entry whose advertised namespace
// is a prefix of (or equal to) ns. This implements the §9.5 rule the
// SUBSCRIBE handler uses:
//
//	"the Relay MUST send a SUBSCRIBE request to each publisher that has
//	 published the subscription's namespace or prefix thereof."
//
// Example: a publisher that advertised PUBLISH_NAMESPACE ("video",) matches
// a SUBSCRIBE for ("video", "cam1"). A publisher that advertised
// ("video", "cam1") matches a SUBSCRIBE for ("video", "cam1") but NOT a
// SUBSCRIBE for ("video",) — the stored namespace must be a prefix of, or
// equal to, the queried namespace.
//
// The returned slice is a fresh allocation; callers may iterate it without
// holding the registry lock.
func (r *NamespaceRegistry) MatchPublishers(ns wire.TrackNamespace) []*PublisherEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*PublisherEntry
	for _, e := range r.publishers {
		if ns.HasPrefix(e.Namespace) {
			out = append(out, e)
		}
	}
	return out
}

// MatchSubscribers returns every subscriber entry whose stored prefix is a
// prefix of (or equal to) ns. This implements the §6.2 / §6.1 forwarding
// rules the PUBLISH_NAMESPACE and PUBLISH handlers use to find which
// downstream subscribers want to be notified of a newly-advertised
// namespace or a newly-published track.
//
// Example: a subscriber that sent SUBSCRIBE_NAMESPACE ("video",) matches a
// PUBLISH_NAMESPACE ("video", "cam1"). A subscriber that sent
// SUBSCRIBE_NAMESPACE ("video", "cam1") matches a PUBLISH_NAMESPACE
// ("video", "cam1") but not ("video",).
//
// Note that a SUBSCRIBE_NAMESPACE with zero fields (§6.1: "the sender is
// interested in all namespaces") matches every PUBLISH_NAMESPACE — that
// case falls out of isPrefixOf naturally.
//
// The returned slice is a fresh allocation; callers may iterate it without
// holding the registry lock.
func (r *NamespaceRegistry) MatchSubscribers(ns wire.TrackNamespace) []*SubscriberEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*SubscriberEntry
	for _, e := range r.subscribers {
		if ns.HasPrefix(e.Prefix) {
			out = append(out, e)
		}
	}
	return out
}

// CopyPublishers returns a snapshot of all publisher entries. Intended for
// tests, metrics, and the Stop path (where the registry is being drained
// and the caller wants to iterate without holding the lock across slow
// per-entry work).
func (r *NamespaceRegistry) CopyPublishers() []*PublisherEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*PublisherEntry, len(r.publishers))
	copy(out, r.publishers)
	return out
}

// CopySubscribers returns a snapshot of all subscriber entries. See
// [NamespaceRegistry.CopyPublishers].
func (r *NamespaceRegistry) CopySubscribers() []*SubscriberEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*SubscriberEntry, len(r.subscribers))
	copy(out, r.subscribers)
	return out
}
