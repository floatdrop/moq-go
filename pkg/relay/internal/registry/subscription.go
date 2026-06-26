package registry

import (
	"fmt"
	"sync"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// SubState is the lifecycle phase of an upstream or downstream subscription
// as managed by the relay. The relay only ever observes two phases, so the
// model is deliberately just those two:
//
//   - SubEstablished: peer accepted; objects may flow.
//   - SubTerminated:  closed cleanly or by error; no further transitions.
//
// The relay constructs an UpstreamSub / DownstreamSub only once the peer has
// already accepted (it sends SUBSCRIBE_OK for a downstream sub; an upstream
// sub is built from the SUBSCRIBE_OK it received), so there is no observable
// "constructed but not yet established" phase to model — subs are born
// Established and the only transition is the one-way move to Terminated (see
// [Subscription.Terminate]).
//
// The state intentionally does NOT track per-object forwarding decisions.
// Those are fanout concerns expressed via the [message.SubscriptionFilter]
// / Forward-state fields on the concrete [UpstreamSub] / [DownstreamSub]
// structs.
type SubState int

const (
	// SubTerminated is the absorbing state. Either the peer ended the
	// subscription (UNSUBSCRIBE / SUBSCRIBE_DONE / PUBLISH_DONE /
	// SUBSCRIBE_ERROR / PUBLISH_ERROR), the underlying request stream
	// died, or the relay tore the subscription down (auth failure,
	// session close, Stop). Once here, the registry slot can be removed
	// safely by the owning goroutine. It is the zero value so a
	// bare-struct subscription is never mistaken for live; the
	// constructors set SubEstablished explicitly.
	SubTerminated SubState = iota

	// SubEstablished means the subscription is live: objects can be
	// forwarded and REQUEST_UPDATE / UNSUBSCRIBE can be sent.
	SubEstablished
)

// String returns "Established" or "Terminated".
func (s SubState) String() string {
	switch s {
	case SubEstablished:
		return "Established"
	case SubTerminated:
		return "Terminated"
	default:
		return fmt.Sprintf("SubState(%d)", int(s))
	}
}

// Subscription is the embedded common state for [UpstreamSub] and
// [DownstreamSub]. It centralises the mutex, the state field, and the
// terminate latch so the two concrete types only have to add their
// direction-specific fields.
//
// Locking discipline:
//
//   - State, ForwardState, and Filter are guarded by mu.
//   - The Session and Stream references are set once at construction and
//     are read-only thereafter; they are not protected.
//   - Callers that read multiple fields together (e.g. State + Filter
//     during fanout) should hold the lock themselves rather than reading
//     fields individually.
type Subscription struct {
	mu sync.RWMutex

	// state is the current lifecycle phase. Set to SubEstablished by the
	// constructors and moved one-way to SubTerminated via Terminate.
	state SubState

	// ID is unique within the relay process. It serves as the stable
	// removal handle in the Track Registry; see [TrackRegistry.RemoveUpstream].
	// Set once at construction; read-only.
	ID uint64

	// RequestID is the MOQT Request ID (§10.1) of the SUBSCRIBE / PUBLISH
	// that opened this subscription's request stream. It is the ID the
	// relay must reuse when sending REQUEST_UPDATE (§10.9) on the same
	// stream — REQUEST_UPDATE rides the original stream and does not
	// consume a new Request ID. Set once at construction; read-only.
	RequestID uint64

	// Session is the MOQT session that owns this subscription's request
	// stream. Read-only after construction.
	Session *session.Session

	// Stream is the bidi request stream the SUBSCRIBE / PUBLISH was
	// issued on. The owning goroutine (the session handler's request
	// loop) is the sole writer; the relay reads from it to observe
	// peer-side updates (REQUEST_UPDATE, UNSUBSCRIBE, PUBLISH_DONE,
	// etc.). Read-only after construction; the goroutine that owns the
	// stream closes it.
	Stream session.Stream

	// TrackAlias is the alias the relay assigned to this subscription on
	// its side of the wire (§11.1). For UpstreamSub it is the alias the
	// publisher uses when sending objects to us; for DownstreamSub it is
	// the alias we use when sending objects to the subscriber. Aliases
	// are per-direction, per-session — the fanout remaps between them.
	// Set once at construction; read-only.
	TrackAlias uint64

	// forwardState is the §9.2 Forward flag the peer most recently
	// requested. 1 = deliver objects, 0 = pause delivery. The session
	// handler updates it on REQUEST_UPDATE and the fanout consults it to
	// decide whether to write objects out.
	forwardState int
}

// Terminate moves the subscription to [SubTerminated], returning true on the
// first call and false on every subsequent call. The one-shot latch lets a
// caller run teardown that must happen exactly once (e.g. emitting a single
// PUBLISH_DONE) without coordinating with other goroutines; it is safe to
// call concurrently from any goroutine.
func (s *Subscription) Terminate() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == SubTerminated {
		return false
	}
	s.state = SubTerminated
	return true
}

// State returns the current lifecycle phase.
func (s *Subscription) State() SubState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

// IsEstablished reports whether the subscription is in [SubEstablished].
// Convenience wrapper for fanout / handler code that only cares whether
// objects may flow.
func (s *Subscription) IsEstablished() bool {
	return s.State() == SubEstablished
}

// IsTerminated reports whether the subscription is in [SubTerminated].
// Convenience wrapper for cleanup paths.
func (s *Subscription) IsTerminated() bool {
	return s.State() == SubTerminated
}

// SetForwardState updates the §9.2 Forward flag. The relay does not validate
// the value here — §10.7's Forward field is canonically 0 or 1, but allowing
// any int keeps the door open for future extensions (e.g. priority-banded
// forwarding) without an API change.
func (s *Subscription) SetForwardState(v int) {
	s.mu.Lock()
	s.forwardState = v
	s.mu.Unlock()
}

// ForwardState returns the most recently set §9.2 Forward flag.
func (s *Subscription) ForwardState() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.forwardState
}

// ---------------------------------------------------------------------------
// UpstreamSub / DownstreamSub
// ---------------------------------------------------------------------------

// UpstreamSub represents one subscription the relay holds against a
// publisher: the relay issued a SUBSCRIBE upstream after either a local
// downstream SUBSCRIBE or an explicit PUBLISH / PUBLISH_NAMESPACE from a
// publishing peer.
//
// The Filter is the upstream-side §5.1.2 filter the relay chose for this
// subscription. Per §9.4 the relay typically subscribes upstream with the
// "Largest Object" filter so disparate downstream filters don't churn the
// upstream subscription.
//
// Embedding [Subscription] gives UpstreamSub its state machine, ID, Session,
// Stream, TrackAlias, and ForwardState fields for free.
type UpstreamSub struct {
	Subscription

	// Filter is the §5.1.2 filter the relay used in its upstream
	// SUBSCRIBE. nil means "filter unset" (i.e. the subscription has not
	// been sent yet); once set, the value is owned by the subscription
	// and must not be mutated externally.
	Filter *message.SubscriptionFilter

	// FetchCapable marks an upstream the relay reached via an on-demand
	// SUBSCRIBE (a relay/origin, set in subscribeUpstream) — one expected to
	// answer FETCH, so the FETCH responder may stitch evicted ranges from it.
	// It stays false for a directly-connected leaf publisher, which pushes
	// live objects and does not serve FETCH.
	FetchCapable bool
}

// NewUpstreamSub constructs an UpstreamSub in [SubEstablished] with the given
// identity fields. The relay only builds an UpstreamSub once the upstream
// SUBSCRIBE_OK has arrived (the TrackAlias comes from it), so the
// subscription is live from construction.
func NewUpstreamSub(id uint64, sess *session.Session, stream session.Stream, trackAlias uint64) *UpstreamSub {
	return &UpstreamSub{
		Subscription: Subscription{
			state:      SubEstablished,
			ID:         id,
			Session:    sess,
			Stream:     stream,
			TrackAlias: trackAlias,
		},
	}
}

// SetFilter installs the upstream filter. Callers must not mutate the filter
// after handing it over.
func (u *UpstreamSub) SetFilter(f *message.SubscriptionFilter) {
	u.mu.Lock()
	u.Filter = f
	u.mu.Unlock()
}

// GetFilter returns the currently installed filter (or nil).
func (u *UpstreamSub) GetFilter() *message.SubscriptionFilter {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.Filter
}

// DownstreamSub represents one subscription the relay holds for a
// subscriber: the relay accepted a SUBSCRIBE from the peer and is now
// responsible for forwarding objects, applying the §5.1.2 filter, honouring
// priority (§7) and group order (§5.2), and respecting the Forward flag
// (§9.2).
type DownstreamSub struct {
	Subscription

	// Filter is the §5.1.2 filter the subscriber declared. The fanout
	// consults it on every object to decide whether to forward. nil
	// means "no filter installed" — the relay treats the subscription
	// as unfiltered (delivers every object on the track).
	Filter *message.SubscriptionFilter

	// LargestAtSubscribe is the largest object the relay had observed on
	// this track at the moment the SUBSCRIBE was accepted, per §5.1.2 /
	// §9.4 ("a relay handling a SUBSCRIBE acts as the publisher").
	// FilterLargestObject and FilterNextGroupStart resolve their start
	// location against this snapshot — not against the live, ever-advancing
	// TrackEntry watermark — so the subscription's start is fixed at
	// subscribe time and doesn't drift as new objects arrive.
	LargestAtSubscribe message.Location

	// HasLargestAtSubscribe is false when no objects had been delivered
	// on the track at SUBSCRIBE time. Per §5.1.2, the LargestObject /
	// NextGroupStart filters fall back to {0,0} in that case.
	HasLargestAtSubscribe bool

	// Priority is the §7 Subscriber Priority the peer asked for. Lower
	// numeric values mean higher delivery priority. Folded into the §7.2
	// stream-scheduling key by [DownstreamSub.EffectiveStreamPriority].
	// Default per §7 / §10.2.7 is 128 (mid-range), set in NewDownstreamSub.
	Priority uint8

	// GroupOrder is the §5.2 Group Order preference. Encoded per §10.2.8:
	// 0x1 = ascending, 0x2 = descending. It drives the group-order
	// tie-breaker in both reorder-capable paths (FETCH responses) and the
	// §7.2 rule-3 GroupKey of the subgroup-stream scheduling priority.
	// Default per §7.1: the publisher's preference, which the relay does not
	// currently track, so an unset GroupOrder is left at zero and treated as
	// Ascending.
	GroupOrder uint8
}

// NewDownstreamSub constructs a DownstreamSub in [SubEstablished]: the relay
// accepts the subscriber's SUBSCRIBE (replying SUBSCRIBE_OK) before building
// the sub, so it is live from construction.
//
// Forward State defaults to 1: §10.7 specifies that when the FORWARD
// parameter is omitted from SUBSCRIBE the subscription forwards objects.
// installSubscribeParams overrides this to 0 only when the peer explicitly
// sends FORWARD=0, and REQUEST_UPDATE can flip it later (§9.2 / §10.9).
func NewDownstreamSub(id uint64, sess *session.Session, stream session.Stream, trackAlias uint64) *DownstreamSub {
	return &DownstreamSub{
		Subscription: Subscription{
			state:        SubEstablished,
			ID:           id,
			Session:      sess,
			Stream:       stream,
			TrackAlias:   trackAlias,
			forwardState: 1,
		},
		// §10.2.7: SUBSCRIBER_PRIORITY defaults to 128 (mid-range) when
		// the peer omits the parameter. installSubscribeParams overrides
		// this only when the SUBSCRIBE / REQUEST_UPDATE carries an explicit
		// value (including an explicit 0, the highest priority).
		Priority: 128,
	}
}

// SetFilter installs the downstream filter. Callers must not mutate the
// filter after handing it over.
func (d *DownstreamSub) SetFilter(f *message.SubscriptionFilter) {
	d.mu.Lock()
	d.Filter = f
	d.mu.Unlock()
}

// GetFilter returns the currently installed filter (or nil).
func (d *DownstreamSub) GetFilter() *message.SubscriptionFilter {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Filter
}

// SetPriority records the §7 Subscriber Priority. Updated when the peer
// sends a REQUEST_UPDATE.
func (d *DownstreamSub) SetPriority(p uint8) {
	d.mu.Lock()
	d.Priority = p
	d.mu.Unlock()
}

// GetPriority returns the §7 Subscriber Priority.
func (d *DownstreamSub) GetPriority() uint8 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Priority
}

// SetGroupOrder records the §5.2 Group Order. Updated when the peer sends a
// REQUEST_UPDATE.
func (d *DownstreamSub) SetGroupOrder(o uint8) {
	d.mu.Lock()
	d.GroupOrder = o
	d.mu.Unlock()
}

// GetGroupOrder returns the §5.2 Group Order.
func (d *DownstreamSub) GetGroupOrder() uint8 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.GroupOrder
}

// SetLargestAtSubscribe records the largest-object snapshot captured when
// the subscription was accepted. The fanout feeds this into the §5.1.2
// filter evaluator so LargestObject / NextGroupStart filters resolve
// against a stable subscribe-time anchor rather than the live watermark.
func (d *DownstreamSub) SetLargestAtSubscribe(loc message.Location, hasLargest bool) {
	d.mu.Lock()
	d.LargestAtSubscribe = loc
	d.HasLargestAtSubscribe = hasLargest
	d.mu.Unlock()
}

// EffectiveStreamPriority builds the composite §7.2 scheduling key for one
// subgroup stream of this subscription, which the relay pushes down to the
// transport via [session.PrioritizedSendStream.SetSendPriority].
//
// All four §7.2 rules are encoded in the returned [session.StreamPriority],
// compared lexicographically (lower is higher priority):
//
//  1. Subscriber: this subscription's SUBSCRIBER_PRIORITY (default 128).
//  2. Publisher:  publisherPriority — the byte the subgroup carries
//     (SubgroupHeader.PublisherPriority). The caller passes it because the
//     relay does not cache the per-track default outside the inbound header.
//  3. GroupKey:   groupID with this subscription's GROUP_ORDER applied —
//     bitwise-complemented for Descending so a "lower is higher priority"
//     comparison sends higher Group IDs first.
//  4. Subgroup:   subgroupID — lowest Subgroup ID in a group goes first.
//
// Rules 3+4 only define an ordering between streams of the same request, but
// the transport sees streams from every subscription; §7.2 leaves the
// cross-subscription tie-break implementation-defined, so feeding it the full
// key is conformant and degrades gracefully when the transport projects the
// key onto a coarser knob.
func (d *DownstreamSub) EffectiveStreamPriority(
	publisherPriority uint8,
	groupID, subgroupID uint64,
) session.StreamPriority {
	d.mu.RLock()
	sub := d.Priority
	order := message.GroupOrder(d.GroupOrder)
	d.mu.RUnlock()

	// §7.2 rule 3: Descending order means higher Group IDs are scheduled
	// first. Complementing the Group ID flips the numeric comparison so the
	// same "lower GroupKey is higher priority" rule yields that direction.
	// An unset GROUP_ORDER (zero value) defaults to Ascending — §7.1 says
	// the publisher's preference applies, which the relay does not track.
	groupKey := groupID
	if order == message.GroupOrderDescending {
		groupKey = ^groupID
	}

	return session.StreamPriority{
		Subscriber: sub,
		Publisher:  publisherPriority,
		GroupKey:   groupKey,
		Subgroup:   subgroupID,
	}
}

// PassesFilter reports whether the object at {group, object} matches the
// subscription's §5.1.2 filter. An unset filter (nil) is treated as
// unfiltered: every object passes.
//
// The filter is evaluated against the subscribe-time LargestObject snapshot,
// per §5.1.2 — *not* the live TrackEntry watermark. Re-evaluating against the
// live watermark would let a subscription's effective start location drift
// forward as objects arrive, which would silently drop the very objects the
// subscriber asked to receive.
func (d *DownstreamSub) PassesFilter(group, object uint64) bool {
	d.mu.RLock()
	f := d.Filter
	largest := d.LargestAtSubscribe
	has := d.HasLargestAtSubscribe
	d.mu.RUnlock()
	if f == nil {
		return true
	}
	return f.Matches(group, object, largest.Group, largest.Object, has)
}

// ForwardDecision folds the §9.2 Forward-State gate and the §5.1.2 filter
// test the fanout applies to every object into a single lock acquisition.
// The per-object × per-subscriber loop would otherwise call ForwardState,
// PassesFilter, and (on a miss) GetFilter separately — three RLock round-trips
// on the same mutex per object per subscriber.
//
// forward is true when the object at {group, object} should be enqueued.
// When forward is false, groupExhausted reports whether the subscription has
// narrowed so this whole group is permanently out of range (§11.4.3), so the
// caller can reset the stream promptly instead of leaving it open. A paused
// subscription (Forward State 0) is never groupExhausted — it may resume.
func (d *DownstreamSub) ForwardDecision(group, object uint64) (forward, groupExhausted bool) {
	d.mu.RLock()
	paused := d.forwardState == 0
	f := d.Filter
	largest := d.LargestAtSubscribe
	has := d.HasLargestAtSubscribe
	d.mu.RUnlock()

	if paused {
		return false, false
	}
	if f == nil {
		return true, false
	}
	if f.Matches(group, object, largest.Group, largest.Object, has) {
		return true, false
	}
	return false, GroupOutOfRange(group, f)
}

// GroupOutOfRange reports whether a Subgroup belonging to group is entirely
// outside the subscription's filter range — i.e. no object in that group can
// ever pass — which makes its in-flight stream eligible for a §11.4.3 reset
// (e.g. after a REQUEST_UPDATE narrowed the End Group or raised the Start
// Location to a higher group). Only the absolute filters carry a fixed range;
// the dynamic (LargestObject / NextGroupStart) and unset filters never put a
// whole group permanently out of range, so they return false.
//
// A group equal to the Start Location's group is NOT out of range even when
// the Start Location's Object rose — objects at or above it still pass, so the
// stream stays relevant and object-level filtering handles the boundary.
func GroupOutOfRange(group uint64, f *message.SubscriptionFilter) bool {
	if f == nil {
		return false
	}
	switch f.Type {
	case message.FilterAbsoluteStart:
		return group < f.StartLocation.Group
	case message.FilterAbsoluteRange:
		return group < f.StartLocation.Group || group > f.EndGroup()
	case message.FilterNextGroupStart, message.FilterLargestObject:
		// Dynamic start derived from the largest object; no fixed range that
		// puts a whole group permanently out of range.
		return false
	default:
		return false
	}
}

// TerminateWithPublishDone gracefully ends this downstream subscription
// per §10.11: the relay writes a PUBLISH_DONE message on the
// subscriber's request stream and FINs the send side. The
// subscriber's handler goroutine, which is blocked in
// [session.DrainAndWait] on the same stream, sees EOF and exits; its
// defer evicts the [DownstreamSub] from the [TrackRegistry].
//
// The Terminate latch prevents double-termination: the first caller
// flips the state and writes the message; subsequent calls return
// without I/O. Safe to call concurrently from any goroutine.
//
// streamCount is the §10.11 "Stream Count" field — the number of
// subgroup streams the relay opened for this subscription. Pass 0
// when the exact count isn't tracked; subscribers treat 0 as
// approximate per the spec.
//
// Used by [TrackRegistry] when the last upstream feeding a track
// disappears, so dependent subscribers stop waiting silently.
func (d *DownstreamSub) TerminateWithPublishDone(code moqt.PublishDoneCode, reason string, streamCount uint64) {
	if !d.Terminate() {
		return // already terminated
	}
	if d.Stream == nil {
		return
	}
	_ = message.Marshal(d.Stream, &message.PublishDone{
		StatusCode:  code,
		StreamCount: streamCount,
		ErrorReason: reason,
	})
	_ = d.Stream.Close()
}
