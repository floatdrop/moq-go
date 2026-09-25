package registry

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

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
// Those are fanout concerns expressed via the [message.LocationFilter]
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
	// that opened this subscription's request stream, kept for identity
	// and diagnostics. (A REQUEST_UPDATE rides the same stream but consumes
	// a fresh ID from the sender's space, §10.1 — the stream, not the ID,
	// names the request being updated.) Set once at construction; read-only.
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
// the value here — §10.2.18's FORWARD is canonically 0 or 1, but allowing
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
	Filter *message.LocationFilter

	// FetchCapable marks an upstream the relay reached via an on-demand
	// SUBSCRIBE (a relay/origin, set in subscribeUpstream) — one expected to
	// answer FETCH, so the FETCH responder may stitch evicted ranges from it.
	// It stays false for a directly-connected leaf publisher, which pushes
	// live objects and does not serve FETCH.
	FetchCapable bool

	// OnDemand marks an upstream subscription the relay itself opened via
	// SUBSCRIBE to serve downstream subscribers (§9.4 aggregation). Such a
	// subscription exists only for its downstreams: when the last one
	// leaves, the registry tears it down ([UpstreamSub.CloseOnDemand]) so
	// the publisher stops streaming into a void. It stays false for
	// PUBLISH-fed upstreams, whose stream is owned by the publisher.
	OnDemand bool

	// Broker owns the request stream's read side (via
	// [session.RequestBroker.Serve], run by the relay's per-upstream reader
	// goroutine) and serializes every relay write on the stream — §10.9
	// REQUEST_UPDATEs via [UpstreamSub.Update] and other control messages
	// via [UpstreamSub.WriteMessage] must not interleave. nil only for
	// literal-constructed test fixtures; [NewUpstreamSub] always builds one.
	Broker *session.RequestBroker

	// done is the PUBLISH_DONE the upstream sent, nil until it does. Guarded
	// by the embedded Subscription's mu.
	done *message.PublishDone
}

// SetPublishDone records the PUBLISH_DONE the upstream ended this
// subscription with (§10.12); see [DownstreamDoneCode].
func (u *UpstreamSub) SetPublishDone(pd *message.PublishDone) {
	u.mu.Lock()
	u.done = pd
	u.mu.Unlock()
}

// publishDone returns what [UpstreamSub.SetPublishDone] recorded.
func (u *UpstreamSub) publishDone() *message.PublishDone {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.done
}

// DownstreamDoneCode is the PUBLISH_DONE status code the relay sends its
// subscribers when a track's last upstream ended with upstream (nil: it ended
// without one — reset, or its session went away). §10.12: "The application
// SHOULD use a relevant status code". A code about the track passes through;
// one about the relay's own upstream subscription (it fell behind, its update
// failed, it expired, it lost its authorization, the upstream was overloaded
// or going away) says nothing true about the subscriber's, so it becomes
// INTERNAL_ERROR, as does a code this relay does not know. An upstream gone
// without PUBLISH_DONE ends the track as far as the relay can tell.
func DownstreamDoneCode(upstream *message.PublishDone) moqt.PublishDoneCode {
	if upstream == nil {
		return moqt.PublishDoneTrackEnded
	}
	switch upstream.StatusCode {
	case moqt.PublishDoneTrackEnded, moqt.PublishDoneMalformedTrack:
		return upstream.StatusCode
	case moqt.PublishDoneInternalError, moqt.PublishDoneUnauthorized, moqt.PublishDoneGoingAway,
		moqt.PublishDoneTooFarBehind, moqt.PublishDoneExpired, moqt.PublishDoneUpdateFailed,
		moqt.PublishDoneExcessiveLoad:
		return moqt.PublishDoneInternalError
	}
	return moqt.PublishDoneInternalError // a code this relay does not know
}

// updateResponseTimeout bounds the wait for the §10.9 REQUEST_OK /
// REQUEST_ERROR after Update writes a REQUEST_UPDATE. A conforming peer
// always answers; the bound keeps a peer that never does from wedging the
// dispatch loop the Update call runs on.
const updateResponseTimeout = 5 * time.Second

// Update sends a REQUEST_UPDATE (§10.9) on the upstream request stream and
// awaits the single REQUEST_OK / REQUEST_ERROR the spec mandates, bounded
// by [updateResponseTimeout] (tightened further by any earlier deadline on
// ctx). It delegates to the sub's [session.RequestBroker]; the response is
// delivered by the relay's per-upstream Serve loop. A REQUEST_ERROR is
// surfaced as a [session.RequestRejectedError]; a closed stream as
// [session.ErrRequestStreamClosed].
func (u *UpstreamSub) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	if u.Broker == nil {
		return nil, session.ErrRequestStreamClosed
	}
	ctx, cancel := context.WithTimeout(ctx, updateResponseTimeout)
	defer cancel()
	return u.Broker.Update(ctx, params)
}

// WriteMessage marshals a control message onto the upstream request stream
// under the broker's write lock — the same lock that serializes Update's
// REQUEST_UPDATE writes. session.Stream does not serialize concurrent
// writers, so every relay write on this stream after the request is
// accepted must go through here or Update.
func (u *UpstreamSub) WriteMessage(msg message.Message) error {
	if u.Broker == nil {
		return session.ErrRequestStreamClosed
	}
	return u.Broker.WriteMessage(msg)
}

// CloseOnDemand tears down an on-demand upstream subscription after its
// last downstream left by cancelling the request: pending updates fail fast
// and both directions are reset — §5.1: "The subscriber terminates a
// subscription ... by sending STOP_SENDING". The broker's Serve loop observes
// the reset and exits, and the publisher stops streaming into a void.
// Idempotent; must be called without registry locks held (stream I/O).
func (u *UpstreamSub) CloseOnDemand() {
	u.Cancel(moqt.StreamResetCancelled)
}

// Cancel ends the relay's subscription to this upstream — an on-demand
// SUBSCRIBE or an accepted PUBLISH — by resetting both directions of its
// request stream with code (§3.3.3); the broker's Serve loop then exits and
// its owner unregisters the upstream. Idempotent; must be called without
// registry locks held (stream I/O).
func (u *UpstreamSub) Cancel(code moqt.StreamResetCode) {
	u.Terminate()
	if u.Broker == nil {
		return
	}
	u.Broker.Close(code)
}

// NewUpstreamSub constructs an UpstreamSub in [SubEstablished] with the given
// identity fields. The relay only builds an UpstreamSub once the upstream
// SUBSCRIBE_OK has arrived (the TrackAlias comes from it), so the
// subscription is live from construction.
//
// requestID is the §10.1 Request ID of the SUBSCRIBE / PUBLISH that opened
// the request stream, recorded for identity and diagnostics.
//
// The Forward State starts at 1: per §10.7 a SUBSCRIBE (or accepted PUBLISH)
// that omits the FORWARD parameter implies Forward State 1, and the relay's
// upstream requests never carry FORWARD. Starting at 0 would make the §9.2
// propagation path emit a spurious REQUEST_UPDATE(Forward=1) on the first
// downstream resume.
//
// peerMayUpdate says whether the upstream publisher may send REQUEST_UPDATE on
// the stream: §10.9 allows it only from "The sender of a request", so true for
// an accepted PUBLISH and false for the relay's own SUBSCRIBE. The publisher
// may always send PUBLISH_STATE_NOTIFY (§10.10). A disallowed follow-up closes
// the session with PROTOCOL_VIOLATION. It is a parameter, not a later call, so
// no upstream path can leave the broker permissive by omission.
func NewUpstreamSub(
	id uint64,
	sess *session.Session,
	stream session.Stream,
	trackAlias, requestID uint64,
	peerMayUpdate bool,
) *UpstreamSub {
	broker := sess.NewRequestBroker(stream)
	broker.PeerMessages(peerMayUpdate, true)
	// The only peer that may update here is the publisher of an accepted
	// PUBLISH (§10.9), so its REQUEST_UPDATEs carry a publisher's scope.
	broker.UpdateScope(message.ScopeUpdateFromPublisher)
	return &UpstreamSub{
		state:        SubEstablished,
		ID:           id,
		RequestID:    requestID,
		Session:      sess,
		Stream:       stream,
		TrackAlias:   trackAlias,
		forwardState: 1,
		Broker:       broker,
	}
}

// SetFilter installs the upstream filter. Callers must not mutate the filter
// after handing it over.
func (u *UpstreamSub) SetFilter(f *message.LocationFilter) {
	u.mu.Lock()
	u.Filter = f
	u.mu.Unlock()
}

// GetFilter returns the currently installed filter (or nil).
func (u *UpstreamSub) GetFilter() *message.LocationFilter {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.Filter
}

// DownstreamSub represents one subscription the relay holds for a
// subscriber: the relay accepted a SUBSCRIBE from the peer and is now
// responsible for forwarding objects, applying the §5.1.2 filter, honouring
// priority (§7) and group order (§10.2.8), and respecting the Forward flag
// (§9.2).
type DownstreamSub struct {
	Subscription

	// writeMu serializes control-message writes on Stream.
	// session.Stream does not serialize concurrent writers and one
	// Marshal is multiple stream Writes, but two goroutines legitimately
	// write here: the subscriber's request handler (SUBSCRIBE_OK,
	// REQUEST_OK / REQUEST_ERROR replies — via WriteMessage) and registry
	// teardown goroutines (PUBLISH_DONE via TerminateWithPublishDone,
	// triggered by a *publisher* leaving).
	writeMu sync.Mutex

	// okSent records that the §10.8 SUBSCRIBE_OK response went out on the
	// stream, or that the relay's own PUBLISH opened it (OpenedByPublish);
	// guarded by writeMu. A termination racing the subscribe
	// handler consults it to answer the request correctly: the peer must
	// receive exactly one SUBSCRIBE_OK / REQUEST_ERROR before any
	// PUBLISH_DONE — a PUBLISH_DONE with no prior response leaves the
	// request permanently unanswered on the subscriber side.
	okSent bool

	// Filter is the §5.1.2 filter the subscriber declared. The fanout
	// consults it on every object to decide whether to forward. nil
	// means "no filter installed" — the relay treats the subscription
	// as unfiltered (delivers every object on the track).
	Filter *message.LocationFilter

	// rangeFilters holds the §5.1.4 Range Filters the subscriber declared
	// (Subgroup ID / Object ID / Publisher Priority / Object Property). The
	// fanout ANDs them with Filter per object. nil = no range restriction.
	// Guarded by mu; access via SetRangeFilters / GetRangeFilters.
	rangeFilters *message.RangeFilterSet

	// deliveryTimeouts holds the §10.2.3 / §10.2.4 values the subscriber asked
	// for. The fanout resolves them against the publisher's Track-level pair
	// (§8: the smaller of the two non-zero values) once per subgroup stream it
	// opens. Guarded by mu; access via SetDeliveryTimeouts / GetDeliveryTimeouts.
	deliveryTimeouts message.DeliveryTimeouts

	// LargestAtSubscribe is the largest object the relay had observed on
	// this track at the moment the SUBSCRIBE was accepted, per §5.1.2 /
	// §9.4, the relay acting as the publisher for its downstream subscribers.
	// The Next Object and relative-start filters resolve their start
	// location against this snapshot — not against the live, ever-advancing
	// TrackEntry watermark — so the subscription's start is fixed at
	// subscribe time and doesn't drift as new objects arrive.
	LargestAtSubscribe message.Location

	// HasLargestAtSubscribe is false when no objects had been delivered
	// on the track at SUBSCRIBE time. Per §5.1.2, the Next Object and
	// relative-start filters fall back to {0,0} in that case.
	HasLargestAtSubscribe bool

	// Priority is the §7 Subscriber Priority the peer asked for. Lower
	// numeric values mean higher delivery priority. Folded into the §7.2
	// stream-scheduling key by [DownstreamSub.EffectiveStreamPriority].
	// Default per §7 / §10.2.7 is 128 (mid-range), set in NewDownstreamSub.
	Priority uint8

	// GroupOrder is the Group Order preference (§7, §10.2.8), encoded as
	// 0x1 = ascending, 0x2 = descending. It drives the group-order
	// tie-breaker in both reorder-capable paths (FETCH responses) and the
	// §7.2 rule-3 GroupKey of the subgroup-stream scheduling priority.
	// Default per §7.1: the publisher's preference, which the relay does not
	// currently track, so an unset GroupOrder is left at zero and treated as
	// Ascending.
	GroupOrder uint8

	// omitProperties records INCLUDE_PROPERTIES=0; see
	// [DownstreamSub.IncludesProperties].
	omitProperties atomic.Bool

	// streamsOpened counts the data streams opened for this subscription —
	// subgroup streams and fill fetch streams — for the §10.12 PUBLISH_DONE
	// Stream Count; streamsOpening counts opens in flight, and streamsOpen the
	// opened streams not yet closed. pendingDone is a PUBLISH_DONE waiting for
	// them. Guarded by mu, the same lock as the lifecycle state, so no stream
	// is counted after termination. See [DownstreamSub.BeginStream].
	streamsOpened  uint64
	streamsOpening int
	streamsOpen    int
	// datagramsSending counts datagram sends in flight; see
	// [DownstreamSub.BeginDatagram].
	datagramsSending int
	pendingDone      *pendingPublishDone
}

// pendingPublishDone is a termination's PUBLISH_DONE, held until the last of
// the subscription's streams closes.
type pendingPublishDone struct {
	code   moqt.PublishDoneCode
	reason string
}

// BeginStream reserves the open of one data stream for this subscription so
// PUBLISH_DONE can report the §10.12 Stream Count. It returns false once the
// subscription is terminated: the caller must not open the stream. Each true
// must be paired with one [DownstreamSub.EndStream].
func (d *DownstreamSub) BeginStream() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == SubTerminated {
		return false
	}
	d.streamsOpening++
	return true
}

// EndStream completes a [DownstreamSub.BeginStream], counting the stream if
// it was opened. An opened stream must later be reported to
// [DownstreamSub.StreamClosed].
func (d *DownstreamSub) EndStream(opened bool) {
	d.mu.Lock()
	d.streamsOpening--
	if opened {
		d.streamsOpened++
		d.streamsOpen++
	}
	done, count := d.takeReadyDoneLocked()
	d.mu.Unlock()
	d.sendPublishDone(done, count)
}

// StreamClosed reports that one of the subscription's opened data streams has
// been closed (FIN) or reset. A PUBLISH_DONE waiting for it goes out once it
// is the last (§10.12).
func (d *DownstreamSub) StreamClosed() {
	d.mu.Lock()
	d.streamsOpen--
	done, count := d.takeReadyDoneLocked()
	d.mu.Unlock()
	d.sendPublishDone(done, count)
}

// BeginDatagram reserves one datagram send for this subscription: §10.12's
// PUBLISH_DONE may go out only once the sender "has no further datagrams to
// send". It returns false once the subscription is terminated, and the caller
// must not send. Each true must be paired with one [DownstreamSub.EndDatagram].
func (d *DownstreamSub) BeginDatagram() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == SubTerminated {
		return false
	}
	d.datagramsSending++
	return true
}

// EndDatagram completes a [DownstreamSub.BeginDatagram].
func (d *DownstreamSub) EndDatagram() {
	d.mu.Lock()
	d.datagramsSending--
	done, count := d.takeReadyDoneLocked()
	d.mu.Unlock()
	d.sendPublishDone(done, count)
}

// takeReadyDoneLocked returns the pending PUBLISH_DONE and its Stream Count
// once no stream of the subscription is open or opening and no datagram is
// being sent, clearing it; nil otherwise. With no open in flight the count is
// exact. The caller holds mu.
func (d *DownstreamSub) takeReadyDoneLocked() (*pendingPublishDone, uint64) {
	if d.pendingDone == nil || d.streamsOpen > 0 || d.streamsOpening > 0 || d.datagramsSending > 0 {
		return nil, 0
	}
	done := d.pendingDone
	d.pendingDone = nil
	return done, d.streamsOpened
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
		state:        SubEstablished,
		ID:           id,
		Session:      sess,
		Stream:       stream,
		TrackAlias:   trackAlias,
		forwardState: 1,
		// §10.2.7: SUBSCRIBER_PRIORITY defaults to 128 (mid-range) when
		// the peer omits the parameter. installSubscribeParams overrides
		// this only when the SUBSCRIBE / REQUEST_UPDATE carries an explicit
		// value (including an explicit 0, the highest priority).
		Priority: 128,
	}
}

// SetFilter installs the downstream filter. Callers must not mutate the
// filter after handing it over.
func (d *DownstreamSub) SetFilter(f *message.LocationFilter) {
	d.mu.Lock()
	d.Filter = f
	d.mu.Unlock()
}

// GetFilter returns the currently installed filter (or nil).
func (d *DownstreamSub) GetFilter() *message.LocationFilter {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.Filter
}

// SetDeliveryTimeouts records the §10.2.3 / §10.2.4 delivery timeouts the
// subscriber asked for. A zero dimension means "no timeout" per §8.
func (d *DownstreamSub) SetDeliveryTimeouts(t message.DeliveryTimeouts) {
	d.mu.Lock()
	d.deliveryTimeouts = t
	d.mu.Unlock()
}

// GetDeliveryTimeouts returns the delivery timeouts the subscriber asked for.
// The zero value means the subscriber requested none, which leaves the
// publisher's Track-level values to stand on their own (§8).
func (d *DownstreamSub) GetDeliveryTimeouts() message.DeliveryTimeouts {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.deliveryTimeouts
}

// GetRangeFilters returns the subscription's current §5.1.4 Range Filters, nil
// when it has none.
func (d *DownstreamSub) GetRangeFilters() *message.RangeFilterSet {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rangeFilters
}

// SetRangeFilters installs the subscription's Range Filters (§5.1.4), which the
// fanout ANDs with the Location filter and Forward gate per object (read
// directly under mu by ForwardDecision). nil clears them (no range restriction).
func (d *DownstreamSub) SetRangeFilters(f *message.RangeFilterSet) {
	d.mu.Lock()
	d.rangeFilters = f
	d.mu.Unlock()
}

// SetPriority records the §7 Subscriber Priority. Updated when the peer
// sends a REQUEST_UPDATE.
func (d *DownstreamSub) SetPriority(p uint8) {
	d.mu.Lock()
	d.Priority = p
	d.mu.Unlock()
}

// SetIncludeProperties records INCLUDE_PROPERTIES (§10.2.21), set once from
// the SUBSCRIBE or SUBSCRIBE_TRACKS.
func (d *DownstreamSub) SetIncludeProperties(include bool) { d.omitProperties.Store(!include) }

// IncludesProperties reports whether the subscriber wants Track Properties
// (INCLUDE_PROPERTIES omitted or 1). One that does not also lacks the track's
// DEFAULT_PUBLISHER_PRIORITY (§12.4), so its subgroups and datagrams carry the
// priority inline.
func (d *DownstreamSub) IncludesProperties() bool { return !d.omitProperties.Load() }

// SetGroupOrder records the Group Order (§10.2.8), set once from the SUBSCRIBE:
// "The group order of an existing subscription cannot be changed" (§7.1), and
// GROUP_ORDER is not in a REQUEST_UPDATE's scope.
func (d *DownstreamSub) SetGroupOrder(o uint8) {
	d.mu.Lock()
	d.GroupOrder = o
	d.mu.Unlock()
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
//     (SubgroupHeader.PublisherPriority), already resolved to the track's
//     §12.4 default when the header omits it.
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

// ForwardVerdict is [DownstreamSub.ForwardDecision]'s answer for one Object.
// §11.4.3 lets a subgroup stream end with a FIN only when it carried every
// Object of the Subgroup "except any Objects with Locations smaller than the
// subscription's Start Location"; every other skip leaves the Subgroup
// incomplete for this subscription, so its stream must end with a reset.
type ForwardVerdict uint8

const (
	// Forward: enqueue the Object.
	Forward ForwardVerdict = iota
	// SkipBeforeStart: the Object lies before the subscription's Start
	// Location — the one omission §11.4.3 still allows a FIN after, unless
	// the Start was raised past Objects already sent (the caller can tell).
	SkipBeforeStart
	// SkipObject: a filter drops this Object only (a Range Filter, or the
	// Location filter past its End); a later one in the group may still
	// pass. The Subgroup is incomplete for this subscription.
	SkipObject
	// SkipPaused: Forward State 0 omits the Object (§5.1: the publisher does
	// not send Objects while it is 0; §5.1.5 treats Forward as a filter).
	// The Subgroup is incomplete for this subscription (§11.4.3: "Omitting a
	// Subgroup Object due to the subscriber's Forward State").
	SkipPaused
	// SkipGroup: the Location filter puts this whole group permanently out of
	// range — it lies before an absolute Start or past the End (§11.4.3); the
	// stream can be reset promptly.
	SkipGroup
	// SkipEnded: the subscription is terminated and takes no new Object
	// (§10.12).
	SkipEnded
)

// ForwardDecision decides whether an Object goes to this subscription, under
// one lock acquisition — the fanout asks it for every Object and every
// subscriber. It ANDs the Forward State, the §5.1.2 Location filter, and the
// §5.1.4 Range Filters (subgroupID/object/priority/objProps) — §5.1.5 "Pass =
// Forward AND Location AND Range" — after the lifecycle state. A Range-filter
// miss drops only the Object, so it is SkipObject; only the Location filter
// can make it SkipGroup or SkipBeforeStart.
//
// The Location filter is evaluated against the subscribe-time LargestObject
// snapshot, *not* the live TrackEntry watermark. Re-evaluating against the
// live watermark would let a subscription's effective start location drift
// forward as objects arrive, silently dropping the very objects the
// subscriber asked to receive.
func (d *DownstreamSub) ForwardDecision(
	group, object, subgroupID uint64, priority uint8, objProps []byte,
) ForwardVerdict {
	d.mu.RLock()
	ended := d.state == SubTerminated
	paused := d.forwardState == 0
	f := d.Filter
	rf := d.rangeFilters
	largest := d.LargestAtSubscribe
	has := d.HasLargestAtSubscribe
	d.mu.RUnlock()

	switch {
	case ended:
		return SkipEnded
	case paused:
		return SkipPaused
	}
	// Location filter first, so its group-exhaustion signal (§11.4.3) governs:
	// a whole group out of range (below a raised Start as much as past a
	// narrowed End) resets the stream promptly.
	loc := message.Location{Group: group, Object: object}
	if f != nil && !f.Matches(loc, largest, has) {
		switch {
		case GroupOutOfRange(group, f):
			return SkipGroup
		case loc.Less(f.Start(largest, has)):
			return SkipBeforeStart
		}
		return SkipObject
	}
	// Range Filters (§5.1.4): per-object AND; a miss drops the object only.
	if rf != nil && !rf.MatchesObject(subgroupID, object, priority, objProps) {
		return SkipObject
	}
	return Forward
}

// GroupOutOfRange reports whether a Subgroup belonging to group is entirely
// outside the subscription's filter range — i.e. no object in that group can
// ever pass — which makes its in-flight stream eligible for a §11.4.3 reset
// (e.g. after a REQUEST_UPDATE narrowed the End Group or raised the Start
// Location to a higher group). Only the absolute filters carry a fixed range;
// the dynamic (Next Object / relative-start) and unset filters never put a
// whole group permanently out of range, so they return false.
//
// A group equal to the Start Location's group is NOT out of range even when
// the Start Location's Object rose — objects at or above it still pass, so the
// stream stays relevant and object-level filtering handles the boundary.
func GroupOutOfRange(group uint64, f *message.LocationFilter) bool {
	if f == nil {
		return false
	}
	if f.Unfiltered() || f.RelativeStart() || f.NextObject() {
		// Start derived from the largest object (or absent entirely); no fixed
		// range that puts a whole group permanently out of range.
		return false
	}
	if group < f.StartGroup {
		return true
	}
	end, ok := f.End()
	return ok && group > end.Group
}

// TerminateWithPublishDone gracefully ends this downstream subscription
// per §10.12: the relay writes a PUBLISH_DONE message on the
// subscriber's request stream and FINs the send side. That ends the
// handler's wait (the stream's send Context), whether or not the subscriber
// has FINned its own side, and its defer evicts the [DownstreamSub] from the
// [TrackRegistry].
//
// If the SUBSCRIBE_OK never went out — the sub is registered (and thus
// reachable by teardown) before the handler replies, so a terminator can
// win that race — a PUBLISH_DONE would leave the SUBSCRIBE without the
// single SUBSCRIBE_OK / REQUEST_ERROR response §10.7 requires. In that
// case the termination answers the request with REQUEST_ERROR
// (DOES_NOT_EXIST: the track's source vanished before the subscription
// was established) instead, and [DownstreamSub.WriteSubscribeOK] refuses
// to send the stale OK afterwards.
//
// The Terminate latch prevents double-termination: the first caller
// flips the state; subsequent calls do nothing. Safe to call concurrently
// from any goroutine, and it does no I/O itself: the answer is written on
// its own goroutine (see [DownstreamSub.sendPublishDone]).
//
// "A sender MUST NOT send PUBLISH_DONE until it has closed all streams it
// will ever open" (§10.12): the latch stops new streams, and the answer
// waits until every stream already opened or opening has closed, reported
// through [DownstreamSub.StreamClosed]. Its Stream Count is then exact: the
// number of data streams opened for this subscription, as tracked by
// [DownstreamSub.BeginStream].
//
// Used by [TrackRegistry] when the last upstream feeding a track
// disappears, so dependent subscribers stop waiting silently.
func (d *DownstreamSub) TerminateWithPublishDone(code moqt.PublishDoneCode, reason string) {
	d.mu.Lock()
	if d.state == SubTerminated {
		d.mu.Unlock()
		return // already terminated
	}
	d.state = SubTerminated
	d.pendingDone = &pendingPublishDone{code: code, reason: reason}
	done, count := d.takeReadyDoneLocked()
	d.mu.Unlock()
	d.sendPublishDone(done, count)
}

// sendPublishDone answers the terminated request on its own goroutine, so a
// subscriber that does not read its request stream delays only its own
// answer, not the callers terminating many subscriptions in a row. Before
// SUBSCRIBE_OK the answer is REQUEST_ERROR (DOES_NOT_EXIST: the track's source
// vanished first) instead of PUBLISH_DONE. A nil done is a no-op.
func (d *DownstreamSub) sendPublishDone(done *pendingPublishDone, streamCount uint64) {
	if done == nil || d.Stream == nil {
		return
	}
	go func() {
		d.writeMu.Lock()
		defer d.writeMu.Unlock()
		if !d.okSent {
			_ = message.Marshal(d.Stream, &message.RequestError{
				ErrorCode:   moqt.RequestDoesNotExist,
				ErrorReason: done.reason,
			})
			// Mirror [session.Request.RejectError]: the losing subscribe
			// handler returns without ever entering its follow-up read
			// loop, so cancel the read side too — otherwise bytes the peer
			// sends before seeing the rejection queue in the transport
			// forever.
			d.Stream.CancelRead(uint64(moqt.StreamResetInternalError))
		} else {
			_ = message.Marshal(d.Stream, &message.PublishDone{
				StatusCode:  done.code,
				StreamCount: streamCount,
				ErrorReason: done.reason,
			})
		}
		_ = d.Stream.Close()
	}()
}

// WriteSubscribeOK writes the §10.8 SUBSCRIBE_OK response under the write
// lock and records that the request now has its response, so a later
// termination emits PUBLISH_DONE (§10.12) rather than a second response.
// If a termination won the race first, it returns
// [ErrSubscriptionTerminated] without writing — the termination answers the
// request with REQUEST_ERROR instead.
func (d *DownstreamSub) WriteSubscribeOK(msg *message.SubscribeOK) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.IsTerminated() {
		return ErrSubscriptionTerminated
	}
	if err := message.Marshal(d.Stream, msg); err != nil {
		return err
	}
	d.okSent = true
	return nil
}

// OpenedByPublish records that the relay's own PUBLISH opened this
// subscription (a PUBLISH forwarded to a SUBSCRIBE_TRACKS holder, §6.1), so,
// like one answered with SUBSCRIBE_OK, it ends with PUBLISH_DONE (§10.12)
// rather than REQUEST_ERROR. Call it before registering the subscription.
func (d *DownstreamSub) OpenedByPublish() {
	d.writeMu.Lock()
	d.okSent = true
	d.writeMu.Unlock()
}

// EndRefused ends a subscription its subscriber refused (REQUEST_ERROR to the
// relay's PUBLISH, §10.11): it is terminated without a PUBLISH_DONE, and the
// stream is closed in both directions, under the same lock as every other
// write on it. A termination already waiting on the subscription's streams
// (see [DownstreamSub.TerminateWithPublishDone]) is cancelled: the refusal
// ended the request first as far as the subscriber is concerned.
func (d *DownstreamSub) EndRefused() {
	d.mu.Lock()
	ended := d.state != SubTerminated || d.pendingDone != nil
	d.state = SubTerminated
	d.pendingDone = nil
	d.mu.Unlock()
	if !ended {
		return // already answered
	}
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	_ = d.Stream.Close()
	d.Stream.CancelRead(uint64(moqt.StreamResetCancelled))
}

// WriteMessage marshals a control message onto the downstream request stream
// under the same lock TerminateWithPublishDone uses. Every relay write on
// this stream after the DownstreamSub is registered must go through here —
// registration makes the sub reachable by registry teardown goroutines, so
// even the SUBSCRIBE_OK reply can otherwise interleave with a PUBLISH_DONE.
//
// A write after termination fails with ErrSubscriptionTerminated: the
// termination's PUBLISH_DONE + FIN is the last thing on this stream, whether
// it has gone out yet or is waiting on the subscription's data streams.
func (d *DownstreamSub) WriteMessage(msg message.Message) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()
	if d.IsTerminated() {
		return ErrSubscriptionTerminated
	}
	return message.Marshal(d.Stream, msg)
}

// ErrSubscriptionTerminated is returned by [DownstreamSub.WriteMessage] when
// the subscription has been terminated; its PUBLISH_DONE has gone out or will
// once its streams close.
var ErrSubscriptionTerminated = errors.New("registry: subscription terminated")
