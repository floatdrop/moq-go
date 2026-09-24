package session

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// AllocOutboundTrackAlias returns the next Track Alias to use when this side
// advertises a new track to the peer (§11.1). Aliases are independent across
// sessions, so callers must remap when forwarding between two sessions.
//
// Allocation starts at 1, never 0: [Session.Publish] and the SUBSCRIBE_OK
// reply path treat a zero TrackAlias as "unset, allocate one for me".
// ([Session.OpenPublish] does not: its caller must allocate.) If this allocator returned 0, a caller that did the natural
// "alias := AllocOutboundTrackAlias(); Publish(&Publish{TrackAlias: alias})"
// would have its 0 silently re-allocated to a different value — and any data
// stream the caller then opened under the original 0 would carry an alias the
// peer never bound to the track (the relay drops it as an unknown alias). So 0
// is reserved as the sentinel and never handed out.
func (s *Session) AllocOutboundTrackAlias() uint64 {
	return s.nextOutboundTrackAlias.Add(1)
}

// ErrDuplicateTrackAlias is returned by RegisterInboundTrackAlias when the
// peer assigns a Track Alias that is already in use for a different track
// (§11.1). The caller MUST close the session with SessionDuplicateTrackAlias.
type ErrDuplicateTrackAlias struct {
	Alias    uint64
	Existing track.Key
	New      track.Key
}

func (e *ErrDuplicateTrackAlias) Error() string {
	return fmt.Sprintf(
		"moqt/session: Track Alias %d already in use for a different track — DUPLICATE_TRACK_ALIAS",
		e.Alias,
	)
}

// InboundTrack is what an inbound Track Alias is bound to (§11.1).
type InboundTrack struct {
	Key track.Key

	// DefaultPublisherPriority is the DEFAULT_PUBLISHER_PRIORITY (§12.4) in the
	// Track Properties of the SUBSCRIBE_OK or PUBLISH that bound the alias, or
	// 128 when omitted. Subgroups and datagrams sent with the DEFAULT_PRIORITY
	// bit inherit it (§11.4.2, §11.3.1). It is captured together with the
	// alias, so a data stream that resolves the alias always sees the value of
	// the control message that established it.
	DefaultPublisherPriority uint8
}

// RegisterInboundTrack records that the peer has assigned alias to the track
// identified by key, along with the Track Properties of the message that did
// so. This MUST be called by the subscriber when it receives a SUBSCRIBE_OK
// (whose TrackAlias field is the alias) and by the server when it receives a
// PUBLISH (whose TrackAlias field is the alias).
//
// If alias is already registered for the same track (idempotent re-registration),
// nil is returned and the first registration is kept. If alias is already
// registered for a different track, *ErrDuplicateTrackAlias is returned and the
// caller MUST close the session with SessionDuplicateTrackAlias (§11.1).
func (s *Session) RegisterInboundTrack(alias uint64, key track.Key, trackProperties []byte) error {
	in := InboundTrack{
		Key:                      key,
		DefaultPublisherPriority: message.TrackDefaultPublisherPriority(trackProperties),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.inboundAliases[alias]; ok {
		if existing.Key != key {
			return &ErrDuplicateTrackAlias{Alias: alias, Existing: existing.Key, New: key}
		}
		return nil // idempotent
	}
	s.inboundAliases[alias] = in
	return nil
}

// RegisterInboundTrackAlias is [Session.RegisterInboundTrack] for a message
// that carried no Track Properties.
func (s *Session) RegisterInboundTrackAlias(alias uint64, key track.Key) error {
	return s.RegisterInboundTrack(alias, key, nil)
}

// UnregisterInboundTrackAlias removes a previously registered alias, freeing
// it for potential reuse. Callers should invoke this when the subscription or
// publication associated with alias has been fully torn down (e.g. after
// PUBLISH_DONE or subscription cancellation and a suitable grace period per
// §11.1: "Subscribers SHOULD retain sufficient state to quickly discard
// unwanted Objects").
//
// Unregistering an alias that was never registered is a no-op.
func (s *Session) UnregisterInboundTrackAlias(alias uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.inboundAliases, alias)
}

// LookupInboundTrack returns what alias was bound to by an earlier
// [Session.RegisterInboundTrack] call, or (zero, false) if the alias is not
// currently registered.
//
// Inbound data streams (SUBGROUP_HEADER, ObjectDatagram, FETCH_HEADER objects)
// identify their track by the alias the publisher chose; consumers — most
// notably the relay's fanout and end-subscriber applications — use this
// method to recover the canonical track identity for routing or rendering.
func (s *Session) LookupInboundTrack(alias uint64) (InboundTrack, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inboundAliases[alias]
	return in, ok
}

// LookupInboundTrackAlias is [Session.LookupInboundTrack] reduced to the
// track.Key.
func (s *Session) LookupInboundTrackAlias(alias uint64) (track.Key, bool) {
	in, ok := s.LookupInboundTrack(alias)
	return in.Key, ok
}
