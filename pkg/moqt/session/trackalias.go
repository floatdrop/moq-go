package session

import (
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// AllocOutboundTrackAlias returns the next Track Alias to use when this side
// advertises a new track to the peer (§11.1). Aliases are independent across
// sessions, so callers must remap when forwarding between two sessions.
//
// Allocation starts at 1, never 0: [Session.Publish], [Session.OpenPublish],
// and the SUBSCRIBE_OK reply path treat a zero TrackAlias as "unset, allocate
// one for me". If this allocator returned 0, a caller that did the natural
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

// RegisterInboundTrackAlias records that the peer has assigned alias to the
// track identified by key. This MUST be called by the subscriber when it
// receives a SUBSCRIBE_OK (whose TrackAlias field is the alias) and by the
// server when it receives a PUBLISH (whose TrackAlias field is the alias).
//
// If alias is already registered for the same track (idempotent re-registration),
// nil is returned. If alias is already registered for a different track,
// *ErrDuplicateTrackAlias is returned and the caller MUST close the session
// with SessionDuplicateTrackAlias (§11.1).
func (s *Session) RegisterInboundTrackAlias(alias uint64, key track.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.inboundAliases[alias]; ok {
		if existing != key {
			return &ErrDuplicateTrackAlias{Alias: alias, Existing: existing, New: key}
		}
		return nil // idempotent
	}
	s.inboundAliases[alias] = key
	return nil
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

// LookupInboundTrackAlias returns the track.Key bound to alias by an earlier
// [Session.RegisterInboundTrackAlias] call, or (zero, false) if the alias is
// not currently registered.
//
// This is the recipient-side companion of [Session.RegisterInboundTrackAlias].
// Inbound data streams (SUBGROUP_HEADER, ObjectDatagram, FETCH_HEADER objects)
// identify their track by the alias the publisher chose; consumers — most
// notably the relay's fanout and end-subscriber applications — use this
// method to recover the canonical track identity for routing or rendering.
func (s *Session) LookupInboundTrackAlias(alias uint64) (track.Key, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key, ok := s.inboundAliases[alias]
	return key, ok
}
