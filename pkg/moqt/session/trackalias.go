package session

import (
	"context"
	"fmt"
	"time"

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

	// MaxCacheDuration is the MAX_CACHE_DURATION (§12.3) in the same Track
	// Properties, and HasMaxCacheDuration whether there was one. §12.3 limits
	// "any individual Object received through this subscription or fetch",
	// so it belongs with the alias Objects arrive on, like
	// DefaultPublisherPriority.
	MaxCacheDuration    time.Duration
	HasMaxCacheDuration bool
}

// RegisterInboundTrack records that the peer has assigned alias to the track
// identified by key, along with the Track Properties of the message that did
// so. This MUST be called by the subscriber when it receives a SUBSCRIBE_OK
// (whose TrackAlias field is the alias) and by the server when it receives a
// PUBLISH (whose TrackAlias field is the alias).
//
// If alias is already registered for the same track, the registration is
// counted and nil returned. §5.1: "An endpoint MAY have multiple concurrent
// subscriptions to the same Track [...]. A publisher MAY assign the same or
// different Track Aliases to these subscriptions." The alias stays registered
// until each registration is released by [Session.UnregisterInboundTrackAlias].
// The latest registration's Track Properties replace the earlier ones (§2.5:
// "the most recent set SHOULD replace any cached values"); the draft does not
// say which a shared alias should carry, and the session cannot tell which
// registration a release ends. If alias is already registered for a different
// track, *ErrDuplicateTrackAlias is returned and the caller MUST close the
// session with SessionDuplicateTrackAlias (§11.1).
func (s *Session) RegisterInboundTrack(alias uint64, key track.Key, trackProperties []byte) error {
	in := InboundTrack{
		Key:                      key,
		DefaultPublisherPriority: message.TrackDefaultPublisherPriority(trackProperties),
	}
	in.MaxCacheDuration, in.HasMaxCacheDuration = message.TrackMaxCacheDuration(trackProperties)
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.inboundAliases[alias]; ok {
		if existing.Key != key {
			return &ErrDuplicateTrackAlias{Alias: alias, Existing: existing.Key, New: key}
		}
		s.inboundAliases[alias] = in
		s.inboundAliasRefs[alias]++
		return nil
	}
	s.inboundAliases[alias] = in
	s.inboundAliasRefs[alias] = 1
	close(s.aliasRegistered)
	s.aliasRegistered = make(chan struct{})
	return nil
}

// RegisterInboundTrackAlias is [Session.RegisterInboundTrack] for a message
// that carried no Track Properties.
func (s *Session) RegisterInboundTrackAlias(alias uint64, key track.Key) error {
	return s.RegisterInboundTrack(alias, key, nil)
}

// UnregisterInboundTrackAlias releases one registration of alias (see
// [Session.RegisterInboundTrack]); the alias is removed, and free for reuse,
// with the last. Callers should invoke this when the subscription or
// publication associated with alias has been fully torn down (e.g. after
// PUBLISH_DONE or subscription cancellation and a suitable grace period per
// §11.1: "Subscribers SHOULD retain sufficient state to quickly discard
// these unwanted Objects").
//
// Unregistering an alias that was never registered is a no-op.
func (s *Session) UnregisterInboundTrackAlias(alias uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inboundAliasRefs[alias] > 1 {
		s.inboundAliasRefs[alias]--
		return
	}
	delete(s.inboundAliases, alias)
	delete(s.inboundAliasRefs, alias)
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

// awaitInboundTrack is [Session.LookupInboundTrack] that waits for alias to be
// registered, until ctx ends or the session closes. See
// [IncomingSubgroupStream.AwaitInboundTrack].
func (s *Session) awaitInboundTrack(ctx context.Context, alias uint64) (InboundTrack, bool) {
	for {
		s.mu.Lock()
		in, ok := s.inboundAliases[alias]
		registered := s.aliasRegistered
		s.mu.Unlock()
		if ok {
			return in, true
		}
		select {
		case <-registered:
		case <-ctx.Done():
			return InboundTrack{}, false
		case <-s.done:
			return InboundTrack{}, false
		}
	}
}

// LookupInboundTrackAlias is [Session.LookupInboundTrack] reduced to the
// track.Key.
func (s *Session) LookupInboundTrackAlias(alias uint64) (track.Key, bool) {
	in, ok := s.LookupInboundTrack(alias)
	return in.Key, ok
}
