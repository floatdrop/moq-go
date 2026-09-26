package session

import (
	"context"
	"fmt"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// AllocOutboundTrackAlias returns the next Track Alias to use when this side
// advertises a new track to the peer (§11.1). Aliases are independent across
// sessions, so callers must remap when forwarding between two sessions.
//
// Allocation starts at 1: [Session.Publish] and [Request.AcceptSubscribe]
// treat a zero TrackAlias as "allocate one for me", so an allocated 0 would be
// silently replaced. [Session.OpenPublish] does not: its caller allocates.
func (s *Session) AllocOutboundTrackAlias() uint64 {
	return s.nextOutboundTrackAlias.Add(1)
}

// ErrDuplicateTrackAlias is returned by [Session.RegisterInboundTrack] when the
// peer assigns a Track Alias that is already in use for a different track
// (§11.1). The session is already closed with
// [moqt.SessionDuplicateTrackAlias].
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

	// DefaultPublisherPriority is the DEFAULT_PUBLISHER_PRIORITY (§12.4) of
	// the message that bound the alias, or 128 when omitted. Subgroups and
	// datagrams with the DEFAULT_PRIORITY bit inherit it (§11.4.2, §11.3.1).
	DefaultPublisherPriority uint8

	// MaxCacheDuration is the MAX_CACHE_DURATION (§12.3) of the same message,
	// if HasMaxCacheDuration.
	MaxCacheDuration    time.Duration
	HasMaxCacheDuration bool
}

// RegisterInboundTrack records that the peer has assigned alias to the track
// identified by key, along with the Track Properties of the message that did
// so. This MUST be called by the subscriber when it receives a SUBSCRIBE_OK
// (whose TrackAlias field is the alias) and by the server when it receives a
// PUBLISH (whose TrackAlias field is the alias).
//
// Registering an alias again for the same track counts one more registration
// (§5.1 allows subscriptions to share an alias); it stays registered until
// each is released by [Session.UnregisterInboundTrackAlias]. Assumption: the
// latest Track Properties replace earlier ones — the draft does not say which
// a shared alias carries, and a release cannot tell which registration it
// ends, so the survivor may keep a released one's properties.
//
// If alias is registered for a different track, the session is closed with
// DUPLICATE_TRACK_ALIAS and *ErrDuplicateTrackAlias returned: "it MUST close
// the session with error DUPLICATE_TRACK_ALIAS" (§11.1).
func (s *Session) RegisterInboundTrack(alias uint64, key track.Key, trackProperties []byte) error {
	in := InboundTrack{
		Key:                      key,
		DefaultPublisherPriority: message.TrackDefaultPublisherPriority(trackProperties),
	}
	in.MaxCacheDuration, in.HasMaxCacheDuration = message.TrackMaxCacheDuration(trackProperties)
	if err := s.registerInboundTrack(alias, in); err != nil {
		_ = s.Close(moqt.SessionDuplicateTrackAlias, err.Error())
		return err
	}
	return nil
}

func (s *Session) registerInboundTrack(alias uint64, in InboundTrack) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.inboundAliases[alias]; ok {
		if existing.Key != in.Key {
			return &ErrDuplicateTrackAlias{Alias: alias, Existing: existing.Key, New: in.Key}
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
// [Session.RegisterInboundTrack]); the last release removes it. Call it once
// the subscription or publication is torn down, after a grace period (§11.1).
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
func (s *Session) LookupInboundTrack(alias uint64) (InboundTrack, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inboundAliases[alias]
	return in, ok
}

// awaitInboundTrack is [Session.LookupInboundTrack] that waits for alias to be
// registered, until ctx ends or the session closes.
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
