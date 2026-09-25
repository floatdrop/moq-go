package session

import (
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ErrUnsupportedMandatoryTrackProperty is returned when Track Properties
// (received in SUBSCRIBE_OK, FETCH_OK, TRACK_STATUS_OK, or an inbound
// PUBLISH) contain a Mandatory Track Property (range 0x4000–0x7FFF per
// §2.5.1) that this endpoint does not understand. The caller MUST NOT
// process or forward the track.
//
// For outbound requests (Subscribe, Fetch, TrackStatus) the session layer
// returns this error directly, and [Request.AcceptPublish] replies
// REQUEST_ERROR UNSUPPORTED_EXTENSION before returning it. A caller that
// handles an inbound PUBLISH itself checks with [Session.CheckTrackProperties].
type ErrUnsupportedMandatoryTrackProperty struct {
	// PropertyType is the first unrecognised mandatory property type found.
	PropertyType message.PropertyType
	// Context describes where the property was encountered (e.g.
	// "SUBSCRIBE_OK", "FETCH_OK", "PUBLISH").
	Context string
}

func (e *ErrUnsupportedMandatoryTrackProperty) Error() string {
	return fmt.Sprintf(
		"moqt/session: unsupported mandatory track property 0x%X in %s (§2.5.1 — UNSUPPORTED_EXTENSION)",
		e.PropertyType, e.Context,
	)
}

// ErrMalformedTrackProperties is wrapped by the error [ValidateTrackProperties]
// returns when raw Track Properties do not parse as a sequence of Properties
// (§2.5). A receiver that cannot parse them cannot rule out an unknown
// Mandatory Track Property either, so it treats the track as malformed:
// [Request.AcceptPublish] refuses such a PUBLISH with MALFORMED_TRACK. The
// draft does not cover unparseable Track Properties, and §10.6 defines
// MALFORMED_TRACK only for FETCH, so that code is this package's choice.
var ErrMalformedTrackProperties = errors.New("moqt/session: malformed track properties")

// ErrTrackPropertiesNotAllowed is returned when an endpoint asks to send a
// REQUEST_OK with Track Properties where §10.5 says they are empty: in
// PUBLISH_OK, REQUEST_UPDATE_OK, SUBSCRIBE_NAMESPACE_OK and
// PUBLISH_NAMESPACE_OK. Nothing is sent, since the peer "MUST close the
// session with a PROTOCOL_VIOLATION" on receiving one.
var ErrTrackPropertiesNotAllowed = errors.New("moqt/session: track properties not allowed in this REQUEST_OK")

// ValidateTrackProperties parses raw Track Properties bytes and checks for
// unknown Mandatory Track Properties (range 0x4000–0x7FFF per §2.5.1).
//
// knownMandatory is the set of Mandatory Track Property types this endpoint
// supports. Every mandatory property found in raw that is not in this set
// causes *ErrUnsupportedMandatoryTrackProperty to be returned. An empty
// (non-nil) map means "I support no mandatory extensions" — any mandatory
// property will be rejected.
//
// Returns the parsed pairs on success. context is used in the error message
// to identify the source message (e.g. "SUBSCRIBE_OK").
func ValidateTrackProperties(
	raw []byte,
	knownMandatory map[message.PropertyType]struct{},
	context string,
) ([]wire.KVPair, error) {
	pairs, err := message.ParseTrackProperties(raw)
	if err != nil {
		return nil, fmt.Errorf("%w in %s: %w", ErrMalformedTrackProperties, context, err)
	}
	// §12.7: a Mandatory Track Property inside Immutable Properties counts
	// too, and contents that do not parse make the track malformed.
	all, err := message.ExpandImmutable(pairs)
	if err != nil {
		return nil, fmt.Errorf("%w in %s: %w", ErrMalformedTrackProperties, context, err)
	}
	if typ, unknown := message.FirstUnknownMandatoryTrackProperty(all, knownMandatory); unknown {
		return nil, &ErrUnsupportedMandatoryTrackProperty{
			PropertyType: typ,
			Context:      context,
		}
	}
	return pairs, nil
}

// CheckTrackProperties reports whether raw Track Properties (from a PUBLISH,
// SUBSCRIBE_OK, FETCH_OK, or TRACK_STATUS_OK) carry a Mandatory Track Property
// this session was not configured to understand via
// [WithKnownMandatoryTrackProperties], returning
// *ErrUnsupportedMandatoryTrackProperty if so, or an error wrapping
// [ErrMalformedTrackProperties] if they do not parse; [TrackPropertiesRejectCode]
// maps either to its REQUEST_ERROR code. §2.5.1: such a track MUST NOT be
// processed or forwarded. It is for callers that handle a request themselves
// rather than through [Request.AcceptPublish] or the outbound openers, which
// already check.
//
// If WithKnownMandatoryTrackProperties was never called (the map is nil), the
// check is skipped and nil is returned.
func (s *Session) CheckTrackProperties(raw []byte, context string) error {
	return s.validateTrackProperties(raw, context)
}

// validateTrackProperties is a session-level convenience that uses the
// session's configured set of known mandatory track property types.
//
// If WithKnownMandatoryTrackProperties was never called (the map is nil),
// the check is skipped entirely, for endpoints that pass Track Properties
// through without acting on them. Pass an empty (non-nil) map to opt in to
// enforcement with no types known.
func (s *Session) validateTrackProperties(raw []byte, context string) error {
	if s.knownMandatoryTrackProperties == nil {
		return nil // not configured — skip enforcement
	}
	_, err := ValidateTrackProperties(raw, s.knownMandatoryTrackProperties, context)
	return err
}

// TrackPropertiesRejectCode is the REQUEST_ERROR code for a Track Properties
// validation error: UNSUPPORTED_EXTENSION for an unknown Mandatory Track
// Property (§2.5.1), MALFORMED_TRACK for Track Properties that do not parse
// (see [ErrMalformedTrackProperties]).
func TrackPropertiesRejectCode(err error) moqt.RequestErrorCode {
	if _, ok := errors.AsType[*ErrUnsupportedMandatoryTrackProperty](err); ok {
		return moqt.RequestUnsupportedExtension
	}
	return moqt.RequestMalformedTrack
}
