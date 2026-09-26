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
// returns this error directly; [Request.AcceptPublish] replies REQUEST_ERROR
// UNSUPPORTED_EXTENSION before returning it.
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
// returns when raw Track Properties do not parse (§2.5). Assumption: the draft
// does not cover this, and rejecting with MALFORMED_TRACK (§10.6 defines it
// only for FETCH) is this package's choice.
var ErrMalformedTrackProperties = errors.New("moqt/session: malformed track properties")

// ErrTrackPropertiesNotAllowed is returned, and nothing sent, when asked to
// send a REQUEST_OK with Track Properties where §10.5 says they are empty.
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
	// §12.7: Mandatory Track Properties inside Immutable Properties count.
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

// CheckTrackProperties validates raw Track Properties: a value the draft makes
// session-fatal closes the session (§12.5, §12.6), and against the types
// configured with [WithKnownMandatoryTrackProperties] (§2.5.1) it returns
// *ErrUnsupportedMandatoryTrackProperty or an error wrapping
// [ErrMalformedTrackProperties]; see [TrackPropertiesRejectCode]. It is for
// callers that bypass [Request.AcceptPublish] and the outbound openers, which
// already check. Without that option only the values are checked.
func (s *Session) CheckTrackProperties(raw []byte, context string) error {
	return s.validateTrackProperties(raw, context)
}

// validateTrackProperties checks Track Properties received in context. A value
// the draft makes session-fatal (see [message.CheckTrackPropertyValues])
// closes the session with PROTOCOL_VIOLATION. Then, against the session's
// configured set of known mandatory track property types, an unknown one is
// refused (§2.5.1); if WithKnownMandatoryTrackProperties was never called (the
// map is nil), that check is skipped, for endpoints that pass Track
// Properties through.
func (s *Session) validateTrackProperties(raw []byte, context string) error {
	if err := s.checkTrackPropertyValues(raw, context); err != nil {
		return err
	}
	if s.knownMandatoryTrackProperties == nil {
		return nil // not configured — skip enforcement
	}
	_, err := ValidateTrackProperties(raw, s.knownMandatoryTrackProperties, context)
	return err
}

// checkTrackPropertyValues closes the session with PROTOCOL_VIOLATION when
// Track Properties received in context hold a value the draft makes
// session-fatal (see [message.CheckTrackPropertyValues]): "If an endpoint
// receives a value outside this range, it MUST close the session" (§12.5,
// §12.6). Immutable Properties are searched when they parse (§12.7); Track
// Properties that do not parse are left to the caller.
func (s *Session) checkTrackPropertyValues(raw []byte, context string) error {
	pairs, parseErr := message.ParseTrackProperties(raw)
	if parseErr == nil {
		if all, err := message.ExpandImmutable(pairs); err == nil {
			pairs = all
		}
	}
	if err := message.CheckTrackPropertyValues(pairs); err != nil {
		return s.closeProtocolViolation(fmt.Errorf("%s: %w", context, err))
	}
	return nil
}

// TrackPropertiesRejectCode is the REQUEST_ERROR code for a Track Properties
// validation error: UNSUPPORTED_EXTENSION (§2.5.1) or MALFORMED_TRACK.
func TrackPropertiesRejectCode(err error) moqt.RequestErrorCode {
	if _, ok := errors.AsType[*ErrUnsupportedMandatoryTrackProperty](err); ok {
		return moqt.RequestUnsupportedExtension
	}
	return moqt.RequestMalformedTrack
}
