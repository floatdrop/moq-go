package session

import (
	"fmt"

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
// returns this error directly. For inbound PUBLISH the caller should use
// ValidateTrackProperties and, on error, reply with REQUEST_ERROR /
// UNSUPPORTED_EXTENSION via Request.RejectError.
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
		return nil, fmt.Errorf("moqt/session: parsing track properties in %s: %w", context, err)
	}
	if typ, unknown := message.FirstUnknownMandatoryTrackProperty(pairs, knownMandatory); unknown {
		return nil, &ErrUnsupportedMandatoryTrackProperty{
			PropertyType: typ,
			Context:      context,
		}
	}
	return pairs, nil
}

// validateTrackProperties is a session-level convenience that uses the
// session's configured set of known mandatory track property types.
//
// If WithKnownMandatoryTrackProperties was never called (the map is nil),
// the check is skipped entirely — this is the default for relays and other
// forwarding endpoints that pass Track Properties through opaquely. End
// subscribers that need to interpret track data should call
// WithKnownMandatoryTrackProperties (even with an empty map) to opt in to
// enforcement.
func (s *Session) validateTrackProperties(raw []byte, context string) error {
	if s.knownMandatoryTrackProperties == nil {
		return nil // not configured — skip enforcement
	}
	_, err := ValidateTrackProperties(raw, s.knownMandatoryTrackProperties, context)
	return err
}
