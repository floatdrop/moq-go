// Package moqt is the umbrella for MoQT protocol primitives, error codes, and
// session machinery used by the relay.
//
// Subpackages:
//   - wire:    on-the-wire primitives and control-message framing.
//   - message: typed message structs with Marshal/Parse.
package moqt

// SessionErrorCode is a MoQT session-termination error code (§3.5, IANA §15.11.1).
type SessionErrorCode uint64

const (
	SessionNoError                  SessionErrorCode = 0x0
	SessionInternalError            SessionErrorCode = 0x1
	SessionUnauthorized             SessionErrorCode = 0x2
	SessionProtocolViolation        SessionErrorCode = 0x3
	SessionInvalidRequestID         SessionErrorCode = 0x4
	SessionDuplicateTrackAlias      SessionErrorCode = 0x5
	SessionKeyValueFormattingError  SessionErrorCode = 0x6
	SessionInvalidPath              SessionErrorCode = 0x8
	SessionMalformedPath            SessionErrorCode = 0x9
	SessionGoawayTimeout            SessionErrorCode = 0x10
	SessionControlMessageTimeout    SessionErrorCode = 0x11
	SessionDataStreamTimeout        SessionErrorCode = 0x12
	SessionAuthTokenCacheOverflow   SessionErrorCode = 0x13
	SessionDuplicateAuthTokenAlias  SessionErrorCode = 0x14
	SessionVersionNegotiationFailed SessionErrorCode = 0x15
	SessionMalformedAuthToken       SessionErrorCode = 0x16
	SessionUnknownAuthTokenAlias    SessionErrorCode = 0x17
	SessionExpiredAuthToken         SessionErrorCode = 0x18
	SessionInvalidAuthority         SessionErrorCode = 0x19
	SessionMalformedAuthority       SessionErrorCode = 0x1A
	SessionTooManyRequestUpdates    SessionErrorCode = 0x1B
)

// RequestErrorCode is a REQUEST_ERROR code (§10.6, IANA §15.11.2).
type RequestErrorCode uint64

const (
	RequestInternalError        RequestErrorCode = 0x0
	RequestUnauthorized         RequestErrorCode = 0x1
	RequestTimeout              RequestErrorCode = 0x2
	RequestNotSupported         RequestErrorCode = 0x3
	RequestMalformedAuthToken   RequestErrorCode = 0x4
	RequestExpiredAuthToken     RequestErrorCode = 0x5
	RequestGoingAway            RequestErrorCode = 0x6
	RequestExcessiveLoad        RequestErrorCode = 0x9
	RequestDoesNotExist         RequestErrorCode = 0x10
	RequestInvalidRange         RequestErrorCode = 0x11
	RequestMalformedTrack       RequestErrorCode = 0x12
	RequestUninterested         RequestErrorCode = 0x20
	RequestPrefixOverlap        RequestErrorCode = 0x30
	RequestNamespaceTooLarge    RequestErrorCode = 0x31
	RequestInvalidJoiningID     RequestErrorCode = 0x32
	RequestUnsupportedExtension RequestErrorCode = 0x33
	RequestRedirect             RequestErrorCode = 0x34
	RequestInvalidFilter        RequestErrorCode = 0x36
)

// PublishDoneCode is a PUBLISH_DONE status code (§10.11, IANA §15.11.3).
type PublishDoneCode uint64

const (
	PublishDoneInternalError     PublishDoneCode = 0x0
	PublishDoneUnauthorized      PublishDoneCode = 0x1
	PublishDoneTrackEnded        PublishDoneCode = 0x2
	PublishDoneSubscriptionEnded PublishDoneCode = 0x3
	PublishDoneGoingAway         PublishDoneCode = 0x4
	PublishDoneTooFarBehind      PublishDoneCode = 0x5
	PublishDoneExpired           PublishDoneCode = 0x6
	PublishDoneUpdateFailed      PublishDoneCode = 0x8
	PublishDoneExcessiveLoad     PublishDoneCode = 0x9
	PublishDoneMalformedTrack    PublishDoneCode = 0x12
)

// StreamResetCode is a per-stream reset code (§3.3.4, IANA §15.11.4).
type StreamResetCode uint64

const (
	StreamResetInternalError       StreamResetCode = 0x0
	StreamResetCancelled           StreamResetCode = 0x1
	StreamResetDeliveryTimeout     StreamResetCode = 0x2
	StreamResetSessionClosed       StreamResetCode = 0x3
	StreamResetGoingAway           StreamResetCode = 0x4
	StreamResetTooFarBehind        StreamResetCode = 0x5
	StreamResetUnknownObjectStatus StreamResetCode = 0x6
	StreamResetExpiredAuthToken    StreamResetCode = 0x7
	StreamResetExcessiveLoad       StreamResetCode = 0x9
	StreamResetMalformedTrack      StreamResetCode = 0x12
)
