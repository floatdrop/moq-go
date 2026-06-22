// Package moqt is the umbrella for MoQT protocol primitives, error codes, and
// session machinery used by the relay.
//
// Subpackages:
//   - wire:    on-the-wire primitives and control-message framing.
//   - message: typed message structs with Marshal/Parse.
package moqt

// SessionErrorCode is a MoQT session-termination error code (§3.5, IANA §15.10.1).
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
)

// IsKnown reports whether c is a defined SESSION error code (IANA §15.10.1).
func (c SessionErrorCode) IsKnown() bool {
	switch c {
	case SessionNoError,
		SessionInternalError,
		SessionUnauthorized,
		SessionProtocolViolation,
		SessionInvalidRequestID,
		SessionDuplicateTrackAlias,
		SessionKeyValueFormattingError,
		SessionInvalidPath,
		SessionMalformedPath,
		SessionGoawayTimeout,
		SessionControlMessageTimeout,
		SessionDataStreamTimeout,
		SessionAuthTokenCacheOverflow,
		SessionDuplicateAuthTokenAlias,
		SessionVersionNegotiationFailed,
		SessionMalformedAuthToken,
		SessionUnknownAuthTokenAlias,
		SessionExpiredAuthToken,
		SessionInvalidAuthority,
		SessionMalformedAuthority:
		return true
	}
	return false
}

// RequestErrorCode is a REQUEST_ERROR code (§10.6, IANA §15.10.2).
type RequestErrorCode uint64

const (
	RequestInternalError         RequestErrorCode = 0x0
	RequestUnauthorized          RequestErrorCode = 0x1
	RequestTimeout               RequestErrorCode = 0x2
	RequestNotSupported          RequestErrorCode = 0x3
	RequestMalformedAuthToken    RequestErrorCode = 0x4
	RequestExpiredAuthToken      RequestErrorCode = 0x5
	RequestGoingAway             RequestErrorCode = 0x6
	RequestExcessiveLoad         RequestErrorCode = 0x9
	RequestDoesNotExist          RequestErrorCode = 0x10
	RequestInvalidRange          RequestErrorCode = 0x11
	RequestMalformedTrack        RequestErrorCode = 0x12
	RequestDuplicateSubscription RequestErrorCode = 0x19
	RequestUninterested          RequestErrorCode = 0x20
	RequestPrefixOverlap         RequestErrorCode = 0x30
	RequestNamespaceTooLarge     RequestErrorCode = 0x31
	RequestInvalidJoiningID      RequestErrorCode = 0x32
	RequestUnsupportedExtension  RequestErrorCode = 0x33
	RequestRedirect              RequestErrorCode = 0x34
)

// IsKnown reports whether c is a defined REQUEST_ERROR code (IANA §15.10.2).
func (c RequestErrorCode) IsKnown() bool {
	switch c {
	case RequestInternalError,
		RequestUnauthorized,
		RequestTimeout,
		RequestNotSupported,
		RequestMalformedAuthToken,
		RequestExpiredAuthToken,
		RequestGoingAway,
		RequestExcessiveLoad,
		RequestDoesNotExist,
		RequestInvalidRange,
		RequestMalformedTrack,
		RequestDuplicateSubscription,
		RequestUninterested,
		RequestPrefixOverlap,
		RequestNamespaceTooLarge,
		RequestInvalidJoiningID,
		RequestUnsupportedExtension,
		RequestRedirect:
		return true
	}
	return false
}

// PublishDoneCode is a PUBLISH_DONE status code (§10.11, IANA §15.10.3).
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

// IsKnown reports whether c is a defined PUBLISH_DONE code (IANA §15.10.3).
func (c PublishDoneCode) IsKnown() bool {
	switch c {
	case PublishDoneInternalError,
		PublishDoneUnauthorized,
		PublishDoneTrackEnded,
		PublishDoneSubscriptionEnded,
		PublishDoneGoingAway,
		PublishDoneTooFarBehind,
		PublishDoneExpired,
		PublishDoneUpdateFailed,
		PublishDoneExcessiveLoad,
		PublishDoneMalformedTrack:
		return true
	}
	return false
}

// StreamResetCode is a per-stream reset code (§3.3.3, IANA §15.10.4).
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

// IsKnown reports whether c is a defined stream-reset code (IANA §15.10.4).
func (c StreamResetCode) IsKnown() bool {
	switch c {
	case StreamResetInternalError,
		StreamResetCancelled,
		StreamResetDeliveryTimeout,
		StreamResetSessionClosed,
		StreamResetGoingAway,
		StreamResetTooFarBehind,
		StreamResetUnknownObjectStatus,
		StreamResetExpiredAuthToken,
		StreamResetExcessiveLoad,
		StreamResetMalformedTrack:
		return true
	}
	return false
}
