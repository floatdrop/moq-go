package moqt_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
)

// ---------------------------------------------------------------------------
// SessionErrorCode.IsKnown
// ---------------------------------------------------------------------------

func TestSessionErrorCodeIsKnown(t *testing.T) {
	known := []moqt.SessionErrorCode{
		moqt.SessionNoError,
		moqt.SessionInternalError,
		moqt.SessionUnauthorized,
		moqt.SessionProtocolViolation,
		moqt.SessionInvalidRequestID,
		moqt.SessionDuplicateTrackAlias,
		moqt.SessionKeyValueFormattingError,
		moqt.SessionInvalidPath,
		moqt.SessionMalformedPath,
		moqt.SessionGoawayTimeout,
		moqt.SessionControlMessageTimeout,
		moqt.SessionDataStreamTimeout,
		moqt.SessionAuthTokenCacheOverflow,
		moqt.SessionDuplicateAuthTokenAlias,
		moqt.SessionVersionNegotiationFailed,
		moqt.SessionMalformedAuthToken,
		moqt.SessionUnknownAuthTokenAlias,
		moqt.SessionExpiredAuthToken,
		moqt.SessionInvalidAuthority,
		moqt.SessionMalformedAuthority,
	}
	for _, c := range known {
		if !c.IsKnown() {
			t.Errorf("SessionErrorCode(%#x).IsKnown() = false, want true", uint64(c))
		}
	}
}

func TestSessionErrorCodeIsKnownUnknown(t *testing.T) {
	unknown := []moqt.SessionErrorCode{0x7, 0x1B, 0xFF, 0x1000}
	for _, c := range unknown {
		if c.IsKnown() {
			t.Errorf("SessionErrorCode(%#x).IsKnown() = true, want false", uint64(c))
		}
	}
}

// ---------------------------------------------------------------------------
// RequestErrorCode.IsKnown
// ---------------------------------------------------------------------------

func TestRequestErrorCodeIsKnown(t *testing.T) {
	known := []moqt.RequestErrorCode{
		moqt.RequestInternalError,
		moqt.RequestUnauthorized,
		moqt.RequestTimeout,
		moqt.RequestNotSupported,
		moqt.RequestMalformedAuthToken,
		moqt.RequestExpiredAuthToken,
		moqt.RequestGoingAway,
		moqt.RequestExcessiveLoad,
		moqt.RequestDoesNotExist,
		moqt.RequestInvalidRange,
		moqt.RequestMalformedTrack,
		moqt.RequestDuplicateSubscription,
		moqt.RequestUninterested,
		moqt.RequestPrefixOverlap,
		moqt.RequestNamespaceTooLarge,
		moqt.RequestInvalidJoiningID,
		moqt.RequestUnsupportedExtension,
		moqt.RequestRedirect,
	}
	for _, c := range known {
		if !c.IsKnown() {
			t.Errorf("RequestErrorCode(%#x).IsKnown() = false, want true", uint64(c))
		}
	}
}

func TestRequestErrorCodeIsKnownUnknown(t *testing.T) {
	unknown := []moqt.RequestErrorCode{0x7, 0x8, 0x13, 0x35, 0xFF}
	for _, c := range unknown {
		if c.IsKnown() {
			t.Errorf("RequestErrorCode(%#x).IsKnown() = true, want false", uint64(c))
		}
	}
}

// ---------------------------------------------------------------------------
// PublishDoneCode.IsKnown
// ---------------------------------------------------------------------------

func TestPublishDoneCodeIsKnown(t *testing.T) {
	known := []moqt.PublishDoneCode{
		moqt.PublishDoneInternalError,
		moqt.PublishDoneUnauthorized,
		moqt.PublishDoneTrackEnded,
		moqt.PublishDoneSubscriptionEnded,
		moqt.PublishDoneGoingAway,
		moqt.PublishDoneTooFarBehind,
		moqt.PublishDoneExpired,
		moqt.PublishDoneUpdateFailed,
		moqt.PublishDoneExcessiveLoad,
		moqt.PublishDoneMalformedTrack,
	}
	for _, c := range known {
		if !c.IsKnown() {
			t.Errorf("PublishDoneCode(%#x).IsKnown() = false, want true", uint64(c))
		}
	}
}

func TestPublishDoneCodeIsKnownUnknown(t *testing.T) {
	unknown := []moqt.PublishDoneCode{0x7, 0xA, 0x11, 0xFF}
	for _, c := range unknown {
		if c.IsKnown() {
			t.Errorf("PublishDoneCode(%#x).IsKnown() = true, want false", uint64(c))
		}
	}
}

// ---------------------------------------------------------------------------
// StreamResetCode.IsKnown
// ---------------------------------------------------------------------------

func TestStreamResetCodeIsKnown(t *testing.T) {
	known := []moqt.StreamResetCode{
		moqt.StreamResetInternalError,
		moqt.StreamResetCancelled,
		moqt.StreamResetDeliveryTimeout,
		moqt.StreamResetSessionClosed,
		moqt.StreamResetGoingAway,
		moqt.StreamResetTooFarBehind,
		moqt.StreamResetUnknownObjectStatus,
		moqt.StreamResetExpiredAuthToken,
		moqt.StreamResetExcessiveLoad,
		moqt.StreamResetMalformedTrack,
	}
	for _, c := range known {
		if !c.IsKnown() {
			t.Errorf("StreamResetCode(%#x).IsKnown() = false, want true", uint64(c))
		}
	}
}

func TestStreamResetCodeIsKnownUnknown(t *testing.T) {
	unknown := []moqt.StreamResetCode{0x8, 0xA, 0x11, 0xFF}
	for _, c := range unknown {
		if c.IsKnown() {
			t.Errorf("StreamResetCode(%#x).IsKnown() = true, want false", uint64(c))
		}
	}
}
