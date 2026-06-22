package message

import (
	"bytes"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestRequestUpdateRoundTrip(t *testing.T) {
	roundtrip(t, &RequestUpdate{
		RequestID: 4,
		Parameters: Parameters{
			SubgroupDeliveryTimeoutParam(1500 * time.Millisecond),
			ForwardParam(true),
		},
	})
}

func TestRequestOKRoundTrip(t *testing.T) {
	roundtrip(t, &RequestOK{
		Parameters: Parameters{ExpiresParam(30 * time.Second)},
	})
}

func TestRequestErrorPlainRoundTrip(t *testing.T) {
	roundtrip(t, &RequestError{
		ErrorCode:     moqt.RequestUnauthorized,
		RetryInterval: 0,
		ErrorReason:   "token expired",
	})
}

func TestRequestErrorRedirectRoundTrip(t *testing.T) {
	roundtrip(t, &RequestError{
		ErrorCode:     moqt.RequestRedirect,
		RetryInterval: 1,
		ErrorReason:   "moved",
		Redirect: &Redirect{
			ConnectURI: []byte("moqt://relay-3.example"),
			Namespace:  wire.TrackNamespace{[]byte("example.com"), []byte("vod")},
			TrackName:  []byte("hello"),
		},
	})
}

func TestValidateRedirectOKNoRedirect(t *testing.T) {
	m := &RequestError{ErrorCode: moqt.RequestUnauthorized}
	if err := m.ValidateRedirect(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRedirectOKWithRedirect(t *testing.T) {
	m := &RequestError{
		ErrorCode: moqt.RequestRedirect,
		Redirect: &Redirect{
			ConnectURI: []byte("moqt://relay.example"),
			Namespace:  wire.TrackNamespace{[]byte("ns")},
			TrackName:  []byte("track"),
		},
	}
	if err := m.ValidateRedirect(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateRedirectErrorCodeRedirectNilBlock(t *testing.T) {
	// ErrorCode == REDIRECT but Redirect block is absent — must fail.
	m := &RequestError{ErrorCode: moqt.RequestRedirect}
	if err := m.ValidateRedirect(); err == nil {
		t.Fatal("expected error for REDIRECT code with nil Redirect, got nil")
	}
}

func TestValidateRedirectErrorNonRedirectCodeWithBlock(t *testing.T) {
	// Non-REDIRECT error code but Redirect block present — must fail.
	m := &RequestError{
		ErrorCode: moqt.RequestUnauthorized,
		Redirect: &Redirect{
			ConnectURI: []byte("moqt://relay.example"),
		},
	}
	if err := m.ValidateRedirect(); err == nil {
		t.Fatal("expected error for non-REDIRECT code with Redirect block, got nil")
	}
}

// The §10.6.2 redirect invariants must be enforced by Parse (via the
// ParsePayload Validate hook), not only by an explicit ValidateRedirect call.
func TestRequestErrorParseValidatesRedirect(t *testing.T) {
	// REDIRECT code with no Redirect block.
	var buf bytes.Buffer
	if err := Marshal(&buf, &RequestError{ErrorCode: moqt.RequestRedirect}); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("Parse must reject a REDIRECT REQUEST_ERROR with no Redirect block")
	}

	// Non-REDIRECT code carrying a Redirect block.
	buf.Reset()
	if err := Marshal(&buf, &RequestError{
		ErrorCode: moqt.RequestUnauthorized,
		Redirect: &Redirect{
			ConnectURI: []byte("moqt://relay.example"),
			Namespace:  wire.TrackNamespace{[]byte("ns")},
			TrackName:  []byte("track"),
		},
	}); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := Parse(&buf); err == nil {
		t.Fatal("Parse must reject a non-REDIRECT REQUEST_ERROR carrying a Redirect block")
	}
}
