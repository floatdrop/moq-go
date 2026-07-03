package message

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestSubscribeRoundTrip(t *testing.T) {
	roundtrip(t, &Subscribe{
		RequestID: 4,
		Namespace: wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Name:      []byte("catalog"),
		Parameters: Parameters{
			AuthorizationTokenParam(Token{AliasType: AliasTypeUseValue, TokenType: 1, TokenValue: []byte{0x02, 0x03}}),
			RendezvousTimeoutParam(2 * time.Second),
			ForwardParam(true),
			GroupOrderParam(GroupOrderAscending),
		},
	})
}

func TestSubscribeOKRoundTrip(t *testing.T) {
	roundtrip(t, &SubscribeOK{
		TrackAlias: 7,
		Parameters: Parameters{
			ExpiresParam(60 * time.Second),
			LargestObjectParam(12, 34),
		},
	})
}

// TestSubscribeFullTrackNameLimit pins the §2.4.1 4096-byte Full Track Name
// cap at the message layer: exactly 4096 bytes round-trips, 4097 is rejected
// at parse time (ParsePayload runs Validate).
func TestSubscribeFullTrackNameLimit(t *testing.T) {
	build := func(nsLen, nameLen int) []byte {
		m := &Subscribe{
			RequestID: 0,
			Namespace: wire.TrackNamespace{make([]byte, nsLen)},
			Name:      make([]byte, nameLen),
		}
		w := wire.NewWriter(nil)
		m.Append(w)
		return w.Bytes()
	}

	if _, err := ParsePayload(TypeSubscribe, build(4000, 96)); err != nil {
		t.Fatalf("4096-byte full track name rejected: %v", err)
	}
	if _, err := ParsePayload(TypeSubscribe, build(4000, 97)); err == nil {
		t.Fatal("4097-byte full track name accepted, want §2.4.1 rejection")
	}
	// Namespace-only overflow is caught by wire.Reader.TrackNamespace.
	if _, err := ParsePayload(TypeSubscribe, build(4097, 0)); err == nil {
		t.Fatal("4097-byte namespace accepted, want §2.4.1 rejection")
	}
}
