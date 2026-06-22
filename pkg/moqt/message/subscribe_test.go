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
