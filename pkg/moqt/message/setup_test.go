package message

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestSetupRoundTrip(t *testing.T) {
	roundtrip(t, &Setup{
		Options: []wire.KVPair{
			PathOption("/relay"),
			MaxAuthTokenCacheSizeOption(16 * 1024),
			MOQTImplementationOption("mediamesh/dev"),
			MaxRequestUpdatesOption(4),
		},
	})
}
