package message

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

func TestPublishRoundTrip(t *testing.T) {
	roundtrip(t, &Publish{
		RequestID:  3,
		Namespace:  wire.TrackNamespace{[]byte("example.com"), []byte("live")},
		Name:       []byte("video"),
		TrackAlias: 9,
		Parameters: Parameters{
			ForwardParam(false),
		},
	})
}

func TestPublishDoneRoundTrip(t *testing.T) {
	roundtrip(t, &PublishDone{
		StatusCode:  moqt.PublishDoneGoingAway,
		StreamCount: 12,
		ErrorReason: "relay draining",
	})
}
