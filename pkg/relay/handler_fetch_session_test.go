package relay_test

import (
	"bytes"
	"math"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFetch_RejectsUnknownTrack: FETCH against a track no publisher has
// touched returns RequestDoesNotExist.
func TestFetch_RejectsUnknownTrack(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 1, Object: math.MaxUint64}),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestTrackStatus_ReplyForKnownTrack: the Track Properties a PUBLISH carried
// are echoed byte for byte in TRACK_STATUS_OK to another session's
// TRACK_STATUS for the track.
func TestTrackStatus_ReplyForKnownTrack(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
		Name:            []byte("cam1"),
		TrackAlias:      1,
		TrackProperties: opaqueProps("rtp-h265"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	querySess := dialAnotherClient(t, pubSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	if got, want := tsStream.OK.TrackProperties, opaqueProps("rtp-h265"); !bytes.Equal(got, want) {
		t.Fatalf("TrackProperties = %x, want %x", got, want)
	}
}

// TestTrackStatus_ReplyEmptyPropertiesForKnownNamespace: a TRACK_STATUS for a
// track under an advertised namespace with no upstream yet gets
// TRACK_STATUS_OK with empty Properties.
func TestTrackStatus_ReplyEmptyPropertiesForKnownNamespace(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pnsStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pnsStream.Close()

	querySess := dialAnotherClient(t, pubSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"),
		Name:      []byte("cam-anything"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	if len(tsStream.OK.TrackProperties) != 0 {
		t.Fatalf("TrackProperties = %q, want empty", tsStream.OK.TrackProperties)
	}
}

// TestTrackStatus_RejectsUnknownTrack pins the no-publisher-no-namespace
// case: TRACK_STATUS for a name no one has claimed returns
// RequestDoesNotExist.
func TestTrackStatus_RejectsUnknownTrack(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"),
		Name:      []byte("phantom"),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}
