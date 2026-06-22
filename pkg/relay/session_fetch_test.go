package relay_test

import (
	"errors"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestFetch_RejectsUnknownTrack: FETCH against a track no publisher has
// touched returns RequestDoesNotExist.
func TestFetch_RejectsUnknownTrack(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	_, err := clientSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     wire.TrackNamespace{[]byte("video")},
			Name:          []byte("cam1"),
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 1, Object: 0},
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestFetch_AuthDenialUsesPolicyCode verifies the authorizer wires through
// for FETCH and runs before the NotSupported stub.
func TestFetch_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: errors.New("no fetch for you")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.Fetch(t.Context(), &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace: wire.TrackNamespace{[]byte("video")},
			Name:      []byte("cam1"),
		},
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.fetchCalls.Load(); got != 1 {
		t.Errorf("fetchCalls = %d, want 1", got)
	}
}

// TestTrackStatus_ReplyForKnownTrack: a publisher claims a track via PUBLISH,
// which populates the TrackRegistry entry's Properties. A separate session's
// TRACK_STATUS for the same name must echo those Properties in TRACK_STATUS_OK.
func TestTrackStatus_ReplyForKnownTrack(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackAlias:      1,
		TrackProperties: []byte("rtp-h265"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	querySess := dialAnotherClient(t, pubSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	if got := string(tsStream.OK.TrackProperties); got != "rtp-h265" {
		t.Fatalf("TrackProperties = %q, want %q", got, "rtp-h265")
	}
}

// TestTrackStatus_ReplyEmptyPropertiesForKnownNamespace verifies the
// fallback when a publisher has advertised the namespace via
// PUBLISH_NAMESPACE but no upstream subscription is yet active. The relay
// answers TRACK_STATUS_OK with empty Properties — telling the caller "this
// track may exist, but I have no metadata.".
func TestTrackStatus_ReplyEmptyPropertiesForKnownNamespace(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pnsStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pnsStream.Close()

	querySess := dialAnotherClient(t, pubSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")},
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
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("phantom"),
	})
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// TestTrackStatus_AuthDenialUsesPolicyCode pins auth precedence on the
// TRACK_STATUS arm.
func TestTrackStatus_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: errors.New("no status")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.trackStatusCalls.Load(); got != 1 {
		t.Errorf("trackStatusCalls = %d, want 1", got)
	}
}
