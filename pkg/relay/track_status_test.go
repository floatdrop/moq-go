package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestTrackStatus_ReturnsLargestObject: TRACK_STATUS_OK carries the track's
// LARGEST_OBJECT (§10.2.17) once Objects were forwarded.
func TestTrackStatus_ReturnsLargestObject(t *testing.T) {
	t.Parallel()

	pubSess, _, publisherAlias := publishAndCache(t)
	publishObjects(t, pubSess, publisherAlias, 4 /*group*/, 3 /*count*/)

	time.Sleep(50 * time.Millisecond) // let the watermark reach {4, 2}

	querySess := dialAnotherClient(t, pubSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	p, found := tsStream.OK.Parameters.Find(message.ParamLargestObject)
	if !found {
		t.Fatal("TRACK_STATUS_OK missing LARGEST_OBJECT parameter")
	}
	if p.Group != 4 || p.Object != 2 {
		t.Fatalf("LARGEST_OBJECT = {%d, %d}, want {4, 2}", p.Group, p.Object)
	}
}

// TestTrackStatus_OmitsLargestObjectBeforeAnyObjects: a known track with no
// Objects yet gets no LARGEST_OBJECT (§10.2.17), not {0, 0}.
func TestTrackStatus_OmitsLargestObjectBeforeAnyObjects(t *testing.T) {
	t.Parallel()

	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	pubStream, err := clientSess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
		Name:            []byte("cam1"),
		TrackAlias:      1,
		TrackProperties: []byte("rtp-h265"),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	querySess := dialAnotherClient(t, clientSess)
	tsStream, err := querySess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	defer tsStream.Close()

	if _, found := tsStream.OK.Parameters.Find(message.ParamLargestObject); found {
		t.Fatal("TRACK_STATUS_OK unexpectedly carried LARGEST_OBJECT before any objects were forwarded")
	}
	if string(tsStream.OK.TrackProperties) != "rtp-h265" {
		t.Fatalf("TrackProperties = %q, want %q", tsStream.OK.TrackProperties, "rtp-h265")
	}
}

// TestRelay_TrackStatusRequestUpdateClosesSession: a REQUEST_UPDATE on a
// TRACK_STATUS stream closes the session (§10.15, §10.9).
func TestRelay_TrackStatusRequestUpdateClosesSession(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	pubSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()
	video := ns("video")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	peer, conn := dialRaw(t, l)
	stream, err := conn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() {
		_ = message.Marshal(stream, &message.TrackStatus{RequestID: 0, Namespace: video, Name: []byte("cam1")})
		if _, err := message.Parse(stream); err != nil { // TRACK_STATUS_OK
			return
		}
		_ = message.Marshal(stream, &message.RequestUpdate{RequestID: 2})
	}()

	requireSessionClosed(t, peer, "a REQUEST_UPDATE on a TRACK_STATUS stream")
}
