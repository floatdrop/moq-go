package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_TrackStatusRequestUpdateClosesSession: TRACK_STATUS cannot be
// updated (§10.15), and "An endpoint that receives a REQUEST_UPDATE other than
// in the two cases above MUST close the session with a PROTOCOL_VIOLATION"
// (§10.9). The relay used to FIN its side and never read the requester's.
func TestRelay_TrackStatusRequestUpdateClosesSession(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	pubSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()
	ns := wire.TrackNamespace{[]byte("video")}
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	peer, conn := dialRaw(t, l)
	stream, err := conn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() {
		_ = message.Marshal(stream, &message.TrackStatus{RequestID: 0, Namespace: ns, Name: []byte("cam1")})
		if _, err := message.Parse(stream); err != nil { // TRACK_STATUS_OK
			return
		}
		_ = message.Marshal(stream, &message.RequestUpdate{RequestID: 2})
	}()

	select {
	case <-peer.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("relay left the session open after a REQUEST_UPDATE on a TRACK_STATUS stream")
	}
}
