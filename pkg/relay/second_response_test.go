package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_SecondResponseCloses: the relay sends no REQUEST_UPDATE on a
// forwarded PUBLISH, so a REQUEST_OK or REQUEST_ERROR the subscriber sends
// after its PUBLISH_OK is a second response, and the relay closes the session
// (§5.1).
func TestRelay_SecondResponseCloses(t *testing.T) {
	t.Parallel()
	for _, resp := range []message.Message{&message.RequestOK{}, &message.RequestError{}} {
		t.Run(resp.Type().String()+" after PUBLISH_OK on a forwarded PUBLISH", func(t *testing.T) {
			t.Parallel()
			subSess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			subscribeTracks(t, subSess, ns("video"))
			publish(t, dialAnotherClient(t, subSess), &message.Publish{
				Namespace: ns("video", "cam7"), Name: []byte("rtp"), TrackAlias: 99,
			})
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			req, err := subSess.AcceptRequest(ctx)
			if err != nil {
				t.Fatalf("AcceptRequest: %v", err)
			}
			if _, err := req.AcceptPublish(); err != nil {
				t.Fatalf("AcceptPublish: %v", err)
			}
			if err := message.Marshal(req.Stream, resp); err != nil {
				t.Fatalf("write second response: %v", err)
			}
			requireSessionClosed(t, subSess, "a second response to a forwarded PUBLISH")
		})
	}
}
