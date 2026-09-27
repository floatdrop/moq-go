package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
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

// TestRelay_StrayResponseCloses: on a request stream the relay answered, the
// requester has nothing to answer, since only it sends REQUEST_UPDATE there
// (§10.9), so a REQUEST_OK or REQUEST_ERROR from it is a protocol violation
// and the relay closes the session. On a PUBLISH, the relay's subscriber side
// may send one, but has not.
func TestRelay_StrayResponseCloses(t *testing.T) {
	t.Parallel()
	for _, rc := range []struct {
		request string
		open    func(t *testing.T) (*session.Session, session.Stream)
	}{
		{"SUBSCRIBE", func(t *testing.T) (*session.Session, session.Stream) {
			pubSess, _ := newCam1Publisher(t, nil)
			subSess := dialAnotherClient(t, pubSess)
			return subSess, subscribeCam1(t, subSess).Stream
		}},
		{"FETCH", func(t *testing.T) (*session.Session, session.Stream) {
			pubSess, subSess, alias := publishAndCache(t)
			sendObjects(pubSess, alias, 0, 1)
			waitRelayLargest(t, subSess, ns("video"), []byte("cam1"), 0, 0)
			fr, err := subSess.Fetch(t.Context(), &message.Fetch{Namespace: ns("video"), Name: []byte("cam1")})
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			return subSess, fr.Stream
		}},
		{"PUBLISH_NAMESPACE", func(t *testing.T) (*session.Session, session.Stream) {
			sess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			return sess, publishNS(t, sess, "video").Stream
		}},
		{"SUBSCRIBE_NAMESPACE", func(t *testing.T) (*session.Session, session.Stream) {
			sess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			s, _ := subscribeNS(t, sess, "video")
			return sess, s.Stream
		}},
		{"PUBLISH", func(t *testing.T) (*session.Session, session.Stream) {
			sess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			return sess, publishVideoTrack(t, sess, "cam1", 7).Stream
		}},
		{"SUBSCRIBE_TRACKS", func(t *testing.T) (*session.Session, session.Stream) {
			sess, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			return sess, subscribeTracks(t, sess, ns("video"))
		}},
	} {
		for _, resp := range []message.Message{&message.RequestOK{}, &message.RequestError{}} {
			t.Run(resp.Type().String()+" on "+rc.request, func(t *testing.T) {
				t.Parallel()
				sess, stream := rc.open(t)
				if err := message.Marshal(stream, resp); err != nil {
					t.Fatalf("write %s: %v", resp.Type(), err)
				}
				requireSessionClosed(t, sess, "a stray "+resp.Type().String())
			})
		}
	}
}
