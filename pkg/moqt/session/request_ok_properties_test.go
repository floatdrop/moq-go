package session_test

import (
	"context"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §10.5 REQUEST_OK: "Track Properties are populated in TRACK_STATUS_OK; they
// are empty in PUBLISH_OK, REQUEST_UPDATE_OK, SUBSCRIBE_NAMESPACE_OK and
// PUBLISH_NAMESPACE_OK. If an endpoint receives Track Properties in one of
// these messages it MUST close the session with a PROTOCOL_VIOLATION."

var trackProps = message.AppendTrackProperties([]wire.KVPair{
	{Type: message.PropertyDefaultPublisherPriority, IntVal: 1},
})

// replyOKWithProperties accepts the next request on server and answers it
// with a REQUEST_OK carrying Track Properties.
func replyOKWithProperties(t *testing.T, server *session.Session) {
	t.Helper()
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = message.Marshal(req.Stream, &message.RequestOK{TrackProperties: trackProps})
	}()
}

func TestRequestOKWithTrackPropertiesClosesSession(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("ns")}
	for _, tc := range []struct {
		name string
		send func(context.Context, *session.Session) error
	}{
		{"PUBLISH_OK", func(ctx context.Context, c *session.Session) error {
			_, err := c.Publish(ctx, &message.Publish{Namespace: ns, Name: []byte("t")})
			return err
		}},
		{"PUBLISH_NAMESPACE_OK", func(ctx context.Context, c *session.Session) error {
			_, err := c.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns})
			return err
		}},
		{"SUBSCRIBE_NAMESPACE_OK", func(ctx context.Context, c *session.Session) error {
			_, err := c.SubscribeNamespace(ctx, &message.SubscribeNamespace{TrackNamespacePrefix: ns})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			replyOKWithProperties(t, server)
			if err := tc.send(t.Context(), client); err == nil {
				t.Fatal("request succeeded despite Track Properties in its REQUEST_OK")
			}
			requireClosedProtocolViolation(t, client)
		})
	}
}

// TestRequestUpdateOKWithTrackPropertiesClosesSession covers REQUEST_UPDATE_OK,
// which arrives on an established request stream rather than as its first
// response — read directly by Update, or routed by a RequestBroker's Serve loop.
func TestRequestUpdateOKWithTrackPropertiesClosesSession(t *testing.T) {
	for _, viaBroker := range []bool{false, true} {
		name := "direct"
		if viaBroker {
			name = "broker"
		}
		t.Run(name, func(t *testing.T) { testRequestUpdateOKWithTrackProperties(t, viaBroker) })
	}
}

func testRequestUpdateOKWithTrackProperties(t *testing.T, viaBroker bool) {
	client, server := openPair(t)
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = req.Reply(&message.SubscribeOK{TrackAlias: 1})
		if _, err := message.Parse(req.Stream); err != nil { // the REQUEST_UPDATE
			return
		}
		_ = message.Marshal(req.Stream, &message.RequestOK{TrackProperties: trackProps})
	}()

	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if viaBroker {
		b := sub.Broker()
		go func() { _ = b.Serve(t.Context(), nil) }()
	}
	if _, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(false)}); err == nil {
		t.Fatal("Update succeeded despite Track Properties in REQUEST_UPDATE_OK")
	}
	requireClosedProtocolViolation(t, client)
}

// TestTrackStatusOKKeepsTrackProperties is the other side of the rule:
// TRACK_STATUS_OK is where Track Properties belong.
func TestTrackStatusOKKeepsTrackProperties(t *testing.T) {
	client, server := openPair(t)
	replyOKWithProperties(t, server)
	ts, err := client.TrackStatus(t.Context(), &message.TrackStatus{Name: []byte("t")})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	if len(ts.OK.TrackProperties) == 0 {
		t.Error("TRACK_STATUS_OK lost its Track Properties")
	}
	select {
	case <-client.Done():
		t.Fatalf("session closed on a TRACK_STATUS_OK with Track Properties: %v", client.Err())
	default:
	}
}

// TestLateRequestUpdateOKWithTrackPropertiesClosesSession: a REQUEST_UPDATE_OK
// that arrives after its Update gave up reaches the broker's Serve loop as an
// unsolicited response. It is still a REQUEST_UPDATE_OK, and §10.5 still
// applies.
func TestLateRequestUpdateOKWithTrackPropertiesClosesSession(t *testing.T) {
	client, server := openPair(t)
	updateSeen := make(chan session.Stream, 1)
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = req.Reply(&message.SubscribeOK{TrackAlias: 1})
		if _, err := message.Parse(req.Stream); err != nil { // the REQUEST_UPDATE
			return
		}
		updateSeen <- req.Stream
	}()

	sub, err := client.Subscribe(t.Context(), &message.Subscribe{Name: []byte("t")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b := sub.Broker()
	go func() { _ = b.Serve(t.Context(), func(message.Message) bool { return true }) }()

	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		stream := <-updateSeen
		cancel() // the Update gives up before the answer arrives
		_ = message.Marshal(stream, &message.RequestOK{TrackProperties: trackProps})
	}()
	_, _ = sub.Update(ctx, message.Parameters{message.ForwardParam(false)})
	requireClosedProtocolViolation(t, client)
}
