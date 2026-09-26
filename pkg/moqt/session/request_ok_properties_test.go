package session_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §10.5 REQUEST_OK: Track Properties belong in TRACK_STATUS_OK and are empty
// in PUBLISH_OK, REQUEST_UPDATE_OK, SUBSCRIBE_NAMESPACE_OK and
// PUBLISH_NAMESPACE_OK; receiving them there is a PROTOCOL_VIOLATION. The
// session also refuses to send them there.

var trackProps = message.AppendTrackProperties([]wire.KVPair{
	{Type: message.PropertyDefaultPublisherPriority, IntVal: 1},
})

// emptyPropertiesOKs are the requests whose first answer is a REQUEST_OK that must carry no Track Properties.
var emptyPropertiesOKs = []struct {
	name string
	send func(context.Context, *session.Session) error
}{
	{"PUBLISH_OK", func(ctx context.Context, c *session.Session) error {
		_, err := c.Publish(ctx, &message.Publish{Namespace: wire.TrackNamespace{[]byte("ns")}, Name: []byte("t")})
		return err
	}},
	{"PUBLISH_NAMESPACE_OK", func(ctx context.Context, c *session.Session) error {
		_, err := c.PublishNamespace(ctx, &message.PublishNamespace{Namespace: wire.TrackNamespace{[]byte("ns")}})
		return err
	}},
	{"SUBSCRIBE_NAMESPACE_OK", func(ctx context.Context, c *session.Session) error {
		_, err := c.SubscribeNamespace(ctx, &message.SubscribeNamespace{
			TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
		})
		return err
	}},
}

// replyOKWithProperties answers the next request on server with a REQUEST_OK carrying Track Properties.
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

// ---------------------------------------------------------------------------
// Receive side
// ---------------------------------------------------------------------------

func TestRequestOKWithTrackPropertiesClosesSession(t *testing.T) {
	for _, tc := range emptyPropertiesOKs {
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
// read directly by Update or routed by a RequestBroker's Serve loop.
func TestRequestUpdateOKWithTrackPropertiesClosesSession(t *testing.T) {
	for _, viaBroker := range []bool{false, true} {
		name := "direct"
		if viaBroker {
			name = "broker"
		}
		t.Run(name, func(t *testing.T) {
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
		})
	}
}

// TestLateRequestUpdateOKWithTrackPropertiesClosesSession: a REQUEST_UPDATE_OK
// arriving after its Update gave up reaches Serve as unsolicited, and §10.5
// still applies.
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

// TestTrackStatusOKKeepsTrackProperties: TRACK_STATUS_OK is where Track
// Properties belong.
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

// ---------------------------------------------------------------------------
// Send side: the session refuses, sends nothing, and the peer stays up
// ---------------------------------------------------------------------------

// TestReplyRefusesTrackPropertiesWhereEmpty: Reply returns
// ErrTrackPropertiesNotAllowed, and an empty REQUEST_OK can still be sent.
func TestReplyRefusesTrackPropertiesWhereEmpty(t *testing.T) {
	for _, tc := range emptyPropertiesOKs {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			replied := make(chan error, 1)
			go func() {
				req, err := server.AcceptRequest(t.Context())
				if err != nil {
					replied <- err
					return
				}
				err = req.Reply(&message.RequestOK{TrackProperties: trackProps})
				replied <- err
				if err != nil {
					_ = req.Reply(&message.RequestOK{})
				}
			}()
			if err := tc.send(t.Context(), client); err != nil {
				t.Fatalf("request failed: %v", err)
			}
			if err := <-replied; !errors.Is(err, session.ErrTrackPropertiesNotAllowed) {
				t.Fatalf("Reply with Track Properties = %v, want ErrTrackPropertiesNotAllowed", err)
			}
			requireStaysOpen(t, client, 50*time.Millisecond)
		})
	}
}

// updater is a client request handle that can send REQUEST_UPDATE.
type updater interface {
	Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error)
}

// streamUpdater updates a request whose handle has no Update method.
type streamUpdater struct {
	s      *session.Session
	stream session.Stream
}

func (u streamUpdater) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	return u.s.UpdateRequest(ctx, u.stream, params)
}

// TestReplyRefusesUpdateOKTrackProperties: a REQUEST_OK after a SUBSCRIBE_OK
// or FETCH_OK is a REQUEST_UPDATE_OK, whose Track Properties are empty (§10.5);
// so is every SUBSCRIBE_TRACKS REQUEST_OK after the first.
func TestReplyRefusesUpdateOKTrackProperties(t *testing.T) {
	for _, tc := range []struct {
		name string
		// answer sends the request's first response; open sends the request
		// from the client and returns the handle to update it with.
		answer func(*session.Request) error
		open   func(context.Context, *session.Session) (updater, error)
	}{
		{
			"SUBSCRIBE",
			func(r *session.Request) error { return r.Reply(&message.SubscribeOK{TrackAlias: 1}) },
			func(ctx context.Context, c *session.Session) (updater, error) {
				return c.Subscribe(ctx, &message.Subscribe{Name: []byte("t")})
			},
		},
		{
			// §10.5 does not name the SUBSCRIBE_TRACKS OK, so its first
			// REQUEST_OK may carry Track Properties.
			"SUBSCRIBE_TRACKS",
			func(r *session.Request) error { return r.Reply(&message.RequestOK{TrackProperties: trackProps}) },
			func(ctx context.Context, c *session.Session) (updater, error) {
				ts, err := c.SubscribeTracks(ctx, &message.SubscribeTracks{
					TrackNamespacePrefix: wire.TrackNamespace{[]byte("ns")},
				})
				if err != nil {
					return nil, err
				}
				return streamUpdater{c, ts.Stream}, nil
			},
		},
		{
			"FETCH",
			func(r *session.Request) error { _, err := r.AcceptFetch(nil); return err },
			func(ctx context.Context, c *session.Session) (updater, error) {
				return c.Fetch(ctx, &message.Fetch{Name: []byte("t")})
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := openPair(t)
			replied := make(chan error, 1)
			go func() {
				req, err := server.AcceptRequest(t.Context())
				if err != nil {
					replied <- err
					return
				}
				if err := tc.answer(req); err != nil {
					replied <- err
					return
				}
				if _, err := message.Parse(req.Stream); err != nil { // the REQUEST_UPDATE
					replied <- err
					return
				}
				err = req.Reply(&message.RequestOK{TrackProperties: trackProps})
				replied <- err
				if err != nil {
					_ = req.Reply(&message.RequestOK{})
				}
			}()
			h, err := tc.open(t.Context(), client)
			must(t, err)
			if _, err := h.Update(t.Context(), message.Parameters{message.ForwardParam(true)}); err != nil {
				t.Fatalf("Update: %v", err)
			}
			if err := <-replied; !errors.Is(err, session.ErrTrackPropertiesNotAllowed) {
				t.Fatalf("Reply with Track Properties = %v, want ErrTrackPropertiesNotAllowed", err)
			}
			requireStaysOpen(t, client, 50*time.Millisecond)
		})
	}
}

// TestReplyKeepsTrackStatusProperties: Reply sends Track Properties in a
// TRACK_STATUS_OK.
func TestReplyKeepsTrackStatusProperties(t *testing.T) {
	client, server := openPair(t)
	replied := make(chan error, 1)
	go func() {
		req, err := server.AcceptRequest(t.Context())
		if err != nil {
			replied <- err
			return
		}
		replied <- req.Reply(&message.RequestOK{TrackProperties: trackProps})
	}()
	ts, err := client.TrackStatus(t.Context(), &message.TrackStatus{Name: []byte("t")})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	must(t, <-replied)
	if len(ts.OK.TrackProperties) == 0 {
		t.Error("TRACK_STATUS_OK lost its Track Properties")
	}
}

// TestUpdateHandlerTrackPropertiesRefused: an update handler returning Track
// Properties gets REQUEST_ERROR INTERNAL_ERROR sent instead; the session stays up.
func TestUpdateHandlerTrackPropertiesRefused(t *testing.T) {
	client, sub, _ := servedPublicationWith(t, func(*message.RequestUpdate) (*message.RequestOK, error) {
		return &message.RequestOK{TrackProperties: trackProps}, nil
	})
	_, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.Code != moqt.RequestInternalError {
		t.Fatalf("Update = %v, want REQUEST_ERROR INTERNAL_ERROR", err)
	}
	requireStaysOpen(t, client, 50*time.Millisecond)
}
