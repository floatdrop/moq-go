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

// The send side of §10.5 (request_ok_properties_test.go has the receive
// side): the session refuses to send Track Properties in a REQUEST_OK the
// peer would have to close the session over, and sends nothing.

// TestReplyRefusesTrackPropertiesWhereEmpty: Request.Reply returns
// ErrTrackPropertiesNotAllowed, the peer's session stays up, and the request
// can still be answered with an empty REQUEST_OK.
func TestReplyRefusesTrackPropertiesWhereEmpty(t *testing.T) {
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
			select {
			case <-client.Done():
				t.Fatalf("the peer closed the session: %v", client.Err())
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// updater is a client request handle that can send REQUEST_UPDATE.
type updater interface {
	Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error)
}

// TestReplyRefusesUpdateOKTrackProperties: on a SUBSCRIBE or FETCH stream the
// first answer is SUBSCRIBE_OK or FETCH_OK, so a REQUEST_OK written with
// Reply there is a REQUEST_UPDATE_OK, and §10.5 says its Track Properties are
// empty too.
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
			select {
			case <-client.Done():
				t.Fatalf("the peer closed the session: %v", client.Err())
			case <-time.After(50 * time.Millisecond):
			}
		})
	}
}

// TestReplyKeepsTrackStatusProperties: TRACK_STATUS_OK is where Track
// Properties belong, so Reply sends them.
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

// TestUpdateHandlerTrackPropertiesRefused: an update handler that returns a
// REQUEST_UPDATE_OK with Track Properties gets a REQUEST_ERROR
// (INTERNAL_ERROR) sent in its place, and the session stays up.
func TestUpdateHandlerTrackPropertiesRefused(t *testing.T) {
	client, sub, _ := servedPublicationWith(t, func(*message.RequestUpdate) (*message.RequestOK, error) {
		return &message.RequestOK{TrackProperties: trackProps}, nil
	})
	_, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)})
	rej, ok := errors.AsType[*session.RequestRejectedError](err)
	if !ok || rej.Code != moqt.RequestInternalError {
		t.Fatalf("Update = %v, want REQUEST_ERROR INTERNAL_ERROR", err)
	}
	select {
	case <-client.Done():
		t.Fatalf("the session closed: %v", client.Err())
	case <-time.After(50 * time.Millisecond):
	}
}
