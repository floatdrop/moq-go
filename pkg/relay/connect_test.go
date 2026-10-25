package relay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// refusePublish refuses every PUBLISH: a peer's, that is; the relay's own
// program publishes on Connect's session whatever its Authorizer says.
type refusePublish struct{ relay.AllowAllAuthorizer }

func (refusePublish) AuthorizePublish(context.Context, *session.Session, *message.Publish) error {
	return errors.New("refused")
}

func TestConnectPublishesTheRelaysOwnTrack(t *testing.T) {
	t.Parallel()
	tr := startTestRelay(t.Context(), relay.Config{Authorizer: refusePublish{}})
	defer tr.stop(t)

	own, err := tr.r.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	const alias = uint64(3)
	pub, err := own.Publish(
		t.Context(),
		&message.Publish{Namespace: ns("room"), Name: []byte("link/p1"), TrackAlias: alias},
	)
	if err != nil {
		t.Fatalf("Publish on the relay's own session: %v", err)
	}
	defer pub.Close()

	// A peer's PUBLISH is still refused by the Authorizer.
	peer := dialClient(t, tr)
	if _, err := peer.Publish(
		t.Context(),
		&message.Publish{Namespace: ns("room"), Name: []byte("link/p2"), TrackAlias: 1},
	); err == nil {
		t.Fatal("a peer's PUBLISH went through a refusing Authorizer")
	}

	sub, err := peer.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("room"), Name: []byte("link/p1")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer sub.Close()
	got := make(chan *message.ObjectDatagram, 1)
	go func() {
		if d, err := peer.ReceiveDatagram(t.Context()); err == nil {
			got <- d
		}
	}()
	// Sent until one arrives: the subscription reaches the fanout a moment
	// after SUBSCRIBE_OK.
	deadline := time.After(2 * time.Second)
	for g := uint64(0); ; g++ {
		if err := own.SendDatagram(
			&message.ObjectDatagram{Type: 0x08, TrackAlias: alias, GroupID: g, ObjectPayload: []byte("rtt=42")},
		); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		select {
		case d := <-got:
			if d.TrackAlias != sub.OK.TrackAlias || string(d.ObjectPayload) != "rtt=42" {
				t.Fatalf("got alias %d payload %q", d.TrackAlias, d.ObjectPayload)
			}
			return
		case <-time.After(20 * time.Millisecond):
		case <-deadline:
			t.Fatal("the relay's own datagram did not reach its subscriber")
		}
	}
}

func TestConnectEndsWithTheRelay(t *testing.T) {
	t.Parallel()
	tr := startTestRelay(t.Context(), relay.Config{})
	own, err := tr.r.Connect(t.Context())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	tr.stop(t)
	select {
	case <-own.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the relay's own session outlived its Stop")
	}
	if _, err := tr.r.Connect(t.Context()); err == nil {
		t.Fatal("Connect after Stop succeeded")
	}
}
