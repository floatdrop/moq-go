package relay_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// staleStore is a Discovery store that never forgets an advertisement, as an
// eventually-consistent backend may still list a relay that is draining.
type staleStore struct{ discovery.DiscoveryStore }

func (staleStore) UnpublishNamespace(context.Context, wire.TrackNamespace, string) error { return nil }
func (staleStore) UnpublishTrack(context.Context, track.Key, string) error               { return nil }
func (staleStore) Withdraw(context.Context, string) error                                { return nil }

// TestCrossRelay_DrainingUpstreamRelayAnswersGoingAway: a SUBSCRIBE whose only
// candidate is a relay reached through the upstream pool that has sent GOAWAY
// is refused with GOING_AWAY, as with a draining local publisher: "The
// endpoint has received a GOAWAY and MAY reject new requests" (§10.6.2). The
// relay sends that relay no request (§10.4), so no answer of its own exists.
//
// A local publisher answering DOES_NOT_EXIST alongside does not change that:
// the draining relay's publisher may still have the track.
func TestCrossRelay_DrainingUpstreamRelayAnswersGoingAway(t *testing.T) {
	t.Parallel()
	t.Run("only candidate", func(t *testing.T) {
		t.Parallel()
		testDrainingUpstreamRelay(t, false)
	})
	t.Run("local publisher refuses", func(t *testing.T) {
		t.Parallel()
		testDrainingUpstreamRelay(t, true)
	})
}

// refuseAll answers every request on sess with DOES_NOT_EXIST.
func refuseAll(t *testing.T, sess *session.Session) {
	go func() {
		for {
			req, err := sess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			_ = req.RejectError(moqt.RequestDoesNotExist, "no cam1")
		}
	}()
}

func testDrainingUpstreamRelay(t *testing.T, localRefuser bool) {
	store := staleStore{discovery.NewMemoryStore()}
	defer store.Close()
	relayB := startTestRelay(t.Context(), relay.Config{
		Discovery: store, RelayAddr: "relay-B", GoawayTimeout: 10 * time.Second,
	})
	relayA := startTestRelay(t.Context(), relay.Config{
		Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(nil, relayB),
	})

	pub := dialClient(t, relayB)
	publishNS(t, pub, "video")
	refuseAll(t, pub)
	if localRefuser {
		local := dialClient(t, relayA)
		publishNS(t, local, "video")
		refuseAll(t, local)
	}
	subSess := dialClient(t, relayA)
	subscribe := func() error {
		_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
		return err
	}
	// Dials relay B, whose publisher has no cam1.
	requireRejectedWithCode(t, subscribe(), moqt.RequestDoesNotExist)

	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		_ = relayB.r.Stop(context.Background())
	}()
	defer func() {
		// B's drain ends once its sessions do: the publisher's and the
		// pooled one relayA.stop closes.
		_ = pub.Close(moqt.SessionNoError, "done")
		relayA.stop(t)
		<-stopped
	}()

	waitFor(t, 2*time.Second, func() bool {
		rej, ok := errors.AsType[*session.RequestRejectedError](subscribe())
		return ok && rej.Code == moqt.RequestGoingAway
	}, "a SUBSCRIBE whose only upstream relay is draining was never refused with GOING_AWAY")
}
