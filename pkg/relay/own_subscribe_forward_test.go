package relay_test

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A SUBSCRIBE_TRACKS holder is not forwarded a PUBLISH for a track it
// SUBSCRIBEs to itself: it receives the track on that subscription. While the
// SUBSCRIBE is between establishing its upstream and registering its
// downstream, the track has an upstream but no downstream on the session, so
// every path that forwards must see the SUBSCRIBE in flight.
//
// The tests that hold a SUBSCRIBE there are not parallel: they install the
// process-wide hook.

// parkOwnSubscribe holds the relay's SUBSCRIBE for name just before it
// registers its downstream, until release. parked is closed once it is held.
// Pass release to [ownSubscribeRelay], which runs it at cleanup ahead of the
// relay's teardown, since that joins the held handler.
func parkOwnSubscribe(t *testing.T, name string) (parked <-chan struct{}, release func()) {
	t.Helper()
	held := make(chan struct{})
	gate := make(chan struct{})
	var holdOnce sync.Once
	restore := relay.SetTestHookBeforeDownstreamRegistered(func(n track.FullTrackName) {
		if string(n.Name) == name {
			holdOnce.Do(func() { close(held) })
			<-gate
		}
	})
	t.Cleanup(restore)
	return held, sync.OnceFunc(func() { close(gate) })
}

// ownSubscribeRelay starts a relay with a video PUBLISH_NAMESPACE publisher
// answering every SUBSCRIBE, and a client on it whose forwarded PUBLISHes
// arrive on reqs. release runs before the relay's teardown.
func ownSubscribeRelay(
	t *testing.T,
	release func(),
) (pubSess, subSess *session.Session, reqs <-chan *session.Request) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	t.Cleanup(release)
	publishNS(t, pubSess, "video")
	go func() {
		for {
			r, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if _, ok := r.First.(*message.Subscribe); ok {
				_ = r.Reply(&message.SubscribeOK{TrackAlias: 7})
			}
		}
	}()
	subSess = dialAnotherClient(t, pubSess)
	return pubSess, subSess, forwardedPublishes(t, subSess)
}

// subscribeHeld SUBSCRIBEs sess to video/name, waits until the relay holds it
// at parked, and returns the channel its result arrives on.
func subscribeHeld(t *testing.T, sess *session.Session, name string, parked <-chan struct{}) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		sub, err := sess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte(name)})
		if err == nil {
			t.Cleanup(func() { _ = sub.Close() })
		}
		done <- err
	}()
	select {
	case <-parked:
	case <-time.After(2 * time.Second):
		t.Fatal("the SUBSCRIBE never reached downstream registration")
	}
	return done
}

// releaseSubscribe lets the held SUBSCRIBE finish, requires it to succeed,
// and requires that its own registration forwards nothing either.
func releaseSubscribe(t *testing.T, release func(), done <-chan error, reqs <-chan *session.Request) {
	t.Helper()
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SUBSCRIBE never answered")
	}
	requireNoForward(t, reqs, "the session's own SUBSCRIBE")
}

// TestSubscribeTracks_OwnSubscribeInFlight_ExistingTracks: a SUBSCRIBE_TRACKS
// sent while the session's SUBSCRIBE is in flight does not forward that track
// among the existing ones (§10.20).
func TestSubscribeTracks_OwnSubscribeInFlight_ExistingTracks(t *testing.T) {
	const name = "own-window-existing"
	parked, release := parkOwnSubscribe(t, name)
	_, subSess, reqs := ownSubscribeRelay(t, release)

	done := subscribeHeld(t, subSess, name, parked)
	subscribeTracks(t, subSess, ns("video"))
	requireNoForward(t, reqs, "a SUBSCRIBE_TRACKS while the session's SUBSCRIBE is in flight")
	releaseSubscribe(t, release, done, reqs)
}

// TestSubscribeTracks_OwnSubscribeInFlight_PrefixUpdate: the same for a
// TRACK_NAMESPACE_PREFIX update that newly covers the track (§10.9.2).
func TestSubscribeTracks_OwnSubscribeInFlight_PrefixUpdate(t *testing.T) {
	const name = "own-window-update"
	parked, release := parkOwnSubscribe(t, name)
	_, subSess, reqs := ownSubscribeRelay(t, release)
	stream := subscribeTracks(t, subSess, ns("audio"))

	done := subscribeHeld(t, subSess, name, parked)
	if _, err := subSess.UpdateRequest(t.Context(), stream,
		message.Parameters{message.TrackNamespacePrefixParam(ns("video"))}); err != nil {
		t.Fatalf("REQUEST_UPDATE: %v", err)
	}
	requireNoForward(t, reqs, "a prefix update while the session's SUBSCRIBE is in flight")
	releaseSubscribe(t, release, done, reqs)
}

// TestSubscribeTracks_OwnSubscribeInFlight_NewPublish: the same for a PUBLISH
// of the track from another session (§6.1).
func TestSubscribeTracks_OwnSubscribeInFlight_NewPublish(t *testing.T) {
	const name = "own-window-publish"
	parked, release := parkOwnSubscribe(t, name)
	pubSess, subSess, reqs := ownSubscribeRelay(t, release)
	subscribeTracks(t, subSess, ns("video"))

	done := subscribeHeld(t, subSess, name, parked)
	publishVideoTrack(t, dialAnotherClient(t, pubSess), name, 9)
	requireNoForward(t, reqs, "a PUBLISH while the session's SUBSCRIBE is in flight")
	releaseSubscribe(t, release, done, reqs)
}

// TestSubscribeTracks_OwnSubscribeFails_HeldForwardSent: a forward held back
// for the session's SUBSCRIBE is sent once that SUBSCRIBE fails, since the
// track is published "within matching namespaces" (§10.20) and the session
// receives it no other way.
func TestSubscribeTracks_OwnSubscribeFails_HeldForwardSent(t *testing.T) {
	t.Parallel()
	const name = "own-subscribe-fails"
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishNS(t, pubSess, "video")
	arrived, gate := make(chan struct{}), make(chan struct{})
	refuse := sync.OnceFunc(func() { close(gate) })
	defer refuse() // before the teardown, on a failure too
	go func() {
		r, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		close(arrived)
		<-gate
		_ = r.RejectError(moqt.RequestDoesNotExist, "no such track")
	}()
	subSess := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, subSess)
	subscribeTracks(t, subSess, ns("video"))

	done := make(chan error, 1)
	go func() {
		_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte(name)})
		done <- err
	}()
	select {
	case <-arrived:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never SUBSCRIBEd upstream")
	}
	publishVideoTrack(t, dialAnotherClient(t, pubSess), name, 9)
	requireNoForward(t, reqs, "a PUBLISH while the session's SUBSCRIBE is in flight")
	refuse()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("the SUBSCRIBE succeeded, want it refused")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SUBSCRIBE never answered")
	}
	if got := publishName(awaitForwarded(t, reqs)); got != name {
		t.Fatalf("forwarded %q, want %q", got, name)
	}
}
