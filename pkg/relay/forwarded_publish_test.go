package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A PUBLISH the relay sends to a SUBSCRIBE_TRACKS holder (§6.1, §10.20) opens
// a subscription the relay serves like any other: Objects flow on the alias it
// chose (§10.11), a REQUEST_UPDATE is answered (§10.9), REQUEST_ERROR ends it,
// and it ends with PUBLISH_DONE (§10.12).

// forwardedPublishes accepts the PUBLISHes the relay forwards to sess.
func forwardedPublishes(t *testing.T, sess *session.Session) <-chan *session.Request {
	t.Helper()
	out := make(chan *session.Request, 4)
	go func() {
		for {
			r, err := sess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if _, ok := r.First.(*message.Publish); ok {
				out <- r
			}
		}
	}()
	return out
}

func awaitForwarded(t *testing.T, reqs <-chan *session.Request) *session.Request {
	t.Helper()
	select {
	case r := <-reqs:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH forwarded to the SUBSCRIBE_TRACKS holder")
	}
	return nil
}

func requireNoForward(t *testing.T, reqs <-chan *session.Request, what string) {
	t.Helper()
	select {
	case r := <-reqs:
		p := r.First.(*message.Publish)
		t.Fatalf("%s: forwarded PUBLISH for %s", what, p.Name)
	case <-time.After(300 * time.Millisecond):
	}
}

func subscribeTracks(t *testing.T, sess *session.Session, fields ...string) {
	t.Helper()
	ts, err := sess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns(fields...)})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
}

// publishVideoTrack PUBLISHes video/<name> from sess with the given alias.
func publishVideoTrack(
	t *testing.T,
	sess *session.Session,
	name string,
	alias uint64,
	params ...message.Parameter,
) *session.Publication {
	t.Helper()
	p, err := sess.Publish(t.Context(), &message.Publish{
		Namespace: ns("video"), Name: []byte(name), TrackAlias: alias, Parameters: params,
	})
	if err != nil {
		t.Fatalf("Publish %s: %v", name, err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// acceptForwarded replies PUBLISH_OK, failing — rather than hanging on the
// unbuffered test pipe — if the relay never reads the reply.
func acceptForwarded(t *testing.T, r *session.Request) *session.IncomingPublication {
	t.Helper()
	type result struct {
		in  *session.IncomingPublication
		err error
	}
	done := make(chan result, 1)
	go func() {
		in, err := r.AcceptPublish()
		done <- result{in, err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("AcceptPublish: %v", res.err)
		}
		return res.in
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never read the PUBLISH_OK")
	}
	return nil
}

// sendObject writes one Object in group on alias, off the test goroutine.
func sendObject(sess *session.Session, alias, group uint64) {
	sg, err := sess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: group,
	})
	if err != nil {
		return
	}
	_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
	_ = sg.Close()
}

// awaitObjectOn waits for a subgroup stream on alias and reads its first Object.
func awaitObjectOn(t *testing.T, sess *session.Session, alias uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			t.Fatalf("no Object delivered on the forwarded PUBLISH's alias %d: %v", alias, err)
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok || sg.Header.TrackAlias != alias {
			continue
		}
		if _, err := sg.ReadObject(); err != nil {
			t.Fatalf("ReadObject: %v", err)
		}
		return
	}
}

func TestForwardedPublish_DeliversObjects(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrack(t, pubSess, "cam", 7)
	fwd := awaitForwarded(t, reqs)
	in := acceptForwarded(t, fwd)
	go sendObject(pubSess, 7, 1)
	awaitObjectOn(t, subSess, in.TrackAlias())
}

// TestForwardedPublish_UninterestedStopsDelivery: "A subscriber receiving a
// PUBLISH for a Track it does not wish to receive SHOULD send REQUEST_ERROR
// with error code UNINTERESTED" (§10.11); the relay stops serving it.
func TestForwardedPublish_UninterestedStopsDelivery(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrack(t, pubSess, "cam", 7)
	fwd := awaitForwarded(t, reqs)
	alias := fwd.First.(*message.Publish).TrackAlias
	// REQUEST_ERROR and a FIN only: no STOP_SENDING, which would end the
	// subscription by itself (§3.3.3) whatever the relay made of the error.
	refused := make(chan struct{})
	go func() {
		_ = message.Marshal(fwd.Stream, &message.RequestError{
			ErrorCode: moqt.RequestUninterested, ErrorReason: "no thanks",
		})
		_ = fwd.Stream.Close()
		close(refused)
	}()
	select {
	case <-refused:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never read the REQUEST_ERROR")
	}
	time.Sleep(100 * time.Millisecond) // let the relay act on it

	go sendObject(pubSess, 7, 1)
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	for {
		ds, err := subSess.AcceptDataStream(ctx)
		if err != nil {
			return // nothing delivered: correct
		}
		if sg, ok := ds.(*session.IncomingSubgroupStream); ok && sg.Header.TrackAlias == alias {
			t.Fatal("relay kept delivering after REQUEST_ERROR UNINTERESTED")
		}
	}
}

// TestForwardedPublish_EndsWithPublishDone: when the track's publisher goes
// away the forwarded subscription ends with PUBLISH_DONE, not a bare FIN.
func TestForwardedPublish_EndsWithPublishDone(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	pub := publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)
	fwd := awaitForwarded(t, reqs)
	in := acceptForwarded(t, fwd)
	msgs := streamMessages(t, in.Stream)
	_ = pub.Done(moqt.PublishDoneTrackEnded, "bye")
	if m := nextMessage(t, msgs); m.Type() != message.TypePublishDone {
		t.Fatalf("got %T, want PUBLISH_DONE", m)
	}
}

// TestForwardedPublish_AnswersRequestUpdate: the subscriber of a PUBLISH may
// send REQUEST_UPDATE (§10.9), and it gets its single mandated response.
func TestForwardedPublish_AnswersRequestUpdate(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)
	fwd := awaitForwarded(t, reqs)
	acceptForwarded(t, fwd)
	// Off the test goroutine: the REQUEST_UPDATE write itself blocks on the
	// unbuffered test pipe if the relay never reads it.
	answered := make(chan error, 1)
	go func() {
		_, err := subSess.UpdateRequest(t.Context(), fwd.Stream, message.Parameters{message.ForwardParam(false)})
		answered <- err
	}()
	select {
	case err := <-answered:
		if err != nil {
			t.Fatalf("REQUEST_UPDATE on a forwarded PUBLISH: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("REQUEST_UPDATE on a forwarded PUBLISH was never answered")
	}
}

// TestForwardedPublish_OnePerTrack: a second publisher of a track the
// subscriber already receives does not open a second subscription to it.
func TestForwardedPublish_OnePerTrack(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)
	fwd := awaitForwarded(t, reqs)
	acceptForwarded(t, fwd)
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 9)
	requireNoForward(t, reqs, "a second publisher of a forwarded track")
}

// TestForwardedPublish_ExistingTrackAnnounced: SUBSCRIBE_TRACKS asks for
// "all tracks within matching namespaces" (§10.20), including one published
// before it arrived.
func TestForwardedPublish_ExistingTrackAnnounced(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam", 7)

	subSess := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, subSess)
	subscribeTracks(t, subSess, "video")
	fwd := awaitForwarded(t, reqs)
	if name := string(fwd.First.(*message.Publish).Name); name != "cam" {
		t.Fatalf("forwarded %q, want cam", name)
	}
}

// TestForwardedPublish_OwnTrackNotEchoed: §6.1 "the publisher sends PUBLISH
// messages for tracks within matching namespaces, excluding tracks published
// by the subscriber".
func TestForwardedPublish_OwnTrackNotEchoed(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, sess, "video")
	reqs := forwardedPublishes(t, sess)
	publishVideoTrack(t, sess, "mine", 7)
	requireNoForward(t, reqs, "the subscriber's own track")
}

// TestForwardedPublish_DropsUpstreamParameters: Message Parameters "are not
// forwarded by Relays" (§10.2.1); an upstream's AUTHORIZATION_TOKEN in
// particular means nothing on another session.
func TestForwardedPublish_DropsUpstreamParameters(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	tok := message.AuthorizationTokenParam(message.Token{
		AliasType: message.AliasTypeUseValue, TokenType: 1, TokenValue: []byte("secret"),
	})
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7, tok)
	fwd := awaitForwarded(t, reqs)
	if _, found := fwd.First.(*message.Publish).Parameters.Find(message.ParamAuthorizationToken); found {
		t.Fatal("forwarded PUBLISH carries the upstream's AUTHORIZATION_TOKEN")
	}
}

// TestForwardedPublish_EchoesSubscribeTracksParameters: SUBSCRIBE_TRACKS
// parameters "are used by the publisher as the initial Subscription parameters
// ... These Parameters are explicitly communicated in PUBLISH" (§10.20.1) —
// except AUTHORIZATION_TOKEN, which "MUST NOT be copied from a
// SUBSCRIBE_TRACKS to the resulting PUBLISH message Parameters" (§10.2.2).
func TestForwardedPublish_EchoesSubscribeTracksParameters(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	tok := message.AuthorizationTokenParam(message.Token{
		AliasType: message.AliasTypeUseValue, TokenType: 1, TokenValue: []byte("mine"),
	})
	ts, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
		Parameters:           message.Parameters{message.SubscriberPriorityParam(9), tok},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	reqs := forwardedPublishes(t, subSess)

	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)
	params := awaitForwarded(t, reqs).First.(*message.Publish).Parameters
	if p, found := params.Find(message.ParamSubscriberPriority); !found || p.Byte != 9 {
		t.Errorf("forwarded PUBLISH SUBSCRIBER_PRIORITY = %+v (found %v), want 9", p, found)
	}
	if _, found := params.Find(message.ParamAuthorizationToken); found {
		t.Error("forwarded PUBLISH carries the SUBSCRIBE_TRACKS's AUTHORIZATION_TOKEN")
	}
}

// TestForwardedPublish_SubscribedTrackNotForwarded: a track the subscriber
// already receives through its own SUBSCRIBE is not forwarded as well.
func TestForwardedPublish_SubscribedTrackNotForwarded(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam", 7)
	sess := dialAnotherClient(t, pubSess)
	sub, err := sess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	reqs := forwardedPublishes(t, sess)
	subscribeTracks(t, sess, "video")
	requireNoForward(t, reqs, "a track the subscriber already SUBSCRIBEd to")
}

// TestForwardedPublish_RepublishedTrackForwardedAgain: once a forwarded
// subscription has ended with PUBLISH_DONE, a new publication of the track is
// forwarded afresh, whether or not the subscriber has closed its side yet
// (§10.12: the sender "can immediately destroy subscription state").
func TestForwardedPublish_RepublishedTrackForwardedAgain(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, "video")
	reqs := forwardedPublishes(t, subSess)

	pub := publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)
	in := acceptForwarded(t, awaitForwarded(t, reqs))
	msgs := streamMessages(t, in.Stream)
	_ = pub.Done(moqt.PublishDoneTrackEnded, "bye")
	if m := nextMessage(t, msgs); m.Type() != message.TypePublishDone {
		t.Fatalf("got %T, want PUBLISH_DONE", m)
	}
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 9)
	awaitForwarded(t, reqs)
}

// TestForwardedPublish_SubscribeTracksParametersValidated: SUBSCRIBE_TRACKS
// carries SUBSCRIBE's parameters (§10.20.1), so it is refused on the same terms
// — a malformed LOCATION_FILTER gets MALFORMED_TRACK, as on SUBSCRIBE — rather
// than accepted and then failing every forwarded PUBLISH.
func TestForwardedPublish_SubscribeTracksParametersValidated(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	_, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
		Parameters:           message.Parameters{message.BytesParam(message.ParamLocationFilter, []byte{0xFF})},
	})
	requireRejectedWithCode(t, err, moqt.RequestMalformedTrack)
}
