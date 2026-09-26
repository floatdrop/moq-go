package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// SUBSCRIBE_TRACKS (§6.1, §10.20): the relay forwards a PUBLISH per matching
// track and serves the resulting subscription like any other — Objects on the
// alias it chose (§10.11), REQUEST_UPDATE answered (§10.9), REQUEST_ERROR ends
// it, PUBLISH_DONE closes it (§10.12).

func TestForwardedPublish_DeliversObjects(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, ns("video"))
	reqs := forwardedPublishes(t, subSess)

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrack(t, pubSess, "cam", 7)
	fwd := awaitForwarded(t, reqs)
	in := acceptForwarded(t, fwd)
	go sendObjects(pubSess, 7, 1, 1)
	awaitObjectOn(t, subSess, in.TrackAlias())
}

// TestForwardedPublish_UninterestedStopsDelivery: REQUEST_ERROR UNINTERESTED on
// a forwarded PUBLISH (§10.11) stops delivery.
func TestForwardedPublish_UninterestedStopsDelivery(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, ns("video"))
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

	go sendObjects(pubSess, 7, 1, 1)
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
	subscribeTracks(t, subSess, ns("video"))
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
	subscribeTracks(t, subSess, ns("video"))
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
	subscribeTracks(t, subSess, ns("video"))
	reqs := forwardedPublishes(t, subSess)

	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 7)
	fwd := awaitForwarded(t, reqs)
	acceptForwarded(t, fwd)
	publishVideoTrack(t, dialAnotherClient(t, subSess), "cam", 9)
	requireNoForward(t, reqs, "a second publisher of a forwarded track")
}

// TestForwardedPublish_ExistingTrackAnnounced: a track published before the
// SUBSCRIBE_TRACKS is forwarded too (§10.20).
func TestForwardedPublish_ExistingTrackAnnounced(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam", 7)

	subSess := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, subSess)
	subscribeTracks(t, subSess, ns("video"))
	fwd := awaitForwarded(t, reqs)
	if name := string(fwd.First.(*message.Publish).Name); name != "cam" {
		t.Fatalf("forwarded %q, want cam", name)
	}
}

// TestForwardedPublish_OwnTrackNotEchoed: the subscriber's own tracks are not
// forwarded to it (§6.1).
func TestForwardedPublish_OwnTrackNotEchoed(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, sess, ns("video"))
	reqs := forwardedPublishes(t, sess)
	publishVideoTrack(t, sess, "mine", 7)
	requireNoForward(t, reqs, "the subscriber's own track")
}

// TestForwardedPublish_DropsUpstreamParameters: the upstream PUBLISH's Message
// Parameters are not forwarded (§10.2.1).
func TestForwardedPublish_DropsUpstreamParameters(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, ns("video"))
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

// TestForwardedPublish_EchoesSubscribeTracksParameters: the SUBSCRIBE_TRACKS
// parameters are echoed in each PUBLISH (§10.20.1), except AUTHORIZATION_TOKEN
// (§10.2.2).
func TestForwardedPublish_EchoesSubscribeTracksParameters(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	tok := message.AuthorizationTokenParam(message.Token{
		AliasType: message.AliasTypeUseValue, TokenType: 1, TokenValue: []byte("mine"),
	})
	subscribeTracks(t, subSess, ns("video"), message.SubscriberPriorityParam(9), tok)
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
	subscribeTracks(t, sess, ns("video"))
	requireNoForward(t, reqs, "a track the subscriber already SUBSCRIBEd to")
}

// TestForwardedPublish_RepublishedTrackForwardedAgain: after PUBLISH_DONE a new
// publication of the track is forwarded afresh, even before the subscriber
// closed its side (§10.12).
func TestForwardedPublish_RepublishedTrackForwardedAgain(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	subscribeTracks(t, subSess, ns("video"))
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
// parameters are validated as on SUBSCRIBE (§10.20.1): a malformed
// LOCATION_FILTER is MALFORMED_TRACK.
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

// A REQUEST_UPDATE on SUBSCRIBE_TRACKS applies to the PUBLISHes sent from then
// on, not to existing subscriptions (§10.2.18 FORWARD, §10.9.2 prefix); tracks
// that newly match are forwarded when it applies (§10.20).

// updateSubscribeTracks sends a REQUEST_UPDATE with ps on a SUBSCRIBE_TRACKS
// stream, failing the test unless it is accepted.
func updateSubscribeTracks(t *testing.T, sess *session.Session, stream session.Stream, ps ...message.Parameter) {
	t.Helper()
	if _, err := sess.UpdateRequest(t.Context(), stream, ps); err != nil {
		t.Fatalf("REQUEST_UPDATE on SUBSCRIBE_TRACKS: %v", err)
	}
}

// trackPropertyFilter is a TRACK_PROPERTY_FILTER admitting propType == v.
func trackPropertyFilter(propType uint64, v uint64) message.Parameter {
	return message.RangeFilterParam(&message.RangeFilter{
		Type: message.ParamTrackPropertyFilter, PropertyType: propType,
		Ranges: []message.Range{{Start: v, End: v}},
	})
}

// publishName is the track name of a forwarded PUBLISH.
func publishName(r *session.Request) string { return string(r.First.(*message.Publish).Name) }

// TestSubscribeTracksUpdate_RangeFilters: an updated TRACK_PROPERTY_FILTER
// forwards an existing track it newly matches, and applies to later PUBLISHes.
func TestSubscribeTracksUpdate_RangeFilters(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, subSess)
	stream := subscribeTracks(t, subSess, ns("video"), trackPropertyFilter(0x40, 5))

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrackProps(t, pubSess, "one", 7, trackProp(0x40, 1))
	requireNoForward(t, reqs, "filter 0x40=5, track 0x40=1")

	updateSubscribeTracks(t, subSess, stream, trackPropertyFilter(0x40, 1))
	if got := publishName(awaitForwarded(t, reqs)); got != "one" {
		t.Fatalf("forwarded %q after the update, want the existing track \"one\"", got)
	}
	publishVideoTrackProps(t, pubSess, "two", 8, trackProp(0x40, 1))
	if got := publishName(awaitForwarded(t, reqs)); got != "two" {
		t.Fatalf("forwarded %q, want the new track \"two\"", got)
	}
	publishVideoTrackProps(t, pubSess, "three", 9, trackProp(0x40, 5))
	requireNoForward(t, reqs, "updated filter 0x40=1, new track 0x40=5")
}

// TestSubscribeTracksUpdate_ForwardAppliesToFuturePublishes: FORWARD=0 in the
// update is carried on PUBLISHes sent afterwards; the existing forwarded
// subscription keeps receiving Objects.
func TestSubscribeTracksUpdate_ForwardAppliesToFuturePublishes(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, subSess)
	stream := subscribeTracks(t, subSess, ns("video"))

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrack(t, pubSess, "one", 7)
	first := awaitForwarded(t, reqs)
	acceptForwarded(t, first)

	updateSubscribeTracks(t, subSess, stream, message.ForwardParam(false))
	publishVideoTrack(t, pubSess, "two", 8)
	second := awaitForwarded(t, reqs)
	if p, ok := second.First.(*message.Publish).Parameters.Find(message.ParamForward); !ok || p.Byte != 0 {
		t.Fatalf("PUBLISH after the FORWARD=0 update carries FORWARD %v (present %v), want 0", p.Byte, ok)
	}

	go sendObjects(pubSess, 7, 1, 1)
	awaitObjectOn(t, subSess, first.First.(*message.Publish).TrackAlias)
}

// TestSubscribeTracksUpdate_PrefixForwardsExistingTracks: after a
// TRACK_NAMESPACE_PREFIX update, the tracks that already exist under the new
// prefix are forwarded, not only later ones.
func TestSubscribeTracksUpdate_PrefixForwardsExistingTracks(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, subSess)
	stream := subscribeTracks(t, subSess, ns("audio"))

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrack(t, pubSess, "cam", 7)
	requireNoForward(t, reqs, "prefix audio, track video/cam")

	updateSubscribeTracks(t, subSess, stream, message.TrackNamespacePrefixParam(ns("video")))
	if got := publishName(awaitForwarded(t, reqs)); got != "cam" {
		t.Fatalf("forwarded %q after the prefix update, want the existing video/cam", got)
	}
}

// TestSubscribeTracksUpdate_ExistingSubscriptionsUnaffected: an update whose
// filters no longer match a forwarded track does not end its subscription,
// and an update does not re-forward a track the subscriber refused.
func TestSubscribeTracksUpdate_ExistingSubscriptionsUnaffected(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, subSess)
	stream := subscribeTracks(t, subSess, ns("video"))

	pubSess := dialAnotherClient(t, subSess)
	publishVideoTrackProps(t, pubSess, "kept", 7, trackProp(0x40, 1))
	kept := awaitForwarded(t, reqs)
	acceptForwarded(t, kept)
	publishVideoTrackProps(t, pubSess, "refused", 8, trackProp(0x40, 1))
	refused := awaitForwarded(t, reqs)
	_ = message.Marshal(refused.Stream, &message.RequestError{ErrorCode: moqt.RequestUninterested})
	_ = refused.Stream.Close()
	time.Sleep(100 * time.Millisecond)

	// Matching does not change, so nothing newly matches: the refused
	// track is not offered again.
	updateSubscribeTracks(t, subSess, stream, message.ForwardParam(true))
	requireNoForward(t, reqs, "an update that changes no match")
	// The kept track stops matching; its subscription carries on.
	updateSubscribeTracks(t, subSess, stream, trackPropertyFilter(0x40, 9))

	go sendObjects(pubSess, 7, 1, 1)
	awaitObjectOn(t, subSess, kept.First.(*message.Publish).TrackAlias)
}

// TestSubscribeTracks_ForwardsTrackGainedBySubscribe: a track the relay gains
// by SUBSCRIBEing to a namespace publisher is forwarded (§10.20), except to the
// subscriber whose SUBSCRIBE caused it.
func TestSubscribeTracks_ForwardsTrackGainedBySubscribe(t *testing.T) {
	t.Parallel()
	holder, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, holder)
	subscribeTracks(t, holder, ns("video"))

	pubSess := dialAnotherClient(t, holder)
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns("video")}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
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
	requireNoForward(t, reqs, "a namespace with no track yet")

	subSess := dialAnotherClient(t, holder)
	subReqs := forwardedPublishes(t, subSess)
	subscribeTracks(t, subSess, ns("video"))
	subscribeCam1(t, subSess)

	if got := publishName(awaitForwarded(t, reqs)); got != "cam1" {
		t.Fatalf("forwarded %q, want video/cam1", got)
	}
	requireNoForward(t, subReqs, "the subscriber whose SUBSCRIBE created the track")
}

// TestSubscribeTracksUpdate_RefusalEndsRequest: a refused REQUEST_UPDATE ends
// the SUBSCRIBE_TRACKS (§10.9.1, §3.3.2), freeing its prefix for a new one.
func TestSubscribeTracksUpdate_RefusalEndsRequest(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	stream := subscribeTracks(t, subSess, ns("video"))

	// An odd Property Type in a TRACK_PROPERTY_FILTER is INVALID_FILTER.
	_, err := subSess.UpdateRequest(t.Context(), stream, message.Parameters{
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamTrackPropertyFilter, PropertyType: 0x41, Ranges: []message.Range{{Start: 1, End: 1}},
		}),
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidFilter)

	deadline := time.Now().Add(2 * time.Second)
	for {
		ts, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns("video")})
		if err == nil {
			_ = ts.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf(
				"SUBSCRIBE_TRACKS video after the refused update: %v; the ended request still holds its prefix",
				err,
			)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestSubscribeTracks_FillParametersOpenFillPerTrack: FILL_PARAMETERS on
// SUBSCRIBE_TRACKS (§10.20.1) opens a fill fetch stream per forwarded PUBLISH,
// carrying that PUBLISH's Request ID (§5.1.3).
func TestSubscribeTracks_FillParametersOpenFillPerTrack(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam", 7)
	sendObjects(pubSess, 7, 0, 1)
	sendObjects(pubSess, 7, 1, 1)
	time.Sleep(50 * time.Millisecond) // the relay caches both Groups

	holder := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, holder)
	subscribeTracks(t, holder, ns("video"),
		message.NextObjectFilter(),
		message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}))
	fwd := awaitForwarded(t, reqs)
	acceptForwarded(t, fwd)

	ds, ok := tryAcceptDataStream(t, holder, 2*time.Second)
	if !ok {
		t.Fatal("no fill fetch stream for the forwarded PUBLISH's subscription")
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want the fill fetch stream", ds)
	}
	if want := fwd.First.(*message.Publish).RequestID; fs.Header.RequestID != want {
		t.Fatalf("fill FETCH_HEADER Request ID %d, want the PUBLISH's %d", fs.Header.RequestID, want)
	}
	if objs := decodeFetchStream(t, fs, message.GroupOrderAscending); len(objs) != 2 {
		t.Fatalf("fill delivered %d Objects, want both cached Groups: %+v", len(objs), objs)
	}
}

// TestSubscribeTracks_NewGroupRequestPropagated: a NEW_GROUP_REQUEST on
// SUBSCRIBE_TRACKS reaches the upstream of a dynamic-Groups track (§10.2.19).
func TestSubscribeTracks_NewGroupRequestPropagated(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrackProps(t, pubSess, "cam", 7, trackProp(message.PropertyDynamicGroups, 1))
	gotNewGroup := watchUpstreamNewGroup(t, pub.Stream)

	holder := dialAnotherClient(t, pubSess)
	reqs := forwardedPublishes(t, holder)
	subscribeTracks(t, holder, ns("video"), message.NewGroupRequestParam(5))
	acceptForwarded(t, awaitForwarded(t, reqs))

	select {
	case v := <-gotNewGroup:
		if v != 5 {
			t.Fatalf("upstream NEW_GROUP_REQUEST = %d, want 5", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay did not send the SUBSCRIBE_TRACKS's NEW_GROUP_REQUEST upstream")
	}
}

// INCLUDE_PROPERTIES=0 (§10.2.21) empties the Track Properties of the OK or
// forwarded PUBLISH. The subscriber then reads DEFAULT_PUBLISHER_PRIORITY as
// 128 (§12.4), so the relay writes the priority inline on what it forwards.

const trackDefaultPriority = 7

// priorityTrackProps sets DEFAULT_PUBLISHER_PRIORITY to trackDefaultPriority.
func priorityTrackProps() []wire.KVPair {
	return trackProp(message.PropertyDefaultPublisherPriority, trackDefaultPriority)
}

// noProps is INCLUDE_PROPERTIES=0.
func noProps() message.Parameter { return message.IncludePropertiesParam(false) }

func TestIncludeProperties_SubscribeOK(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, priorityTrackProps())
	subSess := dialAnotherClient(t, pubSess)
	sub := subscribeCam1(t, subSess, noProps())
	if len(sub.OK.TrackProperties) != 0 {
		t.Fatalf("SUBSCRIBE_OK Track Properties %x with INCLUDE_PROPERTIES=0, want empty", sub.OK.TrackProperties)
	}
	with := subscribeCam1(t, dialAnotherClient(t, pubSess))
	if len(with.OK.TrackProperties) == 0 {
		t.Fatal("SUBSCRIBE_OK lost its Track Properties without INCLUDE_PROPERTIES")
	}

	// The subgroup the publisher sends with the default priority reaches
	// the subscriber with the priority spelled out.
	go sendObjects(pubSess, alias, 1, 1)
	ds, ok := tryAcceptDataStream(t, subSess, 2*time.Second)
	if !ok {
		t.Fatal("no subgroup forwarded")
	}
	sg := ds.(*session.IncomingSubgroupStream)
	if !sg.Header.InlinePriority || sg.Header.PublisherPriority != trackDefaultPriority {
		t.Fatalf("forwarded header inline=%v priority=%d, want inline priority %d",
			sg.Header.InlinePriority, sg.Header.PublisherPriority, trackDefaultPriority)
	}
}

func TestIncludeProperties_Datagram(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, priorityTrackProps())
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess, noProps())
	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type: message.DatagramDefaultPriorityBit, TrackAlias: alias, GroupID: 1, ObjectPayload: []byte("x"),
	}); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	d, err := subSess.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceiveDatagram: %v", err)
	}
	if d.HasDefaultPriority() || d.PublisherPriority != trackDefaultPriority {
		t.Fatalf("forwarded datagram default=%v priority=%d, want explicit priority %d",
			d.HasDefaultPriority(), d.PublisherPriority, trackDefaultPriority)
	}
}

func TestIncludeProperties_FetchAndTrackStatus(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, priorityTrackProps())
	newCam1Subscriber(t, pubSess) // keeps the track's upstream alive
	publishObjects(t, pubSess, alias, 1, 1)
	time.Sleep(50 * time.Millisecond)
	c := dialAnotherClient(t, pubSess)

	ts, err := c.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{noProps()},
	})
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	if len(ts.OK.TrackProperties) != 0 {
		t.Fatalf("TRACK_STATUS_OK Track Properties %x with INCLUDE_PROPERTIES=0, want empty", ts.OK.TrackProperties)
	}

	fr, err := c.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{noProps(),
			fetchRangeFilter(message.Location{Group: 1}, message.Location{Group: 1})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	if len(fr.OK.TrackProperties) != 0 {
		t.Fatalf("FETCH_OK Track Properties %x with INCLUDE_PROPERTIES=0, want empty", fr.OK.TrackProperties)
	}
	go drainAll(t.Context(), c)
}

func TestIncludeProperties_SubscribeTracks(t *testing.T) {
	t.Parallel()
	holder, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, holder)
	subscribeTracks(t, holder, ns("video"), noProps())

	pubSess := dialAnotherClient(t, holder)
	publishVideoTrackProps(t, pubSess, "cam", 7, priorityTrackProps())
	fwd := awaitForwarded(t, reqs)
	if tp := fwd.First.(*message.Publish).TrackProperties; len(tp) != 0 {
		t.Fatalf("forwarded PUBLISH Track Properties %x with INCLUDE_PROPERTIES=0, want empty", tp)
	}
	acceptForwarded(t, fwd)

	go sendObjects(pubSess, 7, 1, 1)
	ds, ok := tryAcceptDataStream(t, holder, 2*time.Second)
	if !ok {
		t.Fatal("no subgroup forwarded")
	}
	if h := ds.(*session.IncomingSubgroupStream).Header; !h.InlinePriority ||
		h.PublisherPriority != trackDefaultPriority {
		t.Fatalf("forwarded header inline=%v priority=%d, want inline priority %d",
			h.InlinePriority, h.PublisherPriority, trackDefaultPriority)
	}
}

// TestIncludeProperties_TrackStatusStillAnswers: INCLUDE_PROPERTIES=0 on a
// known track with no Objects yet still gets TRACK_STATUS_OK.
func TestIncludeProperties_TrackStatusStillAnswers(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, priorityTrackProps())
	c := dialAnotherClient(t, pubSess)
	ts, err := c.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{noProps()},
	})
	if err != nil {
		t.Fatalf("TRACK_STATUS with INCLUDE_PROPERTIES=0 on a known track: %v", err)
	}
	if len(ts.OK.TrackProperties) != 0 {
		t.Fatalf("TRACK_STATUS_OK Track Properties %x, want empty", ts.OK.TrackProperties)
	}
}
