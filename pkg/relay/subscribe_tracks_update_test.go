package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A REQUEST_UPDATE on a SUBSCRIBE_TRACKS changes the parameters used for the
// PUBLISHes the relay sends from then on — §10.2.18 says so of FORWARD: "In
// the case of a REQUEST_UPDATE for SUBSCRIBE_TRACKS, it specifies the
// Forwarding State on future subscriptions that match the prefix. Existing
// subscriptions are unaffected." — and §10.9.2 of the prefix: "Updating the
// prefix of a SUBSCRIBE_TRACKS has no effect on existing subscriptions".
// Tracks that exist and newly match (§10.20: "request PUBLISH messages for all
// tracks within matching namespaces") are forwarded when the update applies.

// openSubscribeTracks sends SUBSCRIBE_TRACKS for prefix and returns its stream,
// for REQUEST_UPDATEs.
func openSubscribeTracks(
	t *testing.T,
	sess *session.Session,
	prefix wire.TrackNamespace,
	ps ...message.Parameter,
) session.Stream {
	t.Helper()
	ts, err := sess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: prefix, Parameters: ps})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	t.Cleanup(func() { _ = ts.Close() })
	return ts.Stream
}

func updateSubscribeTracks(t *testing.T, sess *session.Session, stream session.Stream, ps ...message.Parameter) {
	t.Helper()
	if _, err := sess.UpdateRequest(t.Context(), stream, ps); err != nil {
		t.Fatalf("REQUEST_UPDATE on SUBSCRIBE_TRACKS: %v", err)
	}
}

// publishWithProps PUBLISHes ns/name from sess with one Track Property.
func publishWithProps(
	t *testing.T,
	sess *session.Session,
	namespace wire.TrackNamespace,
	name string,
	alias, propType, propVal uint64,
) {
	t.Helper()
	p, err := sess.Publish(t.Context(), &message.Publish{
		Namespace: namespace, Name: []byte(name), TrackAlias: alias,
		TrackProperties: message.AppendTrackProperties([]wire.KVPair{{Type: propType, IntVal: propVal}}),
	})
	if err != nil {
		t.Fatalf("Publish %s: %v", name, err)
	}
	t.Cleanup(func() { _ = p.Close() })
}

func trackPropertyFilter(propType uint64, v uint64) message.Parameter {
	return message.RangeFilterParam(&message.RangeFilter{
		Type: message.ParamTrackPropertyFilter, PropertyType: propType,
		Ranges: []message.Range{{Start: v, End: v}},
	})
}

func publishName(r *session.Request) string { return string(r.First.(*message.Publish).Name) }

// TestSubscribeTracksUpdate_RangeFilters: an updated TRACK_PROPERTY_FILTER
// forwards an existing track it newly matches, and applies to later PUBLISHes.
func TestSubscribeTracksUpdate_RangeFilters(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	reqs := forwardedPublishes(t, subSess)
	stream := openSubscribeTracks(t, subSess, ns("video"), trackPropertyFilter(0x40, 5))

	pubSess := dialAnotherClient(t, subSess)
	publishWithProps(t, pubSess, ns("video"), "one", 7, 0x40, 1)
	requireNoForward(t, reqs, "filter 0x40=5, track 0x40=1")

	updateSubscribeTracks(t, subSess, stream, trackPropertyFilter(0x40, 1))
	if got := publishName(awaitForwarded(t, reqs)); got != "one" {
		t.Fatalf("forwarded %q after the update, want the existing track \"one\"", got)
	}
	publishWithProps(t, pubSess, ns("video"), "two", 8, 0x40, 1)
	if got := publishName(awaitForwarded(t, reqs)); got != "two" {
		t.Fatalf("forwarded %q, want the new track \"two\"", got)
	}
	publishWithProps(t, pubSess, ns("video"), "three", 9, 0x40, 5)
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
	stream := openSubscribeTracks(t, subSess, ns("video"))

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

	go sendObject(pubSess, 7, 1)
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
	stream := openSubscribeTracks(t, subSess, ns("audio"))

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
	stream := openSubscribeTracks(t, subSess, ns("video"))

	pubSess := dialAnotherClient(t, subSess)
	publishWithProps(t, pubSess, ns("video"), "kept", 7, 0x40, 1)
	kept := awaitForwarded(t, reqs)
	acceptForwarded(t, kept)
	publishWithProps(t, pubSess, ns("video"), "refused", 8, 0x40, 1)
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

	go sendObject(pubSess, 7, 1)
	awaitObjectOn(t, subSess, kept.First.(*message.Publish).TrackAlias)
}
