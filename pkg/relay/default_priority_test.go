package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A DEFAULT_PRIORITY subgroup or datagram inherits the Publisher Priority of
// the message that established the subscription (§11.4.2, §11.3.1): the
// DEFAULT_PUBLISHER_PRIORITY Track Property, or 128 when omitted (§12.4).
// Checked through the cache (FETCH spells the priority out) and the
// PRIORITY_FILTER forward decision.

// fetchedPriority FETCHes {group, 0} of video/cam1 from a new client of via's
// relay and returns its Publisher Priority.
func fetchedPriority(t *testing.T, via *session.Session, group uint64) uint8 {
	t.Helper()
	p, ok := tryFetchedPriority(t, dialAnotherClient(t, via), ns("video"), []byte("cam1"), group)
	if !ok {
		t.Fatalf("FETCH of {%d,0} was not served", group)
	}
	return p
}

// tryFetchedPriority FETCHes {group, 0} and returns the first element's
// Publisher Priority; ok is false while it is not yet servable.
func tryFetchedPriority(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	group uint64,
) (uint8, bool) {
	t.Helper()
	loc := message.Location{Group: group}
	req, err := sess.Fetch(t.Context(), &message.Fetch{
		Namespace:  ns,
		Name:       name,
		Parameters: message.Parameters{fetchRangeFilter(loc, loc)},
	})
	if err != nil {
		return 0, false
	}
	defer req.Close()
	ds, err := sess.AcceptDataStream(t.Context())
	if err != nil {
		return 0, false
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		return 0, false
	}
	obj, err := fs.ReadDecoded()
	if err != nil {
		return 0, false
	}
	return obj.PublisherPriority, true
}

// defaultPriorityProp sets DEFAULT_PUBLISHER_PRIORITY to v.
func defaultPriorityProp(v uint64) []wire.KVPair {
	return trackProp(message.PropertyDefaultPublisherPriority, v)
}

// TestDefaultPriority_Subgroup: a cached DEFAULT_PRIORITY subgroup Object is
// served with the inherited priority; an inline one keeps its own.
func TestDefaultPriority_Subgroup(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		props  []wire.KVPair
		inline int // < 0 leaves the DEFAULT_PRIORITY bit set
		want   uint8
	}{
		{"no property inherits 128", nil, -1, 128},
		{"inherits track property", defaultPriorityProp(200), -1, 200},
		{"inline overrides track property", defaultPriorityProp(200), 5, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, alias := newCam1Publisher(t, tc.props)
			subSess := newCam1Subscriber(t, pubSess)
			hdr := subgroupHeader(alias, 3)
			if tc.inline >= 0 {
				hdr.InlinePriority, hdr.PublisherPriority = true, uint8(tc.inline)
			}
			if err := writeSubgroup(pubSess, hdr, 1); err != nil {
				t.Fatal(err)
			}
			// The relay caches before it forwards.
			if !awaitSubgroupObject(t, subSess, 2*time.Second) {
				t.Fatal("relay did not forward the object")
			}
			if got := fetchedPriority(t, pubSess, 3); got != tc.want {
				t.Errorf("FETCH Publisher Priority = %d, want %d", got, tc.want)
			}
		})
	}
}

// TestDefaultPriority_Datagram: a cached DEFAULT_PRIORITY datagram is served
// with the track's DEFAULT_PUBLISHER_PRIORITY (§11.3.1).
func TestDefaultPriority_Datagram(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, defaultPriorityProp(200))
	subSess := newCam1Subscriber(t, pubSess)

	got := make(chan error, 1)
	go func() {
		_, err := subSess.ReceiveDatagram(t.Context())
		got <- err
	}()
	if err := pubSess.SendDatagram(&message.ObjectDatagram{
		Type:          message.DatagramDefaultPriorityBit,
		TrackAlias:    alias,
		GroupID:       3,
		ObjectPayload: []byte("x"),
	}); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}
	select {
	case err := <-got:
		if err != nil {
			t.Fatalf("ReceiveDatagram: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("relay did not forward the datagram")
	}

	if p := fetchedPriority(t, pubSess, 3); p != 200 {
		t.Errorf("FETCH Publisher Priority = %d, want 200", p)
	}
}

// TestDefaultPriority_PriorityFilter: PRIORITY_FILTER [100, 255] passes a
// subgroup inheriting 128.
func TestDefaultPriority_PriorityFilter(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := newCam1Subscriber(t, pubSess, message.RangeFilterParam(&message.RangeFilter{
		Type:   message.ParamPriorityFilter,
		Ranges: []message.Range{{Start: 100, End: 255}},
	}))
	publishObjects(t, pubSess, alias, 3, 1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("PRIORITY_FILTER [100,255] dropped a subgroup inheriting priority 128")
	}
}

// TestDefaultPriority_UpstreamSubscribeAliasWindow: a subgroup arriving on a
// relay-initiated SUBSCRIBE's alias before the relay recorded the
// SUBSCRIBE_OK's Track Properties still inherits its DEFAULT_PUBLISHER_PRIORITY.
// Not parallel: it installs the process-wide alias-window hook.
func TestDefaultPriority_UpstreamSubscribeAliasWindow(t *testing.T) {
	video := ns("video")
	name := []byte("cam-priority-alias-window")

	restore := relay.SetTestHookAfterAliasRegistered(func(n track.FullTrackName) {
		if string(n.Name) == string(name) {
			// Hold AddUpstream off while the publisher's subgroup lands.
			time.Sleep(50 * time.Millisecond)
		}
	})
	t.Cleanup(restore)

	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	go func() {
		for {
			req, err := upSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if _, ok := req.First.(*message.Subscribe); !ok {
				_ = req.RejectError(moqt.RequestNotSupported, "subscribe only")
				continue
			}
			const alias = uint64(42)
			if err := req.Reply(&message.SubscribeOK{
				TrackAlias:      alias,
				TrackProperties: message.AppendTrackProperties(defaultPriorityProp(200)),
			}); err != nil {
				return
			}
			sendObjects(upSess, alias, 3, 1)
		}
	}()

	live := dialAnotherClient(t, upSess)
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = liveReq.Close() })
	go drainAll(t.Context(), live)

	fetchSess := dialAnotherClient(t, upSess)
	var got uint8
	waitFor(t, 5*time.Second, func() bool {
		p, ok := tryFetchedPriority(t, fetchSess, video, name, 3)
		got = p
		return ok
	}, "relay never cached the subgroup published in the alias window")
	if got != 200 {
		t.Errorf("FETCH Publisher Priority = %d, want 200 (the SUBSCRIBE_OK's DEFAULT_PUBLISHER_PRIORITY)", got)
	}
}

// TestDefaultPriority_PerPublisher: with two publishers of one track, each
// subgroup inherits from the PUBLISH that bound its own alias (§11.4.2).
func TestDefaultPriority_PerPublisher(t *testing.T) {
	t.Parallel()
	pubA, aliasA := newCam1Publisher(t, defaultPriorityProp(200))
	subSess := newCam1Subscriber(t, pubA)

	pubB := dialAnotherClient(t, pubA)
	const aliasB = uint64(9)
	publishVideoTrackProps(t, pubB, "cam1", aliasB, defaultPriorityProp(10))

	publishObjects(t, pubA, aliasA, 3, 1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("relay did not forward publisher A's object")
	}
	publishObjects(t, pubB, aliasB, 4, 1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("relay did not forward publisher B's object")
	}

	if got := fetchedPriority(t, pubA, 3); got != 200 {
		t.Errorf("publisher A object: FETCH Publisher Priority = %d, want 200", got)
	}
	if got := fetchedPriority(t, pubA, 4); got != 10 {
		t.Errorf("publisher B object: FETCH Publisher Priority = %d, want 10", got)
	}
}
