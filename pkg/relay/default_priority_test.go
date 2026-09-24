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

// A subgroup or datagram with the DEFAULT_PRIORITY bit set carries no Priority
// byte; it "inherits the Publisher Priority specified in the control message
// that established the subscription" (§11.4.2, §11.3.1) — the
// DEFAULT_PUBLISHER_PRIORITY Track Property, or 128 when that is omitted
// (§12.4). These tests pin that the relay resolves the inherited value rather
// than treating the absent byte as priority 0, on every path that reads it: the
// cache (observed through FETCH, which must spell the priority out) and the
// PRIORITY_FILTER forward decision.

// publishWithTrackProps PUBLISHes video/cam1 with the given Track Properties
// and returns the publisher session and its Track Alias.
func publishWithTrackProps(t *testing.T, props []wire.KVPair) (*session.Session, uint64) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	const alias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackAlias:      alias,
		TrackProperties: message.AppendTrackProperties(props),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { pubReq.Close() })
	return pubSess, alias
}

// subscribeCam1 subscribes a fresh client to video/cam1 with extra parameters.
func subscribeCam1(t *testing.T, pubSess *session.Session, params ...message.Parameter) *session.Session {
	t.Helper()
	subSess := dialAnotherClient(t, pubSess)
	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		Parameters: params,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subReq.Close() })
	return subSess
}

// awaitSubgroupObject waits for the relay to forward one subgroup object to
// subSess, reporting whether it arrived before the deadline.
func awaitSubgroupObject(t *testing.T, subSess *session.Session, within time.Duration) bool {
	t.Helper()
	got := make(chan bool, 1)
	go func() {
		ds, err := subSess.AcceptDataStream(t.Context())
		if err != nil {
			got <- false
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			got <- false
			return
		}
		_, err = sg.ReadObject()
		got <- err == nil
	}()
	select {
	case ok := <-got:
		return ok
	case <-time.After(within):
		return false
	}
}

// publishSubgroupObject writes a one-object subgroup at {group, 0}. inline < 0
// leaves the DEFAULT_PRIORITY bit set; otherwise the header carries inline.
func publishSubgroupObject(t *testing.T, pubSess *session.Session, alias, group uint64, inline int) {
	t.Helper()
	hdr := message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit,
		TrackAlias:     alias,
		GroupID:        group,
	}
	if inline >= 0 {
		hdr.InlinePriority = true
		hdr.PublisherPriority = uint8(inline)
	}
	sg, err := pubSess.OpenSubgroup(hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("sg.Close: %v", err)
	}
}

// fetchedPriority FETCHes the single object at {group, 0} from the relay's
// cache and returns the Publisher Priority the FETCH response carries.
func fetchedPriority(t *testing.T, pubSess *session.Session, group uint64) uint8 {
	t.Helper()
	fetchSess := dialAnotherClient(t, pubSess)
	loc := message.Location{Group: group}
	reqStream, err := fetchSess.Fetch(t.Context(), &message.Fetch{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		Parameters: message.Parameters{fetchRangeFilter(loc, loc)},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	t.Cleanup(func() { reqStream.Close() })
	ds, err := fetchSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	obj, err := fs.ReadDecoded()
	if err != nil {
		t.Fatalf("ReadDecoded: %v", err)
	}
	return obj.PublisherPriority
}

func defaultPriorityProp(v uint64) []wire.KVPair {
	return []wire.KVPair{{Type: message.PropertyDefaultPublisherPriority, IntVal: v}}
}

func TestDefaultPriority_Subgroup(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		props  []wire.KVPair
		inline int
		want   uint8
	}{
		{"no property inherits 128", nil, -1, 128},
		{"inherits track property", defaultPriorityProp(200), -1, 200},
		{"inline overrides track property", defaultPriorityProp(200), 5, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, alias := publishWithTrackProps(t, tc.props)
			subSess := subscribeCam1(t, pubSess)
			publishSubgroupObject(t, pubSess, alias, 3, tc.inline)
			// The forwarded copy is the sync point: the relay caches
			// before it forwards.
			if !awaitSubgroupObject(t, subSess, 2*time.Second) {
				t.Fatal("relay did not forward the object")
			}
			if got := fetchedPriority(t, pubSess, 3); got != tc.want {
				t.Errorf("FETCH Publisher Priority = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestDefaultPriority_Datagram(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, defaultPriorityProp(200))
	subSess := subscribeCam1(t, pubSess)

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

// TestDefaultPriority_PriorityFilter covers the live forward path: a
// PRIORITY_FILTER admitting [100, 255] must pass a default-priority subgroup
// (inherited 128), which a priority-0 reading would drop.
func TestDefaultPriority_PriorityFilter(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	subSess := subscribeCam1(t, pubSess, message.RangeFilterParam(&message.RangeFilter{
		Type:   message.ParamPriorityFilter,
		Ranges: []message.Range{{Start: 100, End: 255}},
	}))
	publishSubgroupObject(t, pubSess, alias, 3, -1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("PRIORITY_FILTER [100,255] dropped a subgroup inheriting priority 128")
	}
}

// TestDefaultPriority_UpstreamSubscribeAliasWindow covers the relay-initiated
// SUBSCRIBE path. The SUBSCRIBE_OK's Track Alias resolves on inbound data
// streams as soon as session.Subscribe returns, before the relay has recorded
// that SUBSCRIBE_OK's Track Properties on its track entry (see
// TestSubscribeUpstream_TrackEntryPrecedesAliasRouting). A default-priority
// subgroup arriving in that window must still inherit the SUBSCRIBE_OK's
// DEFAULT_PUBLISHER_PRIORITY, not the omitted-property 128. Not parallel: it
// installs the process-wide alias-window hook.
func TestDefaultPriority_UpstreamSubscribeAliasWindow(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
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
	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
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
			// Not publishSubgroupObject: t.Fatal is off-limits here.
			sg, err := upSess.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDExplicit,
				TrackAlias:     alias,
				GroupID:        3,
			})
			if err != nil {
				return
			}
			_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
			_ = sg.Close()
		}
	}()

	live := dialAnotherClient(t, upSess)
	liveReq, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	if err != nil {
		t.Fatalf("live Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = liveReq.Close() })
	go drainAll(t.Context(), live)

	fetchSess := dialAnotherClient(t, upSess)
	var got uint8
	waitFor(t, 5*time.Second, func() bool {
		p, ok := tryFetchedPriority(t, fetchSess, ns, name, 3)
		got = p
		return ok
	}, "relay never cached the subgroup published in the alias window")
	if got != 200 {
		t.Errorf("FETCH Publisher Priority = %d, want 200 (the SUBSCRIBE_OK's DEFAULT_PUBLISHER_PRIORITY)", got)
	}
}

// tryFetchedPriority is a non-fatal [fetchedPriority] for polling: ok is false
// while the object at {group, 0} is not yet servable.
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

// TestDefaultPriority_PerPublisher: two publishers of one track, each with its
// own DEFAULT_PUBLISHER_PRIORITY. Each DEFAULT_PRIORITY subgroup inherits from
// the PUBLISH that bound its own alias (§11.4.2), not from whichever publisher
// happened to set the relay's shared track Properties first.
func TestDefaultPriority_PerPublisher(t *testing.T) {
	t.Parallel()
	pubA, aliasA := publishWithTrackProps(t, defaultPriorityProp(200))
	subSess := subscribeCam1(t, pubA)

	pubB := dialAnotherClient(t, pubA)
	const aliasB = uint64(9)
	pubReqB, err := pubB.Publish(t.Context(), &message.Publish{
		Namespace:       wire.TrackNamespace{[]byte("video")},
		Name:            []byte("cam1"),
		TrackAlias:      aliasB,
		TrackProperties: message.AppendTrackProperties(defaultPriorityProp(10)),
	})
	if err != nil {
		t.Fatalf("Publish B: %v", err)
	}
	t.Cleanup(func() { pubReqB.Close() })

	publishSubgroupObject(t, pubA, aliasA, 3, -1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("relay did not forward publisher A's object")
	}
	publishSubgroupObject(t, pubB, aliasB, 4, -1)
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
