package relay_test

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Malformed tracks (§2.4.2): the relay ends downstream subscriptions with
// PUBLISH_DONE MALFORMED_TRACK, resets fetch streams, and cancels its own
// subscription upstream. An unknown Mandatory Track Property (§2.5.1) refuses
// the track with REQUEST_ERROR UNSUPPORTED_EXTENSION, or resets a fetch stream
// the relay already answered.

// mandatoryProps carries the unknown Mandatory Track Property 0x4000. As Object
// Properties it makes the track malformed (§2.5.1).
func mandatoryProps() []byte {
	return message.AppendTrackProperties(trackProp(message.MandatoryTrackPropertyMin, 1))
}

// malformedProps does not parse as Properties: a varint type with nothing
// after it.
var malformedProps = []byte{0x01}

// requireUpstreamCancelled waits for the relay to cancel the publisher's
// PUBLISH: a read on its request stream fails rather than blocking.
func requireUpstreamCancelled(t *testing.T, pub *session.Publication) {
	t.Helper()
	got := make(chan error, 1)
	go func() {
		_, err := message.Parse(pub.Stream)
		got <- err
	}()
	select {
	case err := <-got:
		if err == nil {
			t.Fatal("the relay sent a message on the PUBLISH stream; want it cancelled")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the relay kept its subscription to the publisher of a malformed track")
	}
}

// gapObject is an Object for [sendGapObjects].
type gapObject struct {
	group, subgroup, object uint64
	props                   []byte
}

// sendGapObjects sends each of objs on its own subgroup stream. The relay may
// read the streams in either order; each case using it is malformed in both.
func sendGapObjects(t *testing.T, pubSess *session.Session, alias uint64, objs []gapObject) {
	t.Helper()
	for _, o := range objs {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias,
			GroupID: o.group, SubgroupID: o.subgroup, Properties: true,
		})
		if err != nil {
			t.Errorf("OpenSubgroup: %v", err)
			return
		}
		if err := sg.WriteObjectAt(
			o.object,
			&message.SubgroupObject{Properties: o.props, Payload: []byte("x")},
		); err != nil {
			t.Errorf("WriteObjectAt: %v", err)
		}
		_ = sg.Close()
	}
}

// testStream is a subgroup stream for [sendStreams]: its Objects, each with a
// Status, and whether it ends with a FIN or stays open.
type testStream struct {
	group, subgroup uint64
	priority        uint8 // inline when set
	endOfGroup      bool  // the header's END_OF_GROUP bit
	objects         []uint64
	status          uint64 // of the last Object
	open            bool
}

// sendStreams sends each of streams from pubSess on alias. The relay may read
// them in either order; each case using it is malformed in both.
func sendStreams(t *testing.T, pubSess *session.Session, alias uint64, streams []testStream) {
	t.Helper()
	for _, s := range streams {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: s.group,
			SubgroupID: s.subgroup, InlinePriority: s.priority != 0, PublisherPriority: s.priority,
			EndOfGroup: s.endOfGroup,
		})
		if err != nil {
			t.Errorf("OpenSubgroup: %v", err)
			return
		}
		for i, id := range s.objects {
			o := &message.SubgroupObject{Payload: []byte("x")}
			if i == len(s.objects)-1 && s.status != 0 {
				o = &message.SubgroupObject{ObjectStatus: s.status}
			}
			_ = sg.WriteObjectAt(id, o)
		}
		if s.open {
			t.Cleanup(func() { _ = sg.Close() })
			continue
		}
		_ = sg.Close()
	}
}

// priorGap is Object Properties carrying a Prior Group or Object ID Gap.
func priorGap(typ message.PropertyType, n uint64) []byte {
	return message.AppendTrackProperties([]wire.KVPair{{Type: typ, IntVal: n}})
}

// TestRelay_MalformedObjectEndsTrack: each kind of malformed Object ends the
// downstream subscription with MALFORMED_TRACK and cancels the upstream one
// (§2.4.2).
func TestRelay_MalformedObjectEndsTrack(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		send func(t *testing.T, pubSess *session.Session, alias uint64)
	}{
		{"Object Properties", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 1, Properties: true,
			})
			if err != nil {
				t.Errorf("OpenSubgroup: %v", err)
				return
			}
			_ = sg.WriteObject(&message.SubgroupObject{Properties: mandatoryProps(), Payload: []byte("x")})
			_ = sg.Close()
		}},
		// §2.4.2 condition 4: an Object after the final Object in the Group.
		{"Object after END_OF_GROUP", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
				SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 1,
			})
			if err != nil {
				t.Errorf("OpenSubgroup: %v", err)
				return
			}
			_ = sg.WriteObject(&message.SubgroupObject{ObjectStatus: message.ObjectStatusEndOfGroup})
			_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
			_ = sg.Close()
		}},
		// §12.8: two Prior Group ID Gap values in one Group.
		{"different group gaps in a Group", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sendGapObjects(t, pubSess, alias, []gapObject{
				{5, 0, 0, priorGap(message.PropertyPriorGroupIDGap, 2)},
				{5, 1, 1, priorGap(message.PropertyPriorGroupIDGap, 1)},
			})
		}},
		// §2.4.2 item 1: a Subgroup's Publisher Priority changes.
		{"priority changes in a Subgroup", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sendStreams(t, pubSess, alias, []testStream{
				{group: 1, priority: 1, objects: []uint64{0}, open: true},
				{group: 1, priority: 2, objects: []uint64{1}, open: true},
			})
		}},
		// §2.4.2 item 2: an Object past the last one before a Subgroup's FIN.
		{"Object past a Subgroup's FIN", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sendStreams(t, pubSess, alias, []testStream{
				{group: 1, objects: []uint64{0, 1}},
				{group: 1, objects: []uint64{2}, open: true},
			})
		}},
		// §2.4.2 item 4: an Object past an END_OF_GROUP.
		{"Object past END_OF_GROUP", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sendStreams(t, pubSess, alias, []testStream{
				{group: 1, objects: []uint64{2}, status: message.ObjectStatusEndOfGroup},
				{group: 1, subgroup: 1, objects: []uint64{3}, open: true},
			})
		}},
		{"Object past an END_OF_GROUP Subgroup's FIN", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sendStreams(t, pubSess, alias, []testStream{
				{group: 1, endOfGroup: true, objects: []uint64{0, 1}},
				{group: 1, subgroup: 1, objects: []uint64{2}, open: true},
			})
		}},
		{"datagram past a datagram's END_OF_GROUP", func(t *testing.T, pubSess *session.Session, alias uint64) {
			for _, d := range []*message.ObjectDatagram{
				{Type: message.DatagramEndOfGroupBit, TrackAlias: alias, GroupID: 1, ObjectID: 2, ObjectPayload: []byte("x")},
				{TrackAlias: alias, GroupID: 1, ObjectID: 3, ObjectPayload: []byte("x")},
			} {
				if err := pubSess.SendDatagram(d); err != nil {
					t.Errorf("SendDatagram: %v", err)
				}
			}
		}},
		// §2.4.2 item 5: an Object past the END_OF_TRACK.
		{"Object past END_OF_TRACK", func(t *testing.T, pubSess *session.Session, alias uint64) {
			sendStreams(t, pubSess, alias, []testStream{
				{group: 1, objects: []uint64{2}, status: message.ObjectStatusEndOfTrack},
				{group: 2, objects: []uint64{0}, open: true},
			})
		}},
		{"datagrams with different group gaps in a Group", func(t *testing.T, pubSess *session.Session, alias uint64) {
			for i, gap := range []uint64{2, 1} {
				if err := pubSess.SendDatagram(&message.ObjectDatagram{
					Type: message.DatagramPropertiesBit, TrackAlias: alias, GroupID: 5, ObjectID: uint64(i),
					Properties: priorGap(message.PropertyPriorGroupIDGap, gap), ObjectPayload: []byte("x"),
				}); err != nil {
					t.Errorf("SendDatagram: %v", err)
				}
			}
		}},
		{"datagram Object Properties", func(t *testing.T, pubSess *session.Session, alias uint64) {
			if err := pubSess.SendDatagram(&message.ObjectDatagram{
				Type: message.DatagramPropertiesBit, TrackAlias: alias, GroupID: 1,
				Properties: mandatoryProps(), ObjectPayload: []byte("x"),
			}); err != nil {
				t.Errorf("SendDatagram: %v", err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			pub := publishVideoTrack(t, pubSess, "cam1", 7)
			subSess := dialAnotherClient(t, pubSess)
			subReq := subscribeCam1(t, subSess)
			go drainAll(t.Context(), subSess)

			go tc.send(t, pubSess, 7)
			if pd := awaitPublishDone(t, subReq); pd.StatusCode != moqt.PublishDoneMalformedTrack {
				t.Fatalf("downstream PUBLISH_DONE %#x, want MALFORMED_TRACK %#x",
					uint64(pd.StatusCode), uint64(moqt.PublishDoneMalformedTrack))
			}
			requireUpstreamCancelled(t, pub)
		})
	}
}

// TestRelay_MalformedObjectEndsTrackWithStreamsOpen: PUBLISH_DONE is not held
// back by another idle outbound stream (§10.12); the relay resets it.
func TestRelay_MalformedObjectEndsTrackWithStreamsOpen(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 7)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1(t, subSess)

	idle, err := pubSess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: 7, GroupID: 1,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	defer idle.Close()
	if err := idle.WriteObject(&message.SubgroupObject{Payload: []byte("ok")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	// The subscriber reads the idle stream's Object, so its outbound
	// stream is open, and then leaves it; later streams are drained.
	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	if _, err := ds.(*session.IncomingSubgroupStream).ReadObject(); err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	go drainAll(t.Context(), subSess)

	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: 7, GroupID: 2, Properties: true,
		})
		if err != nil {
			return
		}
		_ = sg.WriteObject(&message.SubgroupObject{Properties: mandatoryProps(), Payload: []byte("x")})
		_ = sg.Close()
	}()
	if pd := awaitPublishDone(t, subReq); pd.StatusCode != moqt.PublishDoneMalformedTrack {
		t.Fatalf("downstream PUBLISH_DONE %#x, want MALFORMED_TRACK", uint64(pd.StatusCode))
	}
	requireUpstreamCancelled(t, pub)
}

// TestRelay_PublishTrackPropertiesRejected: a PUBLISH with an unknown Mandatory
// Track Property is UNSUPPORTED_EXTENSION (§2.5.1); one whose Track Properties
// do not parse is INTERNAL_ERROR (MALFORMED_TRACK answers only a FETCH, §10.6.2).
func TestRelay_PublishTrackPropertiesRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		want  moqt.RequestErrorCode
	}{
		{"unknown mandatory", mandatoryProps(), moqt.RequestUnsupportedExtension},
		{"malformed", malformedProps, moqt.RequestInternalError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			_, err := sess.Publish(t.Context(), &message.Publish{
				Namespace:       ns("video"),
				Name:            []byte("cam1"),
				TrackProperties: tc.props,
			})
			requireRejectedWithCode(t, err, tc.want)
		})
	}
}

// TestRelay_PublishKnownMandatoryPropertyAccepted: a Mandatory Track Property
// the relay is configured to know is accepted.
func TestRelay_PublishKnownMandatoryPropertyAccepted(t *testing.T) {
	t.Parallel()
	sess, teardown := connectRelay(t, relay.Config{
		KnownMandatoryTrackProperties: []message.PropertyType{message.MandatoryTrackPropertyMin},
	})
	defer teardown()
	pub, err := sess.Publish(t.Context(), &message.Publish{
		Namespace:       ns("video"),
		Name:            []byte("cam1"),
		TrackProperties: mandatoryProps(),
	})
	if err != nil {
		t.Fatalf("Publish with a configured mandatory property: %v", err)
	}
	_ = pub.Close()
}

// TestRelay_UpstreamSubscribeOKTrackPropertiesRejected: the relay refuses the
// downstream SUBSCRIBE when the upstream SUBSCRIBE_OK carries an unknown
// Mandatory Track Property (§2.5.1) or malformed Track Properties.
func TestRelay_UpstreamSubscribeOKTrackPropertiesRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		want  moqt.RequestErrorCode
	}{
		{"unknown mandatory", mandatoryProps(), moqt.RequestUnsupportedExtension},
		{"malformed", malformedProps, moqt.RequestInternalError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			video := ns("video")
			upSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
				t.Fatalf("PublishNamespace: %v", err)
			}
			go func() {
				r, err := upSess.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackProperties: tc.props})
			}()
			subSess := dialAnotherClient(t, upSess)
			_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
			requireRejectedWithCode(t, err, tc.want)
		})
	}
}

// TestRelay_UpstreamMandatoryPropertyWinsOverOtherFailure: with one publisher
// answering an unknown Mandatory Track Property and another refusing, the
// subscriber gets UNSUPPORTED_EXTENSION in either order (§2.5.1).
func TestRelay_UpstreamMandatoryPropertyWinsOverOtherFailure(t *testing.T) {
	t.Parallel()
	for _, mandatoryFirst := range []bool{true, false} {
		t.Run(fmt.Sprintf("mandatoryFirst=%v", mandatoryFirst), func(t *testing.T) {
			t.Parallel()
			video := ns("video")
			first, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			second := dialAnotherClient(t, first)
			serve := func(sess *session.Session, mandatory bool) {
				if _, err := sess.PublishNamespace(
					t.Context(),
					&message.PublishNamespace{Namespace: video},
				); err != nil {
					t.Fatalf("PublishNamespace: %v", err)
				}
				go func() {
					r, err := sess.AcceptRequest(t.Context())
					if err != nil {
						return
					}
					if mandatory {
						_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackProperties: mandatoryProps()})
						return
					}
					_ = r.RejectError(moqt.RequestDoesNotExist, "not here")
				}()
			}
			serve(first, mandatoryFirst)
			serve(second, !mandatoryFirst)
			subSess := dialAnotherClient(t, first)
			_, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
			requireRejectedWithCode(t, err, moqt.RequestUnsupportedExtension)
		})
	}
}

// TestRelay_UpstreamFetchOKUnknownMandatoryPropertyResetsStream: an upstream
// FETCH_OK with an unknown Mandatory Track Property (§2.5.1), or Track
// Properties that do not parse, resets the downstream fetch stream the relay
// already answered; no Object reaches the subscriber, not even cached ones.
// The request stream is reset with the data stream's code (§3.3.3):
// INTERNAL_ERROR, or MALFORMED_TRACK for Properties that do not parse, which
// make the track malformed (§12.7, §2.4.2).
func TestRelay_UpstreamFetchOKUnknownMandatoryPropertyResetsStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		code  moqt.StreamResetCode
	}{
		{"unknown Mandatory Track Property", mandatoryProps(), moqt.StreamResetInternalError},
		{"Track Properties that do not parse", malformedProps, moqt.StreamResetMalformedTrack},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refusedFetchResetsStream(t, tc.props, nil, tc.code, fetchStreamReset, requestStreamReset)
		})
	}
}

// TestRelay_UpstreamFetchMalformedObjectResetsStream: an upstream FETCH
// response Object with a Mandatory Track Property as an Object Property makes
// the track malformed (§2.5.1, §2.4.2): the downstream FETCH's data stream and
// request stream are reset with MALFORMED_TRACK (§3.3.3).
func TestRelay_UpstreamFetchMalformedObjectResetsStream(t *testing.T) {
	t.Parallel()
	refusedFetchResetsStream(t, nil, mandatoryProps(),
		moqt.StreamResetMalformedTrack, fetchStreamReset, requestStreamReset)
}

// refusedFetchResetsStream requires a stitched FETCH to be reset when the
// upstream answers it with FETCH_OK carrying upstreamProps and, if objProps is
// non-nil, one Object carrying objProps: the streams of kinds, each with code.
// The upstream misbehaves only once armed, so the FETCHes waiting for the live
// tail to be cached succeed.
func refusedFetchResetsStream(
	t *testing.T,
	upstreamProps, objProps []byte,
	code moqt.StreamResetCode,
	kinds ...resetStream,
) {
	var armed atomic.Bool
	l := newPipeListener()
	resets := make(chan streamReset, 64)
	l.resetsFor = resetsOn(3, resets) // the upstream is 1, the live subscriber 2
	pubSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()
	video := ns("video")
	name := []byte("cam1")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	written := make(chan struct{})
	tailWritten := sync.OnceFunc(func() { close(written) })
	go func() {
		for {
			req, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch first := req.First.(type) {
			case *message.Subscribe:
				if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
					return
				}
				for g := stitchLiveLo; g <= stitchLiveHi; g++ {
					sg, err := openSubgroupWaiting(t, pubSess, message.SubgroupHeader{
						SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: 42, GroupID: g, EndOfGroup: true,
					})
					if err != nil {
						return
					}
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}})
					_ = sg.Close()
				}
				tailWritten()
			case *message.Fetch:
				var props []byte
				if armed.Load() {
					props = upstreamProps
				}
				_ = req.Reply(&message.FetchOK{
					EndLocation:     message.Location{Group: stitchLiveLo - 1},
					TrackProperties: props,
				})
				if objProps == nil || !armed.Load() {
					continue
				}
				out, err := pubSess.OpenFetchStream(message.FetchHeader{RequestID: first.RequestID})
				if err != nil {
					return
				}
				_ = out.WriteObject(&message.FetchObject{
					SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
						message.FetchFlagPriority | message.FetchFlagProperties,
					Properties: objProps, ObjectPayload: []byte("x"),
				})
				_ = out.Close()
			}
		}
	}()

	live := dialAnotherClient(t, pubSess)
	if _, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go drainAll(t.Context(), live)
	awaitTailCached(t, written)
	fc := dialAnotherClient(t, pubSess)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if objs, served := fetchRange(t, fc, video, name,
			message.Location{Group: stitchLiveLo}, message.Location{Group: stitchLiveHi}); served && len(objs) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the relay never cached the live tail")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Reaches below the cache, so the relay stitches from the upstream.
	armed.Store(true)
	fr, err := fc.Fetch(t.Context(), &message.Fetch{
		Namespace: video, Name: name,
		Parameters: message.Parameters{fetchRangeFilter(message.Location{}, message.Location{Group: stitchLiveHi})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	ds, err := fc.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a FETCH stream", ds)
	}
	obj, err := fs.ReadDecoded()
	switch {
	case err == nil:
		t.Fatalf("the relay forwarded Object {%d,%d} of a track it refused",
			obj.GroupID, obj.ObjectID)
	case errors.Is(err, io.EOF):
		t.Fatal("the FETCH stream completed; want it reset over what the upstream sent")
	}
	awaitResets(t, resets, code, kinds...)
}

// TestRelay_EndSignalsAgree: an END_OF_GROUP status at 2 and a FIN after Object
// 1 on a stream with END_OF_GROUP set end the Group at the same place
// (§11.2.1.1, §11.4.2, §9.1), so the track goes on.
func TestRelay_EndSignalsAgree(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam1", 7)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	events := make(chan objEvent, 16)
	go readSubgroups(t.Context(), subSess, events)

	sendStreams(t, pubSess, 7, []testStream{
		{group: 1, endOfGroup: true, objects: []uint64{0, 1}},
		{group: 1, subgroup: 1, objects: []uint64{2}, status: message.ObjectStatusEndOfGroup},
	})
	// Both downstream streams FIN only after the relay recorded both ends.
	for fins := 0; fins < 2; {
		select {
		case ev := <-events:
			if ev.err == nil {
				continue
			}
			if !errors.Is(ev.err, io.EOF) {
				t.Fatalf("a downstream stream ended with %v, want a FIN", ev.err)
			}
			fins++
		case <-time.After(2 * time.Second):
			t.Fatalf("%d/2 downstream streams FIN'd", fins)
		}
	}
	sendStreams(t, pubSess, 7, []testStream{{group: 2, objects: []uint64{0}, open: true}})
	select {
	case ev := <-events:
		if ev.err != nil || ev.absID != 0 {
			t.Fatalf("got %+v, want Object 0 of Group 2: the track ended", ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Group 2 not forwarded: the track ended")
	}
}

// malformedPriorityStreams makes the track on alias malformed: two streams of
// Subgroup 0 of Group 1 with different Publisher Priorities (§2.4.2 item 1).
func malformedPriorityStreams(t *testing.T, pubSess *session.Session, alias uint64) {
	t.Helper()
	sendStreams(t, pubSess, alias, []testStream{
		{group: 1, priority: 1, objects: []uint64{0}, open: true},
		{group: 1, priority: 2, objects: []uint64{1}, open: true},
	})
}

// awaitResets waits up to 1s for a reset of each of kinds on the conn resets
// records, and requires each to carry code. A kind's first reset counts; the
// others are ignored.
func awaitResets(t *testing.T, resets <-chan streamReset, code moqt.StreamResetCode, kinds ...resetStream) {
	t.Helper()
	pending := map[resetStream]bool{}
	for _, k := range kinds {
		pending[k] = true
	}
	deadline := time.After(time.Second)
	for len(pending) > 0 {
		select {
		case r := <-resets:
			if !pending[r.stream] {
				continue
			}
			if r.code != code {
				t.Fatalf("%v with %#x, want %#x", r.stream, uint64(r.code), uint64(code))
			}
			delete(pending, r.stream)
		case <-deadline:
			t.Fatalf("within 1s, no %v with %#x", slices.Collect(maps.Keys(pending)), uint64(code))
		}
	}
}

// TestRelay_MalformedTrackCancelsUpstreamFetch: a malformed live Object from
// the publisher an upstream FETCH is waiting on cancels that FETCH (§2.4.2:
// "MUST cancel any corresponding subscription or fetches for that Track from
// that publisher"), its data stream with MALFORMED_TRACK, and resets the
// downstream fetch stream it was filling with MALFORMED_TRACK, not after
// FILL_TIMEOUT.
func TestRelay_MalformedTrackCancelsUpstreamFetch(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	upResets := make(chan streamReset, 16)
	resets := make(chan streamReset, 16)
	l.resetsFor = func(conn int) chan<- streamReset {
		switch conn {
		case 1: // the upstream
			return upResets
		case 3: // the fetcher; the live subscriber is 2
			return resets
		}
		return nil
	}
	upSess, teardown := connectRelayOn(t, relay.Config{}, l)
	t.Cleanup(teardown)
	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns("video")}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	fetched := make(chan *session.Request, 1)
	go func() {
		for {
			req, err := upSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch m := req.First.(type) {
			case *message.Subscribe:
				if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
					return
				}
				// The live stream misses Object 1.
				publishCam1Group(t, upSess, 42, true, cam1Object{0, 0, nil}, cam1Object{0, 2, nil})
			case *message.Fetch:
				if req.Reply(&message.FetchOK{EndLocation: fetchOKEnd(m)}) != nil {
					return
				}
				out, err := upSess.OpenFetchStream(message.FetchHeader{RequestID: m.RequestID})
				if err != nil {
					return
				}
				// Nothing more: the stream stays open.
				t.Cleanup(func() { out.Cancel(moqt.StreamResetCancelled) })
				fetched <- req
			}
		}
	}()
	live := dialAnotherClient(t, upSess)
	subscribeCam1(t, live)
	go drainAll(t.Context(), live)
	fc := dialAnotherClient(t, upSess)
	waitRelayLargest(t, fc, ns("video"), []byte("cam1"), 0, 2)

	fr, err := fc.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 2}),
			message.FillTimeoutParam(10 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	if _, err := fc.AcceptDataStream(t.Context()); err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	var upFetch *session.Request
	select {
	case upFetch = <-fetched:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay did not FETCH the hole from the upstream")
	}

	malformedPriorityStreams(t, upSess, 42)
	select {
	case <-upFetch.Stream.Context().Done():
	case <-time.After(time.Second):
		t.Fatal("the relay kept its FETCH to the publisher of a malformed track")
	}
	awaitResets(t, upResets, moqt.StreamResetMalformedTrack, fetchStreamStop)
	awaitResets(t, resets, moqt.StreamResetMalformedTrack, fetchStreamReset, requestStreamReset)
}

// TestRelay_MalformedTrackResetsCachedFetch: a FETCH served from the cache is
// reset with MALFORMED_TRACK when the track is found malformed while its
// stream is written (§2.4.2: "reset any fetch streams with Status Code
// MALFORMED_TRACK").
func TestRelay_MalformedTrackResetsCachedFetch(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	resets := make(chan streamReset, 16)
	l.resetsFor = resetsOn(3, resets) // the publisher is 1, the live subscriber 2
	pubSess, teardown := connectRelayOn(t, relay.Config{}, l)
	t.Cleanup(teardown)
	publishVideoTrack(t, pubSess, "cam1", 7)
	live := dialAnotherClient(t, pubSess)
	subscribeCam1(t, live)
	go drainAll(t.Context(), live)
	publishObjects(t, pubSess, 7, 0, 3)
	fc := dialAnotherClient(t, pubSess)
	waitRelayLargest(t, fc, ns("video"), []byte("cam1"), 0, 2)

	fr, err := fc.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"), Name: []byte("cam1"),
		Parameters: message.Parameters{fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 2})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	// Unread, the stream holds the relay's first Object write.
	if _, err := fc.AcceptDataStream(t.Context()); err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}

	malformedPriorityStreams(t, pubSess, 7)
	awaitResets(t, resets, moqt.StreamResetMalformedTrack, fetchStreamReset, requestStreamReset)
}

// TestRelay_MalformedTrackResetsFillStream: a fill fetch stream (§5.1.3) is a
// fetch stream too, reset with MALFORMED_TRACK when the track is found
// malformed while it is written (§2.4.2).
func TestRelay_MalformedTrackResetsFillStream(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	resets := make(chan streamReset, 16)
	l.resetsFor = resetsOn(3, resets) // the publisher is 1, the live subscriber 2
	pubSess, teardown := connectRelayOn(t, relay.Config{}, l)
	t.Cleanup(teardown)
	publishVideoTrack(t, pubSess, "cam1", 7)
	live := dialAnotherClient(t, pubSess)
	subscribeCam1(t, live)
	go drainAll(t.Context(), live)
	publishObjects(t, pubSess, 7, 0, 3)
	subSess := dialAnotherClient(t, pubSess)
	waitRelayLargest(t, subSess, ns("video"), []byte("cam1"), 0, 2)

	subscribeCam1(t, subSess, message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}))
	// Unread, the fill stream holds the relay's first Object write.
	ds, err := subSess.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	if _, ok := ds.(*session.IncomingFetchStream); !ok {
		t.Fatalf("AcceptDataStream = %T, want the fill fetch stream", ds)
	}

	malformedPriorityStreams(t, pubSess, 7)
	awaitResets(t, resets, moqt.StreamResetMalformedTrack, fetchStreamReset)
}
