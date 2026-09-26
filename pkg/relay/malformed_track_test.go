package relay_test

import (
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Malformed tracks (§2.4.2): the relay ends downstream subscriptions with
// PUBLISH_DONE MALFORMED_TRACK, resets fetch streams, and cancels its own
// subscription upstream. Unknown Mandatory Track Properties (§2.5.1) refuse
// the track the same way.

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
// do not parse is MALFORMED_TRACK.
func TestRelay_PublishTrackPropertiesRejected(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		want  moqt.RequestErrorCode
	}{
		{"unknown mandatory", mandatoryProps(), moqt.RequestUnsupportedExtension},
		{"malformed", malformedProps, moqt.RequestMalformedTrack},
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
		{"malformed", malformedProps, moqt.RequestMalformedTrack},
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

// TestRelay_UpstreamMandatoryPropertyWinsOverOtherFailure: with two
// publishers for the namespace, one answering SUBSCRIBE_OK with an unknown
// Mandatory Track Property and the other refusing, the downstream subscriber
// still gets UNSUPPORTED_EXTENSION (§2.5.1), whichever publisher is tried last.
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
func TestRelay_UpstreamFetchOKUnknownMandatoryPropertyResetsStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
	}{
		{"unknown Mandatory Track Property", mandatoryProps()},
		{"Track Properties that do not parse", malformedProps},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refusedFetchResetsStream(t, tc.props, nil)
		})
	}
}

// TestRelay_UpstreamFetchMalformedObjectResetsStream: an upstream FETCH
// response Object that makes the track malformed resets the downstream fetch
// stream (§2.4.2).
func TestRelay_UpstreamFetchMalformedObjectResetsStream(t *testing.T) {
	t.Parallel()
	refusedFetchResetsStream(t, nil, mandatoryProps())
}

// refusedFetchResetsStream requires a stitched FETCH to be reset when the
// upstream answers it with FETCH_OK carrying upstreamProps and, if objProps is
// non-nil, one Object carrying objProps. The upstream misbehaves only once
// armed, so the FETCHes waiting for the live tail to be cached succeed.
func refusedFetchResetsStream(t *testing.T, upstreamProps, objProps []byte) {
	var armed atomic.Bool
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	video := ns("video")
	name := []byte("cam1")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
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
						SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: 42, GroupID: g,
					})
					if err != nil {
						return
					}
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}})
					_ = sg.Close()
				}
			case *message.Fetch:
				_ = req.Reply(&message.FetchOK{
					EndLocation:     message.Location{Group: stitchLiveLo - 1},
					TrackProperties: upstreamProps,
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
}
