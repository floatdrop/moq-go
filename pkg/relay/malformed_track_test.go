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

// §2.4.2: "If a relay detects a Malformed Track, it MUST immediately terminate
// downstream subscriptions with PUBLISH_DONE and reset any fetch streams with
// Status Code MALFORMED_TRACK. Object(s) triggering Malformed Track status
// MUST NOT be cached." As a subscriber it "MUST cancel any corresponding
// subscription or fetches for that Track from that publisher".

// mandatoryObjectProps is an Object Property the track may not carry: a
// Mandatory Track Property used as an Object Property (§2.5.1).
func mandatoryObjectProps() []byte {
	return message.AppendTrackProperties([]wire.KVPair{{Type: 0x4000, IntVal: 1}})
}

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
			_ = sg.WriteObject(&message.SubgroupObject{Properties: mandatoryObjectProps(), Payload: []byte("x")})
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
				Properties: mandatoryObjectProps(), ObjectPayload: []byte("x"),
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
			subReq := subscribeCam1Req(t, subSess)
			go drainAllStreams(t.Context(), subSess)

			go tc.send(t, pubSess, 7)
			if pd := awaitPublishDone(t, subReq); pd.StatusCode != moqt.PublishDoneMalformedTrack {
				t.Fatalf("downstream PUBLISH_DONE %#x, want MALFORMED_TRACK %#x",
					uint64(pd.StatusCode), uint64(moqt.PublishDoneMalformedTrack))
			}
			requireUpstreamCancelled(t, pub)
		})
	}
}
