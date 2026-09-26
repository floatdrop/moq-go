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

// objCopy is one publisher's copy of Object {1, 0}.
type objCopy struct {
	datagram bool
	subgroup uint64
	priority uint8 // inline when set; else the track default
	status   uint64
	props    []byte
	payload  string
}

// sendCopy sends c as Object {1, 0} from sess on alias and, if withNext, a
// further Object: {1, 1} on the same subgroup stream, or datagram {2, 0}; the
// relay reads them in order. Only the copy's send must succeed: a malformed one
// gets the stream cancelled.
func sendCopy(t *testing.T, sess *session.Session, alias uint64, c objCopy, withNext bool) {
	t.Helper()
	if c.datagram {
		ds := []*message.ObjectDatagram{
			{GroupID: 1, ObjectStatus: c.status, Properties: c.props, ObjectPayload: []byte(c.payload)},
			{GroupID: 2, ObjectPayload: []byte("next")},
		}
		if !withNext {
			ds = ds[:1]
		}
		for _, d := range ds {
			d.TrackAlias = alias
			d.Type = message.DatagramDefaultPriorityBit
			if len(d.Properties) > 0 {
				d.Type |= message.DatagramPropertiesBit
			}
			if d.ObjectStatus != 0 {
				d.Type |= message.DatagramStatusBit
			}
			if err := sess.SendDatagram(d); err != nil {
				t.Errorf("SendDatagram: %v", err)
			}
		}
		return
	}
	sg, err := sess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: alias, GroupID: 1, SubgroupID: c.subgroup,
		Properties: true, InlinePriority: c.priority != 0, PublisherPriority: c.priority,
	})
	if err != nil {
		t.Errorf("OpenSubgroup: %v", err)
		return
	}
	t.Cleanup(func() { _ = sg.Close() })
	if err := sg.WriteObject(&message.SubgroupObject{
		ObjectStatus: c.status, Properties: c.props, Payload: []byte(c.payload),
	}); err != nil {
		t.Errorf("WriteObject: %v", err)
	}
	if withNext {
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("next")})
	}
}

func immutableProps(v uint64) []byte {
	return message.AppendTrackProperties([]wire.KVPair{{
		Type:    message.PropertyImmutableProperties,
		ByteVal: message.AppendTrackProperties([]wire.KVPair{{Type: 0x40, IntVal: v}}),
	}})
}

func mutableProps(v uint64) []byte {
	return message.AppendTrackProperties([]wire.KVPair{{Type: 0x40, IntVal: v}})
}

// TestRelay_DuplicateConsistency: a duplicate of a cached Object with a
// different Forwarding Preference, Subgroup ID, Priority or Payload (§9.1), or
// different Immutable Properties (§2.4.2, §12.7), makes the track malformed.
// Mutable Properties may differ (§9.1), and Normal may become End of Group
// (§9.1: existing to not existing).
func TestRelay_DuplicateConsistency(t *testing.T) {
	t.Parallel()
	sub := objCopy{payload: "a"}
	dg := objCopy{datagram: true, payload: "a"}
	with := func(c objCopy, f func(*objCopy)) objCopy { f(&c); return c }
	for _, tc := range []struct {
		name          string
		first, second objCopy
		malformed     bool
	}{
		{"payload", sub, with(sub, func(c *objCopy) { c.payload = "b" }), true},
		{"priority", sub, with(sub, func(c *objCopy) { c.priority = 7 }), true},
		{"Subgroup ID", sub, with(sub, func(c *objCopy) { c.subgroup = 1 }), true},
		{"forwarding preference", sub, dg, true},
		{
			"Immutable Properties",
			with(sub, func(c *objCopy) { c.props = immutableProps(1) }),
			with(sub, func(c *objCopy) { c.props = immutableProps(2) }), true,
		},
		{"Immutable Properties removed", with(sub, func(c *objCopy) { c.props = immutableProps(1) }), sub, true},
		{"datagram payload", dg, with(dg, func(c *objCopy) { c.payload = "b" }), true},

		{"identical", sub, sub, false},
		{
			"mutable Properties",
			with(sub, func(c *objCopy) { c.props = mutableProps(1) }),
			with(sub, func(c *objCopy) { c.props = mutableProps(2) }), false,
		},
		{"identical datagram", dg, dg, false},
		{"Normal, then End of Group", dg, with(dg, func(c *objCopy) {
			c.status, c.payload = message.ObjectStatusEndOfGroup, ""
		}), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubA, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			pubB := dialAnotherClient(t, pubA)
			publishVideoTrack(t, pubA, "cam1", 1)
			publishVideoTrack(t, pubB, "cam1", 2)
			subSess := dialAnotherClient(t, pubA)
			subReq := subscribeCam1(t, subSess)
			received := make(chan got, 8)
			go receiveDatagrams(t.Context(), subSess, received)
			go receiveSubgroupObjects(t.Context(), subSess, received)
			await := func(want got) {
				t.Helper()
				for {
					select {
					case g := <-received:
						if g == want {
							return
						}
					case <-time.After(2 * time.Second):
						t.Fatalf("Object %+v not forwarded", want)
					}
				}
			}

			sendCopy(t, pubA, 1, tc.first, false)
			await(got{1, 0}) // forwarded, so cached
			sendCopy(t, pubB, 2, tc.second, true)
			if tc.malformed {
				if pd := awaitPublishDone(t, subReq); pd.StatusCode != moqt.PublishDoneMalformedTrack {
					t.Fatalf("PUBLISH_DONE %#x, want MALFORMED_TRACK", uint64(pd.StatusCode))
				}
				return
			}
			// B's next Object is read after its copy: forwarded, the track
			// survived the copy.
			next := got{1, 1}
			if tc.second.datagram {
				next = got{2, 0}
			}
			await(next)
		})
	}
}
