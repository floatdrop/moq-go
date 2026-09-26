package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// got is an Object's location as the subscriber received it.
type got struct{ group, id uint64 }

// receiveDatagrams emits the location of each datagram sess receives.
func receiveDatagrams(ctx context.Context, sess *session.Session, out chan<- got) {
	for {
		d, err := sess.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		out <- got{d.GroupID, d.ObjectID}
	}
}

// receiveSubgroupObjects emits the location of each Object on every subgroup
// stream sess accepts.
func receiveSubgroupObjects(ctx context.Context, sess *session.Session, out chan<- got) {
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			return
		}
		go func() {
			for {
				o, err := sg.ReadDecoded()
				if err != nil {
					return
				}
				out <- got{o.GroupID, o.ObjectID}
			}
		}()
	}
}

// TestRelay_ObjectInsideAnnouncedGapDropped: an Object inside a Prior Object or
// Group ID Gap announced earlier is known not to exist (§2.1), so the relay
// neither forwards nor caches it (§9.1), and the track goes on.
func TestRelay_ObjectInsideAnnouncedGapDropped(t *testing.T) {
	t.Parallel()
	type object struct {
		group, id uint64
		props     []byte
	}
	for _, tc := range []struct {
		name     string
		datagram bool
		// first announces the gap; dropped is inside it; next follows, on the
		// same downstream stream as dropped for subgroups.
		first, dropped, next object
	}{
		{
			name:    "subgroup, object gap",
			first:   object{1, 3, priorGap(message.PropertyPriorObjectIDGap, 2)},
			dropped: object{1, 2, nil}, next: object{1, 4, nil},
		},
		{
			name: "datagram, object gap", datagram: true,
			first:   object{1, 3, priorGap(message.PropertyPriorObjectIDGap, 2)},
			dropped: object{1, 2, nil}, next: object{1, 4, nil},
		},
		{
			name: "datagram, group gap", datagram: true,
			first:   object{5, 0, priorGap(message.PropertyPriorGroupIDGap, 2)},
			dropped: object{4, 0, nil}, next: object{5, 1, nil},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, teardown := connectRelay(t, relay.Config{})
			defer teardown()
			pub := publishVideoTrack(t, pubSess, "cam1", 1)
			subSess := newCam1Subscriber(t, pubSess)

			received := make(chan got, 8)
			if tc.datagram {
				go receiveDatagrams(t.Context(), subSess, received)
			} else {
				go receiveSubgroupObjects(t.Context(), subSess, received)
			}
			await := func() got {
				t.Helper()
				select {
				case g := <-received:
					return g
				case <-time.After(2 * time.Second):
					t.Fatal("no Object forwarded")
					return got{}
				}
			}
			send := func(sg *session.OutgoingSubgroupStream, o object) {
				t.Helper()
				if tc.datagram {
					d := &message.ObjectDatagram{
						TrackAlias:    1,
						GroupID:       o.group,
						ObjectID:      o.id,
						Properties:    o.props,
						ObjectPayload: []byte("x"),
					}
					if len(o.props) > 0 {
						d.Type = message.DatagramPropertiesBit
					}
					if err := pubSess.SendDatagram(d); err != nil {
						t.Fatalf("SendDatagram: %v", err)
					}
					return
				}
				if err := sg.WriteObjectAt(
					o.id,
					&message.SubgroupObject{Properties: o.props, Payload: []byte("x")},
				); err != nil {
					t.Fatalf("WriteObjectAt: %v", err)
				}
			}
			open := func(group, subgroup uint64) *session.OutgoingSubgroupStream {
				t.Helper()
				if tc.datagram {
					return nil
				}
				sg, err := pub.OpenSubgroup(message.SubgroupHeader{
					SubgroupIDMode: message.SubgroupIDExplicit, GroupID: group, SubgroupID: subgroup, Properties: true,
				})
				if err != nil {
					t.Fatalf("OpenSubgroup: %v", err)
				}
				t.Cleanup(func() { _ = sg.Close() })
				return sg
			}

			send(open(tc.first.group, 0), tc.first)
			if g := await(); g != (got{tc.first.group, tc.first.id}) {
				t.Fatalf("forwarded %+v, want the first Object", g)
			}
			// Datagrams are read in order; so are Objects on one stream.
			later := open(tc.dropped.group, 1)
			send(later, tc.dropped)
			send(later, tc.next)
			if g := await(); g != (got{tc.next.group, tc.next.id}) {
				t.Fatalf("forwarded %+v, want %+v: the Object inside the gap is not forwarded", g, tc.next)
			}

			// subSess's reader would take the FETCH response stream.
			fetcher := dialAnotherClient(t, pubSess)
			fetchReq, err := fetcher.Fetch(t.Context(), &message.Fetch{
				Namespace: ns("video"), Name: []byte("cam1"),
				Parameters: message.Parameters{fetchRangeFilter(
					message.Location{},
					message.Location{Group: tc.next.group, Object: tc.next.id},
				)},
			})
			if err != nil {
				t.Fatalf("Fetch: %v", err)
			}
			defer fetchReq.Close()
			servedNext := false
			for _, e := range collectFetchElems(t, fetcher, message.GroupOrderAscending, 2*time.Second) {
				if e.Marker {
					continue
				}
				if e.Group == tc.dropped.group && e.Object == tc.dropped.id {
					t.Fatalf("FETCH served Object %d of Group %d, inside the announced gap", e.Object, e.Group)
				}
				servedNext = servedNext || e.Group == tc.next.group && e.Object == tc.next.id
			}
			if !servedNext {
				t.Fatal("FETCH did not serve the Object after the gap")
			}
		})
	}
}
