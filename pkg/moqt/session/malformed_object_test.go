package session_test

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// §2.4.2: "When a subscriber detects a Malformed Track, it MUST cancel any
// corresponding subscription or fetches for that Track from that publisher
// (see Section 3.3.3), and SHOULD deliver an error to the application." The
// session delivers the error — ErrMalformedTrack from the read — and leaves
// the cancelling to the caller, which holds the subscription. The session
// itself stays up: a malformed track is not a protocol violation.

// objProps builds raw Object Properties.
func objProps(pairs ...wire.KVPair) []byte { return message.AppendTrackProperties(pairs) }

func requireMalformedTrack(t *testing.T, err error, sess *session.Session) {
	t.Helper()
	if !errors.Is(err, session.ErrMalformedTrack) {
		t.Fatalf("read = %v, want an error wrapping ErrMalformedTrack", err)
	}
	select {
	case <-sess.Done():
		t.Fatalf("session closed over a malformed track: %v", sess.Err())
	case <-time.After(50 * time.Millisecond):
	}
}

// TestSubgroupObjectMalformedProperties: the checks use the Object's absolute
// ID — Object 4, delta-encoded as 0 after Object 3, with a Prior Object ID
// Gap of 5 is malformed (§12.9); Object 3 with a gap of 3 is not.
func TestSubgroupObjectMalformedProperties(t *testing.T) {
	for _, tc := range []struct {
		name   string
		second []byte // Object 4's Properties
		read   func(*session.IncomingSubgroupStream) error
	}{
		{"gap past the ID, ReadObject", objProps(wire.KVPair{Type: message.PropertyPriorObjectIDGap, IntVal: 5}),
			func(s *session.IncomingSubgroupStream) error { _, err := s.ReadObject(); return err }},
		{"gap past the ID, ReadDecoded", objProps(wire.KVPair{Type: message.PropertyPriorObjectIDGap, IntVal: 5}),
			func(s *session.IncomingSubgroupStream) error { _, err := s.ReadDecoded(); return err }},
		{"Mandatory Track Property", objProps(wire.KVPair{Type: 0x4000, IntVal: 1}),
			func(s *session.IncomingSubgroupStream) error { _, err := s.ReadDecoded(); return err }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			hdr := message.SubgroupHeader{TrackAlias: 1, GroupID: 7, Properties: true}
			var wg sync.WaitGroup
			wg.Go(func() {
				out, err := cli.OpenSubgroup(hdr)
				if err != nil {
					return
				}
				_ = out.WriteObject(&message.SubgroupObject{
					ObjectIDDelta: 3,
					Properties:    objProps(wire.KVPair{Type: message.PropertyPriorObjectIDGap, IntVal: 3}),
				})
				_ = out.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Properties: tc.second})
				_ = out.Close()
			})
			ds, err := srv.AcceptDataStream(t.Context())
			must(t, err)
			in := ds.(*session.IncomingSubgroupStream)
			if err := tc.read(in); err != nil {
				t.Fatalf("Object 3 with Prior Object ID Gap 3: %v", err)
			}
			requireMalformedTrack(t, tc.read(in), srv)
			in.Cancel(0)
			wg.Wait()
		})
	}
}

// TestDatagramMalformedProperties: ReceiveDatagram returns the Object with
// the error, so the caller knows which track to cancel.
func TestDatagramMalformedProperties(t *testing.T) {
	cli, srv := openPair(t)
	must(t, cli.SendDatagram(&message.ObjectDatagram{
		Type: message.DatagramPropertiesBit, TrackAlias: 42, GroupID: 7, ObjectID: 3,
		Properties:    objProps(wire.KVPair{Type: message.PropertyPriorGroupIDGap, IntVal: 8}),
		ObjectPayload: []byte("x"),
	}))
	d, err := srv.ReceiveDatagram(t.Context())
	requireMalformedTrack(t, err, srv)
	if d == nil || d.TrackAlias != 42 {
		t.Fatalf("ReceiveDatagram returned %+v with the error, want the Object on alias 42", d)
	}
}

// TestFetchObjectMalformedProperties: ReadDecoded, which knows the Object's
// absolute IDs, checks a FETCH response's Objects.
func TestFetchObjectMalformedProperties(t *testing.T) {
	cli, srv := openPair(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		req, err := srv.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if _, err := req.AcceptFetch(nil); err != nil {
			return
		}
		out, err := srv.OpenFetchStream(message.FetchHeader{RequestID: req.First.(*message.Fetch).RequestID})
		if err != nil {
			return
		}
		_ = out.WriteObject(&message.FetchObject{
			SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
				message.FetchFlagPriority | message.FetchFlagProperties,
			GroupIDDelta: 2, ObjectIDDelta: 0,
			Properties: objProps(wire.KVPair{
				Type: message.PropertyImmutableProperties, ByteVal: objProps(wire.KVPair{
					Type: message.PropertyImmutableProperties, ByteVal: nil,
				}),
			}),
			ObjectPayload: []byte("x"),
		})
		_ = out.Close()
	})
	if _, err := cli.Fetch(t.Context(), &message.Fetch{Name: []byte("t")}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	ds, err := cli.AcceptDataStream(t.Context())
	must(t, err)
	_, err = ds.(*session.IncomingFetchStream).ReadDecoded()
	requireMalformedTrack(t, err, cli)
	ds.(*session.IncomingFetchStream).Cancel(0)
	wg.Wait()
}
