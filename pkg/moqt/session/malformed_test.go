package session_test

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ---------------------------------------------------------------------------
// Malformed request-stream messages (§10): a Length that disagrees with the
// Message Body, or an unknown type, closes the session with
// PROTOCOL_VIOLATION at every read point — the opener, its response, and a
// follow-up. A frame cut short by the stream ending stays scoped to its stream.
// ---------------------------------------------------------------------------

// writeTrailing writes m's frame with one extra body byte, so the Length covers bytes the body does not.
func writeTrailing(w io.Writer, m message.Message) error {
	enc := wire.NewWriter(nil)
	m.Append(enc)
	return wire.WriteFrame(w, uint64(m.Type()), append(enc.Bytes(), 0x00))
}

// writeShort writes m's frame without its last body byte, for messages whose last field runs to the end.
func writeShort(w io.Writer, m message.Message) error {
	enc := wire.NewWriter(nil)
	m.Append(enc)
	body := enc.Bytes()
	return wire.WriteFrame(w, uint64(m.Type()), body[:len(body)-1])
}

func TestMalformedOpenerClosesSession(t *testing.T) {
	t.Parallel()
	longName := bytes.Repeat([]byte("n"), 4097) // §2.4.1: over 4,096 bytes
	cases := []struct {
		name  string
		write func(io.Writer) error
	}{
		{"trailing bytes", func(w io.Writer) error {
			return writeTrailing(w, &message.Subscribe{Namespace: videoNS, Name: []byte("t")})
		}},
		{"Full Track Name over 4,096 bytes", func(w io.Writer) error {
			return message.Marshal(w, &message.Subscribe{Namespace: videoNS, Name: longName})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, server, cliConn, _ := openPairWithConns(t)
			stream, err := cliConn.OpenStream()
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			go func() { _ = tc.write(stream) }()
			if _, err := server.AcceptRequest(t.Context()); err == nil {
				t.Fatal("AcceptRequest accepted a malformed opener")
			}
			requireClosedProtocolViolation(t, server)
		})
	}
}

// TestTruncatedOpenerResetsOnlyStream: a stream ending or reset before its
// first frame is complete is the peer giving up on that request (§3.3.2,
// §3.3.3), not a Length mismatch: AcceptRequest moves on to the next request.
func TestTruncatedOpenerResetsOnlyStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		end  func(session.Stream)
	}{
		{"FIN", func(s session.Stream) { _ = s.Close() }},
		{"reset", func(s session.Stream) { s.CancelWrite(0) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server, cliConn, _ := openPairWithConns(t)
			stream, err := cliConn.OpenStream()
			if err != nil {
				t.Fatalf("OpenStream: %v", err)
			}
			go func() {
				// Type SUBSCRIBE, Length 16, then only two body bytes.
				_, _ = stream.Write([]byte{byte(message.TypeSubscribe), 0x00, 0x10, 0x00, 0x00})
				tc.end(stream)
				_, _ = session.OpenRequestForTest(client, &message.Subscribe{
					RequestID: 0, Namespace: videoNS, Name: []byte("next"),
				})
			}()
			req, err := server.AcceptRequest(t.Context())
			if err != nil {
				t.Fatalf("AcceptRequest: %v, want the request after the truncated one", err)
			}
			if name := string(req.First.(*message.Subscribe).Name); name != "next" {
				t.Fatalf("accepted %q, want the request after the truncated one", name)
			}
			requireStaysOpen(t, server, 100*time.Millisecond)
		})
	}
}

func TestMalformedResponseClosesSession(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		_ = writeShort(r.Stream, &message.SubscribeOK{TrackAlias: 1})
	}()
	if _, err := client.Subscribe(t.Context(), &message.Subscribe{Namespace: videoNS, Name: []byte("t")}); err == nil {
		t.Fatal("Subscribe accepted a malformed SUBSCRIBE_OK")
	}
	requireClosedProtocolViolation(t, client)
}

// TestMalformedFollowupClosesSession covers the broker, which reads every
// follow-up on a request the application holds a typed handle for.
func TestMalformedFollowupClosesSession(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		write func(io.Writer) error
	}{
		{"trailing bytes", func(w io.Writer) error {
			return writeTrailing(w, &message.RequestUpdate{RequestID: 1})
		}},
		{"unknown message type", func(w io.Writer) error {
			return wire.WriteFrame(w, 0x3F00, nil) // unassigned type
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, server := openPair(t)
			go func() {
				r, err := server.AcceptRequest(t.Context())
				if err != nil {
					return
				}
				if _, err := r.AcceptPublish(); err != nil {
					return
				}
				_ = tc.write(r.Stream)
			}()
			pub, err := client.Publish(t.Context(), &message.Publish{Namespace: videoNS, Name: []byte("t")})
			if err != nil {
				t.Fatalf("Publish: %v", err)
			}
			go func() { _ = pub.Broker().Serve(t.Context(), nil) }()
			requireClosedProtocolViolation(t, client)
		})
	}
}

func TestMalformedPublishSkippedClosesSession(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	go func() {
		r, err := server.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if err := r.Reply(&message.RequestOK{}); err != nil {
			return
		}
		_ = writeTrailing(
			r.Stream,
			&message.PublishSkipped{TrackNamespaceSuffix: wire.TrackNamespace{[]byte("x")}, TrackName: []byte("t")},
		)
	}()
	ts, err := client.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: videoNS})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	if _, err := ts.ReadPublishSkipped(); err == nil {
		t.Fatal("ReadPublishSkipped accepted a malformed PUBLISH_SKIPPED")
	}
	requireClosedProtocolViolation(t, client)
}

// ---------------------------------------------------------------------------
// Malformed Tracks (§2.4.2): the read returns ErrMalformedTrack and the
// session stays up; cancelling the subscription is left to the caller.
// ---------------------------------------------------------------------------

// objProps builds raw Object Properties.
func objProps(pairs ...wire.KVPair) []byte { return message.AppendTrackProperties(pairs) }

// requireMalformedTrack checks err wraps ErrMalformedTrack and sess stays up.
func requireMalformedTrack(t *testing.T, err error, sess *session.Session) {
	t.Helper()
	if !errors.Is(err, session.ErrMalformedTrack) {
		t.Fatalf("read = %v, want an error wrapping ErrMalformedTrack", err)
	}
	requireStaysOpen(t, sess, 50*time.Millisecond)
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

// TestImplicitSubgroupIDSurvivesMalformedFirstObject: §11.4.2 SUBGROUP_ID_MODE
// 0b01 makes the Subgroup ID the stream's first Object ID. It stays so for a
// caller that reads on after that first Object was reported malformed.
func TestImplicitSubgroupIDSurvivesMalformedFirstObject(t *testing.T) {
	cli, srv := openPair(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		out, err := cli.OpenSubgroup(message.SubgroupHeader{
			TrackAlias: 1, GroupID: 7, SubgroupIDMode: message.SubgroupIDImplicitFirstObject, Properties: true,
		})
		if err != nil {
			return
		}
		_ = out.WriteObject(&message.SubgroupObject{
			ObjectIDDelta: 5, Properties: objProps(wire.KVPair{Type: 0x4000, IntVal: 1}),
		})
		_ = out.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Properties: []byte{}})
		_ = out.Close()
	})
	ds, err := srv.AcceptDataStream(t.Context())
	must(t, err)
	in := ds.(*session.IncomingSubgroupStream)
	if _, err := in.ReadDecoded(); !errors.Is(err, session.ErrMalformedTrack) {
		t.Fatalf("first Object: %v, want ErrMalformedTrack", err)
	}
	d, err := in.ReadDecoded()
	must(t, err)
	if d.SubgroupID != 5 || d.ObjectID != 6 {
		t.Fatalf("second Object = Subgroup %d Object %d, want Subgroup 5 Object 6", d.SubgroupID, d.ObjectID)
	}
	wg.Wait()
}
