package session_test

import (
	"bytes"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// writeSubgroup opens a subgroup stream on s in the background and writes
// each chunk once the previous one's receive is acknowledged on next, then
// FINs. The in-process transport is synchronous, so a write returns only once
// the peer has read it.
func writeSubgroup(t *testing.T, s *session.Session, hdr message.SubgroupHeader, next <-chan struct{},
	chunks ...[]byte,
) {
	t.Helper()
	go func() {
		out, err := s.OpenSubgroup(hdr)
		if err != nil {
			t.Errorf("OpenSubgroup: %v", err)
			return
		}
		for i, c := range chunks {
			if i > 0 {
				<-next
			}
			if _, err := out.Write(c); err != nil {
				t.Errorf("Write chunk %d: %v", i, err)
				return
			}
		}
		<-next
		_ = out.Close()
	}()
}

// acceptSubgroup accepts the next data stream on s as a subgroup stream.
func acceptSubgroup(t *testing.T, s *session.Session) *session.IncomingSubgroupStream {
	t.Helper()
	ds, err := s.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	ss, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}
	return ss
}

// TestReadObjectHeaderBeforePayload: an Object's header is readable, with its
// absolute Object ID, while its payload is still arriving — the first byte of
// §3.1.4 — and ReadObjectPayload then reads the rest.
func TestReadObjectHeaderBeforePayload(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	next := make(chan struct{})
	// Object ID Delta 7 (the first Object's absolute ID), Payload Length 4,
	// then half the payload; the other half only once the header was read.
	writeSubgroup(t, client, message.SubgroupHeader{TrackAlias: 1, GroupID: 3}, next,
		[]byte{0x07, 0x04, 'a', 'b'}, []byte{'c', 'd'})
	in := acceptSubgroup(t, server)

	obj, err := in.ReadObjectHeader()
	if err != nil {
		t.Fatalf("ReadObjectHeader: %v", err)
	}
	if in.ObjectID() != 7 || obj.Payload != nil {
		t.Fatalf("header: ObjectID %d, Payload %q; want 7 and none yet", in.ObjectID(), obj.Payload)
	}
	close(next)
	if err := in.ReadObjectPayload(obj); err != nil {
		t.Fatalf("ReadObjectPayload: %v", err)
	}
	if !bytes.Equal(obj.Payload, []byte("abcd")) {
		t.Fatalf("Payload = %q, want %q", obj.Payload, "abcd")
	}
}

// TestReadObjectPayloadFINMidObject: a FIN after the header but before the
// payload's last byte is a FIN mid-Object (§11.3), closing the session with
// PROTOCOL_VIOLATION, as it is through ReadObject.
func TestReadObjectPayloadFINMidObject(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	next := make(chan struct{})
	writeSubgroup(t, client, message.SubgroupHeader{TrackAlias: 1}, next, []byte{0x00, 0x04, 'a', 'b'})
	in := acceptSubgroup(t, server)
	obj, err := in.ReadObjectHeader()
	if err != nil {
		t.Fatalf("ReadObjectHeader: %v", err)
	}
	close(next) // FIN with the payload half sent
	if err := in.ReadObjectPayload(obj); err == nil {
		t.Fatal("ReadObjectPayload: want an error for a FIN mid-payload")
	}
	requireClosedProtocolViolation(t, server)
}
