package session_test

import (
	"errors"
	"io"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// TestSubgroupObjectReadRejectsInvalidStatus: ReadObject validates each decoded
// object, so an empty payload with a status that is not
// Normal/EndOfGroup/EndOfTrack is a §11 protocol violation, not a valid object.
func TestSubgroupObjectReadRejectsInvalidStatus(t *testing.T) {
	cli, srv := openPair(t)
	hdr := message.SubgroupHeader{TrackAlias: 42, GroupID: 7, SubgroupIDMode: message.SubgroupIDImplicitZero}
	// 0x2 is not a defined Object Status.
	in, writeErr := sendSubgroup(t, cli, srv, hdr, writeObjects(&message.SubgroupObject{ObjectStatus: 0x2}))

	if _, err := in.ReadObject(); err == nil {
		t.Fatal("ReadObject must reject an object with an invalid status")
	}
	// The rejection closes the session under the writer, so its result is not checked.
	<-writeErr
}

// TestSubgroupObjectRoundTrip: objects written with WriteObject read back
// unchanged, the header's Properties flag reaching both ends without the caller
// tracking it, and the closed stream then reads as io.EOF.
func TestSubgroupObjectRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		hdr  message.SubgroupHeader
		objs []*message.SubgroupObject
	}{
		{
			name: "without properties",
			hdr:  message.SubgroupHeader{TrackAlias: 42, GroupID: 7, SubgroupIDMode: message.SubgroupIDImplicitZero},
			objs: []*message.SubgroupObject{
				{ObjectIDDelta: 0, Payload: []byte("hello")},
				{ObjectIDDelta: 0, Payload: []byte("world")},
			},
		},
		{
			name: "with properties",
			hdr:  message.SubgroupHeader{TrackAlias: 1, GroupID: 0, Properties: true},
			// One property: Type 2 (a varint value), value 7.
			objs: []*message.SubgroupObject{{Properties: []byte{0x02, 0x07}, Payload: []byte("data")}},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			in, writeErr := sendSubgroup(t, cli, srv, tc.hdr, writeObjects(tc.objs...))

			if in.Header.TrackAlias != tc.hdr.TrackAlias || in.Header.GroupID != tc.hdr.GroupID {
				t.Errorf("header mismatch: got %+v, want %+v", in.Header, tc.hdr)
			}
			for i, want := range tc.objs {
				got, err := in.ReadObject()
				if err != nil {
					t.Fatalf("ReadObject(%d): %v", i, err)
				}
				if got.ObjectIDDelta != want.ObjectIDDelta {
					t.Errorf("object %d delta: got %d, want %d", i, got.ObjectIDDelta, want.ObjectIDDelta)
				}
				if string(got.Properties) != string(want.Properties) {
					t.Errorf("object %d properties: got %x, want %x", i, got.Properties, want.Properties)
				}
				if string(got.Payload) != string(want.Payload) {
					t.Errorf("object %d payload: got %q, want %q", i, got.Payload, want.Payload)
				}
			}
			if _, err := in.ReadObject(); !errors.Is(err, io.EOF) {
				t.Errorf("ReadObject after close: got %v, want io.EOF", err)
			}
			if err := <-writeErr; err != nil {
				t.Errorf("writer: %v", err)
			}
		})
	}
}

// TestWriteObjectWrongType: the wrong object type is a compile error (distinct
// WriteObject signatures); at runtime a zero-value object must not panic.
func TestWriteObjectWrongType(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	// Drain the server side so the synchronous pipe doesn't deadlock the write.
	go func() {
		if ds, err := srv.AcceptDataStream(ctx); err == nil {
			io.Copy(io.Discard, ds)
		}
	}()

	outStream, err := cli.OpenSubgroup(message.SubgroupHeader{TrackAlias: 1, GroupID: 0})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	defer outStream.Cancel(0)

	// A nil payload is valid on the wire.
	if err := outStream.WriteObject(&message.SubgroupObject{}); err != nil {
		t.Errorf("WriteObject(zero SubgroupObject): unexpected error: %v", err)
	}
}
