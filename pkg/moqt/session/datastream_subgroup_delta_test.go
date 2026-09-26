package session_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestWriteObjectAt: WriteObjectAt is the exact encoding inverse of
// ReadDecoded — absolute Object IDs become the §11.4.2 deltas on the wire
// (first object's delta = absolute ID; later = currentID-prevID-1).
func TestWriteObjectAt(t *testing.T) {
	cli, srv := openPair(t)
	hdr := message.SubgroupHeader{TrackAlias: 42, GroupID: 7, SubgroupIDMode: message.SubgroupIDImplicitZero}

	writeIDs := []uint64{4, 5, 9}
	wantDeltas := []uint64{4, 0, 3}

	in, writeErr := sendSubgroup(t, cli, srv, hdr, func(out *session.OutgoingSubgroupStream) error {
		for i, id := range writeIDs {
			if err := out.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte{byte('a' + i)}}); err != nil {
				return err
			}
		}
		return nil
	})

	// Read the RAW objects: the exact wire deltas are the strongest check that
	// the absolute→delta mapping is right.
	for i := range writeIDs {
		raw, err := in.ReadObject()
		if err != nil {
			t.Fatalf("ReadObject #%d: %v", i, err)
		}
		if raw.ObjectIDDelta != wantDeltas[i] {
			t.Errorf("obj #%d: ObjectIDDelta got %d, want %d", i, raw.ObjectIDDelta, wantDeltas[i])
		}
		if string(raw.Payload) != string(byte('a'+i)) {
			t.Errorf("obj #%d: payload got %q, want %q", i, raw.Payload, string(byte('a'+i)))
		}
	}
	if _, err := in.ReadObject(); !errors.Is(err, io.EOF) {
		t.Errorf("trailing ReadObject: got %v, want io.EOF", err)
	}
	if err := <-writeErr; err != nil {
		t.Errorf("writer: %v", err)
	}
}

// TestWriteObjectAtRejectsNonIncreasing: an Object ID not greater than the
// previous one is rejected with ErrObjectIDNotIncreasing, nothing is written,
// and the stream stays usable for a subsequent in-order write.
func TestWriteObjectAtRejectsNonIncreasing(t *testing.T) {
	cli, srv := openPair(t)
	hdr := message.SubgroupHeader{TrackAlias: 1, GroupID: 0, SubgroupIDMode: message.SubgroupIDImplicitZero}

	in, writeErr := sendSubgroup(t, cli, srv, hdr, func(out *session.OutgoingSubgroupStream) error {
		if err := out.WriteObjectAt(5, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
			return err
		}
		for _, id := range []uint64{5, 3} { // equal, then lower
			err := out.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte("x")})
			if !errors.Is(err, session.ErrObjectIDNotIncreasing) {
				return fmt.Errorf("WriteObjectAt(%d) = %w, want ErrObjectIDNotIncreasing", id, err)
			}
		}
		return out.WriteObjectAt(6, &message.SubgroupObject{Payload: []byte("b")})
	})

	// Only the two accepted objects reach the wire.
	wantIDs := []uint64{5, 6}
	wantPayloads := []string{"a", "b"}
	for i := range wantIDs {
		got, err := in.ReadDecoded()
		if err != nil {
			t.Fatalf("ReadDecoded #%d: %v", i, err)
		}
		if got.ObjectID != wantIDs[i] {
			t.Errorf("obj #%d: ObjectID got %d, want %d", i, got.ObjectID, wantIDs[i])
		}
		if string(got.Payload) != wantPayloads[i] {
			t.Errorf("obj #%d: payload got %q, want %q", i, got.Payload, wantPayloads[i])
		}
	}
	if _, err := in.ReadDecoded(); !errors.Is(err, io.EOF) {
		t.Errorf("trailing ReadDecoded: got %v, want io.EOF", err)
	}
	if err := <-writeErr; err != nil {
		t.Errorf("writer: %v", err)
	}
}

// TestIncomingSubgroupStream_ReadDecoded covers absolute ObjectID
// reconstruction (first object's delta is the absolute ID; subsequent deltas
// encode currentID - prevID - 1) and the three §11.4.2 SubgroupID modes.
func TestIncomingSubgroupStream_ReadDecoded(t *testing.T) {
	cases := []struct {
		name       string
		mode       message.SubgroupIDMode
		explicitID uint64 // Explicit mode only
		wantSubID  uint64
	}{
		{"ImplicitZero", message.SubgroupIDImplicitZero, 0, 0},
		{"ImplicitFirstObject", message.SubgroupIDImplicitFirstObject, 0, 4 /* first abs ObjectID */},
		{"Explicit", message.SubgroupIDExplicit, 99, 99},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			hdr := message.SubgroupHeader{
				TrackAlias:     42,
				GroupID:        7,
				SubgroupIDMode: tc.mode,
				SubgroupID:     tc.explicitID,
			}
			// Absolute IDs 4, 5, 9: the first delta carries the absolute ID,
			// the second is consecutive, the third skips 6/7/8.
			in, writeErr := sendSubgroup(t, cli, srv, hdr, writeObjects(
				&message.SubgroupObject{ObjectIDDelta: 4, Payload: []byte("a")},
				&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("b")},
				&message.SubgroupObject{ObjectIDDelta: 3, Payload: []byte("c")},
			))

			wantIDs := []uint64{4, 5, 9}
			wantPayloads := []string{"a", "b", "c"}
			for i := range wantIDs {
				got, err := in.ReadDecoded()
				if err != nil {
					t.Fatalf("ReadDecoded #%d: %v", i, err)
				}
				if got.GroupID != 7 {
					t.Errorf("obj #%d: GroupID got %d, want 7", i, got.GroupID)
				}
				if got.ObjectID != wantIDs[i] {
					t.Errorf("obj #%d: ObjectID got %d, want %d", i, got.ObjectID, wantIDs[i])
				}
				if got.SubgroupID != tc.wantSubID {
					t.Errorf("obj #%d: SubgroupID got %d, want %d", i, got.SubgroupID, tc.wantSubID)
				}
				if string(got.Payload) != wantPayloads[i] {
					t.Errorf("obj #%d: payload got %q, want %q", i, got.Payload, wantPayloads[i])
				}
			}
			if _, err := in.ReadDecoded(); !errors.Is(err, io.EOF) {
				t.Errorf("trailing ReadDecoded: got %v, want io.EOF", err)
			}
			if err := <-writeErr; err != nil {
				t.Errorf("writer: %v", err)
			}
		})
	}
}
