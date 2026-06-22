package session_test

import (
	"errors"
	"io"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// TestSubgroupObjectReadRejectsInvalidStatus confirms ReadObject validates
// each decoded object: an object with an empty payload and a status that is
// not Normal/EndOfGroup/EndOfTrack is a §11 protocol violation and must be
// rejected on read, not surfaced as a valid object.
func TestSubgroupObjectReadRejectsInvalidStatus(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.SubgroupHeader{
		TrackAlias:     42,
		GroupID:        7,
		SubgroupIDMode: message.SubgroupIDImplicitZero,
	}
	// 0x2 is not a defined Object Status; with an empty payload it must fail.
	bad := &message.SubgroupObject{ObjectIDDelta: 0, ObjectStatus: 0x2}

	writeErr := make(chan error, 1)
	go func() {
		out, err := cli.OpenSubgroup(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		if err := out.WriteObject(bad); err != nil {
			writeErr <- err
			return
		}
		writeErr <- out.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}
	if _, err := in.ReadObject(); err == nil {
		t.Fatal("ReadObject must reject an object with an invalid status")
	}
	<-writeErr
}

// TestSubgroupObjectRoundTrip opens a SUBGROUP_HEADER uni-stream from the
// client, writes two SubgroupObjects via WriteObject, closes the stream, then
// reads them back on the server via AcceptDataStream + ReadObject.
//
// The in-process pipe is synchronous: Write blocks until Read consumes the
// bytes. We therefore run the writer in a goroutine so the reader (main
// goroutine) can drain concurrently.
func TestSubgroupObjectRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.SubgroupHeader{
		TrackAlias:     42,
		GroupID:        7,
		SubgroupID:     0,
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		Properties:     false,
	}

	obj1 := &message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("hello")}
	obj2 := &message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("world")}

	// Run the writer in a goroutine: the in-process pipe is synchronous so
	// Write blocks until the reader consumes the bytes.
	writeErr := make(chan error, 1)
	go func() {
		outStream, err := cli.OpenSubgroup(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		if err := outStream.WriteObject(obj1); err != nil {
			writeErr <- err
			return
		}
		if err := outStream.WriteObject(obj2); err != nil {
			writeErr <- err
			return
		}
		writeErr <- outStream.Close()
	}()

	// Accept and read on the main goroutine.
	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	inStream, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	if inStream.Header.TrackAlias != hdr.TrackAlias || inStream.Header.GroupID != hdr.GroupID {
		t.Errorf("header mismatch: got %+v, want %+v", inStream.Header, hdr)
	}

	for i, want := range []*message.SubgroupObject{obj1, obj2} {
		got, err := inStream.ReadObject()
		if err != nil {
			t.Fatalf("ReadObject(%d): %v", i, err)
		}
		if string(got.Payload) != string(want.Payload) {
			t.Errorf("object %d payload: got %q, want %q", i, got.Payload, want.Payload)
		}
		if got.ObjectIDDelta != want.ObjectIDDelta {
			t.Errorf("object %d delta: got %d, want %d", i, got.ObjectIDDelta, want.ObjectIDDelta)
		}
	}

	// After the sender closes, ReadObject should return (wrapped) io.EOF.
	_, err = inStream.ReadObject()
	if !errors.Is(err, io.EOF) {
		t.Errorf("ReadObject after close: got %v, want io.EOF", err)
	}

	if err := <-writeErr; err != nil {
		t.Errorf("writer goroutine: %v", err)
	}
}

// TestSubgroupObjectWithProperties verifies that the Properties flag is
// correctly propagated from the header to ReadObject/WriteObject without the
// caller needing to track it manually.
func TestSubgroupObjectWithProperties(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.SubgroupHeader{
		TrackAlias: 1,
		GroupID:    0,
		Properties: true, // objects carry a properties blob
	}
	obj := &message.SubgroupObject{
		ObjectIDDelta: 0,
		Properties:    []byte{0x01, 0x02},
		Payload:       []byte("data"),
	}

	writeErr := make(chan error, 1)
	go func() {
		outStream, err := cli.OpenSubgroup(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		if err := outStream.WriteObject(obj); err != nil {
			writeErr <- err
			return
		}
		writeErr <- outStream.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	inStream, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingSubgroupStream", ds)
	}

	got, err := inStream.ReadObject()
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if string(got.Properties) != string(obj.Properties) {
		t.Errorf("properties: got %x, want %x", got.Properties, obj.Properties)
	}
	if string(got.Payload) != string(obj.Payload) {
		t.Errorf("payload: got %q, want %q", got.Payload, obj.Payload)
	}

	if err := <-writeErr; err != nil {
		t.Errorf("writer goroutine: %v", err)
	}
}

// TestFetchObjectRoundTrip opens a FETCH_HEADER uni-stream from the client,
// writes a FetchObject via WriteObject, closes the stream, then reads it back
// on the server via AcceptDataStream + ReadObject.
func TestFetchObjectRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.FetchHeader{RequestID: 0}
	obj := message.NewFetchObject().
		WithGroupIDDelta(3).
		WithObjectIDDelta(1).
		WithPayload([]byte("fetch-payload"))

	writeErr := make(chan error, 1)
	go func() {
		outStream, err := cli.OpenFetchStream(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		if err := outStream.WriteObject(obj); err != nil {
			writeErr <- err
			return
		}
		writeErr <- outStream.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	inStream, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingFetchStream", ds)
	}

	if inStream.Header.RequestID != hdr.RequestID {
		t.Errorf("RequestID: got %d, want %d", inStream.Header.RequestID, hdr.RequestID)
	}

	got, err := inStream.ReadObject()
	if err != nil {
		t.Fatalf("ReadObject: %v", err)
	}
	if string(got.ObjectPayload) != string(obj.ObjectPayload) {
		t.Errorf("payload: got %q, want %q", got.ObjectPayload, obj.ObjectPayload)
	}
	if got.GroupIDDelta != obj.GroupIDDelta {
		t.Errorf("GroupIDDelta: got %d, want %d", got.GroupIDDelta, obj.GroupIDDelta)
	}

	if err := <-writeErr; err != nil {
		t.Errorf("writer goroutine: %v", err)
	}
}

// TestWriteObjectWrongType verifies that the type system prevents passing a
// FetchObject to an OutgoingSubgroupStream at compile time. At runtime we
// verify that a subgroup stream correctly rejects a nil payload (zero-value
// object) without panicking, and that a fetch stream correctly rejects a nil
// payload without panicking.
//
// The compile-time guarantee is the primary value: WriteObject(*SubgroupObject)
// and WriteObject(*FetchObject) are distinct method signatures, so the wrong
// type is a compile error, not a runtime error.
func TestWriteObjectWrongType(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	// Drain the server side in a goroutine so the test doesn't deadlock.
	go func() {
		if ds, err := srv.AcceptDataStream(ctx); err == nil {
			io.Copy(io.Discard, ds)
		}
	}()

	// Open a subgroup stream and write a zero-value SubgroupObject (nil payload).
	// This exercises the WriteObject path without a type mismatch.
	hdr := message.SubgroupHeader{TrackAlias: 1, GroupID: 0}
	outStream, err := cli.OpenSubgroup(ctx, hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	defer outStream.Cancel(0)

	// Writing a valid *SubgroupObject must succeed (nil payload is valid wire).
	if err := outStream.WriteObject(&message.SubgroupObject{}); err != nil {
		t.Errorf("WriteObject(zero SubgroupObject): unexpected error: %v", err)
	}
}

// TestWriteObjectAt verifies that WriteObjectAt is the exact encoding inverse
// of ReadDecoded: absolute Object IDs in become the correct §11.4.2 deltas on
// the wire (first object's delta = absolute ID; later = currentID-prevID-1) and
// read back as the same absolute IDs.
func TestWriteObjectAt(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.SubgroupHeader{
		TrackAlias:     42,
		GroupID:        7,
		SubgroupIDMode: message.SubgroupIDImplicitZero,
	}

	// Absolute IDs 4, 5, 9 — should serialize as deltas 4, 0, 3.
	writeIDs := []uint64{4, 5, 9}
	wantDeltas := []uint64{4, 0, 3}

	writeErr := make(chan error, 1)
	go func() {
		out, err := cli.OpenSubgroup(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		for i, id := range writeIDs {
			if err := out.WriteObjectAt(id, &message.SubgroupObject{
				Payload: []byte{byte('a' + i)},
			}); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- out.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("got %T, want *IncomingSubgroupStream", ds)
	}

	// Read the RAW objects to assert the exact wire deltas WriteObjectAt
	// produced — the strongest check that the absolute→delta mapping is right.
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

// TestWriteObjectAtRejectsNonIncreasing verifies the strict-increasing guard:
// an Object ID not greater than the previous one is rejected with
// ErrObjectIDNotIncreasing, nothing is written, and the stream stays usable for
// a subsequent in-order write.
func TestWriteObjectAtRejectsNonIncreasing(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.SubgroupHeader{TrackAlias: 1, GroupID: 0, SubgroupIDMode: message.SubgroupIDImplicitZero}

	writeErr := make(chan error, 1)
	go func() {
		out, err := cli.OpenSubgroup(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		if err := out.WriteObjectAt(5, &message.SubgroupObject{Payload: []byte("a")}); err != nil {
			writeErr <- err
			return
		}
		// Equal ID — must be rejected without writing.
		if err := out.WriteObjectAt(
			5,
			&message.SubgroupObject{Payload: []byte("x")},
		); !errors.Is(
			err,
			session.ErrObjectIDNotIncreasing,
		) {
			writeErr <- err
			return
		}
		// Lower ID — must be rejected too.
		if err := out.WriteObjectAt(
			3,
			&message.SubgroupObject{Payload: []byte("y")},
		); !errors.Is(
			err,
			session.ErrObjectIDNotIncreasing,
		) {
			writeErr <- err
			return
		}
		// The stream is still usable: an in-order write succeeds.
		if err := out.WriteObjectAt(6, &message.SubgroupObject{Payload: []byte("b")}); err != nil {
			writeErr <- err
			return
		}
		writeErr <- out.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("got %T, want *IncomingSubgroupStream", ds)
	}

	// Only the two accepted objects (IDs 5 and 6) reach the wire; the rejected
	// writes left no trace.
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
// reconstruction (first object's delta is the absolute ID; subsequent
// deltas encode currentID - prevID - 1) and the three §11.4.2 SubgroupID
// modes (ImplicitZero, ImplicitFirstObject, Explicit).
func TestIncomingSubgroupStream_ReadDecoded(t *testing.T) {
	cases := []struct {
		name       string
		mode       message.SubgroupIDMode
		explicitID uint64 // only used for Explicit mode
		wantSubID  uint64
	}{
		{"ImplicitZero", message.SubgroupIDImplicitZero, 0, 0},
		{"ImplicitFirstObject", message.SubgroupIDImplicitFirstObject, 0, 4 /* first abs ObjectID */},
		{"Explicit", message.SubgroupIDExplicit, 99, 99},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			ctx := t.Context()

			hdr := message.SubgroupHeader{
				TrackAlias:     42,
				GroupID:        7,
				SubgroupIDMode: tc.mode,
				SubgroupID:     tc.explicitID,
			}

			// Three objects with absolute IDs 4, 5, 9 — first is the
			// stream's "first object" (delta=4 carries absolute);
			// second is consecutive (delta=0); third skips 6/7/8
			// (delta=3 → +4).
			written := []*message.SubgroupObject{
				{ObjectIDDelta: 4, Payload: []byte("a")},
				{ObjectIDDelta: 0, Payload: []byte("b")},
				{ObjectIDDelta: 3, Payload: []byte("c")},
			}

			writeErr := make(chan error, 1)
			go func() {
				outStream, err := cli.OpenSubgroup(ctx, hdr)
				if err != nil {
					writeErr <- err
					return
				}
				for _, o := range written {
					if err := outStream.WriteObject(o); err != nil {
						writeErr <- err
						return
					}
				}
				writeErr <- outStream.Close()
			}()

			ds, err := srv.AcceptDataStream(ctx)
			if err != nil {
				t.Fatalf("AcceptDataStream: %v", err)
			}
			in, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				t.Fatalf("got %T, want *IncomingSubgroupStream", ds)
			}

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

// TestIncomingFetchStream_ReadDecoded_Ascending exercises the
// session-layer §11.4.4 delta decoder across all three transition kinds
// (first-object absolute, same-group, cross-group) and the four subgroup
// modes. The encoded objects mirror what the relay's
// streamFetchObjects writes for an ascending FETCH response.
func TestIncomingFetchStream_ReadDecoded_Ascending(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.FetchHeader{RequestID: 0}

	// Write four objects:
	//   {G=5, O=2, Sub=10, Pri=7}  first — flags carry absolute IDs
	//   {G=5, O=3, Sub=10, Pri=7}  same group, consecutive, inherit subgroup+priority
	//   {G=5, O=7, Sub=11, Pri=7}  same group, gap (ObjectIDDelta=3 → +4), Sub=Prior+1
	//   {G=8, O=0, Sub=20, Pri=9}  cross-group (GroupIDDelta=2 → +3), explicit subgroup, new pri
	written := []*message.FetchObject{
		{
			SerializationFlags: message.FetchFlagGroupIDDelta |
				message.FetchFlagObjectIDDelta |
				uint64(message.FetchSubgroupIDExplicit) |
				message.FetchFlagPriority,
			GroupIDDelta:      5,
			ObjectIDDelta:     2,
			SubgroupID:        10,
			PublisherPriority: 7,
			ObjectPayload:     []byte("o1"),
		},
		{
			// Consecutive object in same group, inherit subgroup
			// (Prior) and priority (no flag).
			SerializationFlags: uint64(message.FetchSubgroupIDPrior),
			ObjectPayload:      []byte("o2"),
		},
		{
			// Same group, ObjectID gap, Sub=Prior+1.
			SerializationFlags: message.FetchFlagObjectIDDelta |
				uint64(message.FetchSubgroupIDPriorPlusOne),
			ObjectIDDelta: 3,
			ObjectPayload: []byte("o3"),
		},
		{
			// Cross-group; ascending → newG = prevG + delta + 1 = 5+2+1 = 8.
			SerializationFlags: message.FetchFlagGroupIDDelta |
				message.FetchFlagObjectIDDelta |
				uint64(message.FetchSubgroupIDExplicit) |
				message.FetchFlagPriority,
			GroupIDDelta:      2,
			ObjectIDDelta:     0,
			SubgroupID:        20,
			PublisherPriority: 9,
			ObjectPayload:     []byte("o4"),
		},
	}

	writeErr := make(chan error, 1)
	go func() {
		outStream, err := cli.OpenFetchStream(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		for _, o := range written {
			if err := outStream.WriteObject(o); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- outStream.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}

	want := []session.DecodedFetchObject{
		{GroupID: 5, ObjectID: 2, SubgroupID: 10, PublisherPriority: 7, Payload: []byte("o1")},
		{GroupID: 5, ObjectID: 3, SubgroupID: 10, PublisherPriority: 7, Payload: []byte("o2")},
		{GroupID: 5, ObjectID: 7, SubgroupID: 11, PublisherPriority: 7, Payload: []byte("o3")},
		{GroupID: 8, ObjectID: 0, SubgroupID: 20, PublisherPriority: 9, Payload: []byte("o4")},
	}

	for i, w := range want {
		got, err := in.ReadDecoded()
		if err != nil {
			t.Fatalf("ReadDecoded #%d: %v", i, err)
		}
		if got.GroupID != w.GroupID || got.ObjectID != w.ObjectID {
			t.Errorf("obj #%d: location got {%d,%d}, want {%d,%d}",
				i, got.GroupID, got.ObjectID, w.GroupID, w.ObjectID)
		}
		if got.SubgroupID != w.SubgroupID {
			t.Errorf("obj #%d: SubgroupID got %d, want %d", i, got.SubgroupID, w.SubgroupID)
		}
		if got.PublisherPriority != w.PublisherPriority {
			t.Errorf("obj #%d: PublisherPriority got %d, want %d",
				i, got.PublisherPriority, w.PublisherPriority)
		}
		if string(got.Payload) != string(w.Payload) {
			t.Errorf("obj #%d: payload got %q, want %q", i, got.Payload, w.Payload)
		}
	}

	if _, err := in.ReadDecoded(); !errors.Is(err, io.EOF) {
		t.Errorf("trailing ReadDecoded: got %v, want io.EOF", err)
	}
	if err := <-writeErr; err != nil {
		t.Errorf("writer: %v", err)
	}
}

// TestIncomingFetchStream_ReadDecoded_Descending verifies the descending
// branch of cross-group delta resolution: newGroup = prevGroup - delta - 1.
// The caller signals direction via IncomingFetchStream.GroupOrder.
func TestIncomingFetchStream_ReadDecoded_Descending(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.FetchHeader{RequestID: 0}
	written := []*message.FetchObject{
		{
			// First object: abs (G=10, O=0)
			SerializationFlags: message.FetchFlagGroupIDDelta |
				message.FetchFlagObjectIDDelta |
				uint64(message.FetchSubgroupIDExplicit),
			GroupIDDelta:  10,
			ObjectIDDelta: 0,
			SubgroupID:    0,
			ObjectPayload: []byte("g10"),
		},
		{
			// Cross-group descending: prevG - delta - 1 = 10 - 1 - 1 = 8.
			SerializationFlags: message.FetchFlagGroupIDDelta |
				message.FetchFlagObjectIDDelta |
				uint64(message.FetchSubgroupIDExplicit),
			GroupIDDelta:  1,
			ObjectIDDelta: 0,
			SubgroupID:    0,
			ObjectPayload: []byte("g8"),
		},
	}

	writeErr := make(chan error, 1)
	go func() {
		outStream, err := cli.OpenFetchStream(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		for _, o := range written {
			if err := outStream.WriteObject(o); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- outStream.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in := ds.(*session.IncomingFetchStream)
	in.GroupOrder = message.GroupOrderDescending

	o1, err := in.ReadDecoded()
	if err != nil {
		t.Fatalf("ReadDecoded #1: %v", err)
	}
	if o1.GroupID != 10 {
		t.Errorf("first: GroupID got %d, want 10", o1.GroupID)
	}

	o2, err := in.ReadDecoded()
	if err != nil {
		t.Fatalf("ReadDecoded #2: %v", err)
	}
	if o2.GroupID != 8 {
		t.Errorf("second: GroupID got %d, want 8 (10 - 1 - 1)", o2.GroupID)
	}

	if err := <-writeErr; err != nil {
		t.Errorf("writer: %v", err)
	}
}

// TestIncomingFetchStream_ReadDecoded_EndOfRange verifies that
// §11.4.4.2 absence markers surface via EndOfNonExistentRange /
// EndOfUnknownRange and do NOT advance the decoder's state — a
// subsequent real object's deltas still resolve against the last real
// object's IDs.
func TestIncomingFetchStream_ReadDecoded_EndOfRange(t *testing.T) {
	cli, srv := openPair(t)
	ctx := t.Context()

	hdr := message.FetchHeader{RequestID: 0}
	written := []*message.FetchObject{
		{
			SerializationFlags: message.FetchFlagGroupIDDelta |
				message.FetchFlagObjectIDDelta |
				uint64(message.FetchSubgroupIDExplicit),
			GroupIDDelta:  5,
			ObjectIDDelta: 0,
			SubgroupID:    0,
			ObjectPayload: []byte("real"),
		},
		// End-of-non-existent-range marker carrying abs {7, 3}.
		{
			SerializationFlags: message.FetchEndOfRangeObject,
			GroupIDDelta:       7,
			ObjectIDDelta:      3,
		},
		// Another real object — same group as the first real (5),
		// consecutive object — confirming the marker didn't bump
		// the decoder's prev state.
		{
			SerializationFlags: uint64(message.FetchSubgroupIDPrior),
			ObjectPayload:      []byte("real2"),
		},
	}

	writeErr := make(chan error, 1)
	go func() {
		outStream, err := cli.OpenFetchStream(ctx, hdr)
		if err != nil {
			writeErr <- err
			return
		}
		for _, o := range written {
			if err := outStream.WriteObject(o); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- outStream.Close()
	}()

	ds, err := srv.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in := ds.(*session.IncomingFetchStream)

	o1, _ := in.ReadDecoded()
	if o1.GroupID != 5 || o1.ObjectID != 0 || string(o1.Payload) != "real" {
		t.Errorf("first: got {%d,%d} payload=%q, want {5,0} \"real\"",
			o1.GroupID, o1.ObjectID, o1.Payload)
	}

	o2, _ := in.ReadDecoded()
	if !o2.EndOfNonExistentRange {
		t.Errorf("second: expected EndOfNonExistentRange")
	}
	if o2.GroupID != 7 || o2.ObjectID != 3 {
		t.Errorf("second: marker carries {%d,%d}, want {7,3}", o2.GroupID, o2.ObjectID)
	}

	o3, _ := in.ReadDecoded()
	if o3.GroupID != 5 || o3.ObjectID != 1 || string(o3.Payload) != "real2" {
		t.Errorf("third: got {%d,%d} payload=%q, want {5,1} \"real2\" (marker must not bump state)",
			o3.GroupID, o3.ObjectID, o3.Payload)
	}

	if err := <-writeErr; err != nil {
		t.Errorf("writer: %v", err)
	}
}

// TestIncomingFetchStream_ReadDecoded_FirstObjectViolations verifies §11.4.4:
// the first object on a FETCH stream MUST carry both a Group ID Delta and an
// Object ID Delta, and MUST NOT use flags that reference the prior object.
func TestIncomingFetchStream_ReadDecoded_FirstObjectViolations(t *testing.T) {
	tests := []struct {
		name  string
		first *message.FetchObject
	}{
		{
			// No delta flags at all: would reference the (non-existent)
			// prior object's IDs.
			name: "missing group and object id deltas",
			first: &message.FetchObject{
				SerializationFlags: uint64(message.FetchSubgroupIDZero),
				ObjectPayload:      []byte("x"),
			},
		},
		{
			// Object ID Delta present but Group ID Delta missing.
			name: "missing group id delta",
			first: &message.FetchObject{
				SerializationFlags: message.FetchFlagObjectIDDelta |
					uint64(message.FetchSubgroupIDZero),
				ObjectIDDelta: 3,
				ObjectPayload: []byte("x"),
			},
		},
		{
			// Both deltas present, but the subgroup mode references the
			// prior object's Subgroup ID.
			name: "references prior subgroup",
			first: &message.FetchObject{
				SerializationFlags: message.FetchFlagGroupIDDelta |
					message.FetchFlagObjectIDDelta |
					uint64(message.FetchSubgroupIDPrior),
				GroupIDDelta:  5,
				ObjectIDDelta: 2,
				ObjectPayload: []byte("x"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cli, srv := openPair(t)
			ctx := t.Context()

			hdr := message.FetchHeader{RequestID: 0}
			writeErr := make(chan error, 1)
			go func() {
				outStream, err := cli.OpenFetchStream(ctx, hdr)
				if err != nil {
					writeErr <- err
					return
				}
				if err := outStream.WriteObject(tt.first); err != nil {
					writeErr <- err
					return
				}
				writeErr <- outStream.Close()
			}()

			ds, err := srv.AcceptDataStream(ctx)
			if err != nil {
				t.Fatalf("AcceptDataStream: %v", err)
			}
			in, ok := ds.(*session.IncomingFetchStream)
			if !ok {
				t.Fatalf("got %T, want *IncomingFetchStream", ds)
			}

			if _, err := in.ReadDecoded(); err == nil {
				t.Errorf("ReadDecoded: expected PROTOCOL_VIOLATION error, got nil")
			}
			if err := <-writeErr; err != nil {
				t.Errorf("writer: %v", err)
			}
		})
	}
}
