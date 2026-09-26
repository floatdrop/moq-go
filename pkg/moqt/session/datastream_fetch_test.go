package session_test

import (
	"errors"
	"fmt"
	"io"
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// sendFetch writes objs on a new fetch stream (Request ID 0) from from, closes
// it, and returns to's end with the writer's result. The writer runs in a
// goroutine because the test pipe is synchronous.
func sendFetch(
	t *testing.T,
	from, to *session.Session,
	objs ...*message.FetchObject,
) (*session.IncomingFetchStream, <-chan error) {
	t.Helper()
	writeErr := make(chan error, 1)
	go func() {
		out, err := from.OpenFetchStream(message.FetchHeader{RequestID: 0})
		if err != nil {
			writeErr <- err
			return
		}
		for _, o := range objs {
			if err := out.WriteObject(o); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- out.Close()
	}()
	ds, err := to.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T, want *session.IncomingFetchStream", ds)
	}
	return in, writeErr
}

// TestFetchObjectRoundTrip: a FetchObject written with WriteObject reads back
// unchanged via AcceptDataStream + ReadObject.
func TestFetchObjectRoundTrip(t *testing.T) {
	cli, srv := openPair(t)
	obj := &message.FetchObject{
		SerializationFlags: message.FetchFlagGroupIDDelta | message.FetchFlagObjectIDDelta |
			message.FetchFlagPriority,
		GroupIDDelta:  3,
		ObjectIDDelta: 1,
		ObjectPayload: []byte("fetch-payload"),
	}
	in, writeErr := sendFetch(t, cli, srv, obj)

	if in.Header.RequestID != 0 {
		t.Errorf("RequestID: got %d, want 0", in.Header.RequestID)
	}
	got, err := in.ReadObject()
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
		t.Errorf("writer: %v", err)
	}
}

// TestIncomingFetchStream_ReadDecoded exercises the session-layer §11.4.4
// delta decoder: first-object absolute, same-group and cross-group transitions
// in both group orders, the subgroup modes, and End of Range markers.
func TestIncomingFetchStream_ReadDecoded(t *testing.T) {
	tests := []struct {
		name    string
		order   message.GroupOrder
		written []*message.FetchObject
		want    []session.DecodedFetchObject
	}{
		{
			// Mirrors what the relay's streamFetchObjects writes for an
			// ascending FETCH response. §11.4.4.1: without a Group ID Delta
			// "the Object ID is the prior Object's ID plus the Object ID
			// Delta" — no +1, unlike the subgroup rule — and an absent Object
			// ID Delta means "the prior Object's ID plus one, regardless of
			// which group it belongs to".
			name: "Ascending",
			written: []*message.FetchObject{
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
					// Same group, delta absent → 2+1; inherits subgroup (Prior)
					// and priority (no flag).
					SerializationFlags: uint64(message.FetchSubgroupIDPrior),
					ObjectPayload:      []byte("o2"),
				},
				{
					// Same group, ObjectIDDelta=3 → 3+3, Sub=Prior+1.
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
				{
					// Cross-group (+1) with the Object ID Delta omitted: prior ID + 1.
					SerializationFlags: message.FetchFlagGroupIDDelta |
						uint64(message.FetchSubgroupIDPrior),
					GroupIDDelta:  0,
					ObjectPayload: []byte("o5"),
				},
			},
			want: []session.DecodedFetchObject{
				{GroupID: 5, ObjectID: 2, SubgroupID: 10, PublisherPriority: 7, Payload: []byte("o1")},
				{GroupID: 5, ObjectID: 3, SubgroupID: 10, PublisherPriority: 7, Payload: []byte("o2")},
				{GroupID: 5, ObjectID: 6, SubgroupID: 11, PublisherPriority: 7, Payload: []byte("o3")},
				{GroupID: 8, ObjectID: 0, SubgroupID: 20, PublisherPriority: 9, Payload: []byte("o4")},
				{GroupID: 9, ObjectID: 1, SubgroupID: 20, PublisherPriority: 9, Payload: []byte("o5")},
			},
		},
		{
			// The caller signals direction via IncomingFetchStream.GroupOrder.
			name:  "Descending",
			order: message.GroupOrderDescending,
			written: []*message.FetchObject{
				{
					SerializationFlags: message.FetchFlagGroupIDDelta |
						message.FetchFlagObjectIDDelta |
						message.FetchFlagPriority |
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
			},
			want: []session.DecodedFetchObject{
				{GroupID: 10, ObjectID: 0, Payload: []byte("g10")},
				{GroupID: 8, ObjectID: 0, Payload: []byte("g8")},
			},
		},
		{
			// End of Range markers surface as EndOfNonExistentRange and become
			// the prior Group and Object ID; the prior Subgroup ID and Priority
			// stay the last Object's (§11.4.4.2).
			name: "EndOfRange",
			written: []*message.FetchObject{
				{
					SerializationFlags: message.FetchFlagGroupIDDelta |
						message.FetchFlagObjectIDDelta |
						message.FetchFlagPriority |
						uint64(message.FetchSubgroupIDExplicit),
					GroupIDDelta:      5,
					ObjectIDDelta:     0,
					SubgroupID:        9,
					PublisherPriority: 42,
					ObjectPayload:     []byte("real"),
				},
				// Marker carrying absolute {7, 3}.
				{
					SerializationFlags: message.FetchEndOfNonExistentRange,
					GroupIDDelta:       7,
					ObjectIDDelta:      3,
				},
				// No deltas: per §11.4.4.2 the prior Group/Object IDs are the
				// MARKER's, so it decodes as {7, 4}; its Subgroup (mode Prior) and
				// Priority (flag absent) come from the last ACTUAL object.
				{
					SerializationFlags: uint64(message.FetchSubgroupIDPrior),
					ObjectPayload:      []byte("real2"),
				},
			},
			want: []session.DecodedFetchObject{
				{GroupID: 5, ObjectID: 0, SubgroupID: 9, PublisherPriority: 42, Payload: []byte("real")},
				{GroupID: 7, ObjectID: 3, EndOfNonExistentRange: true},
				// The marker IS the prior (§11.4.4.2).
				{GroupID: 7, ObjectID: 4, SubgroupID: 9, PublisherPriority: 42, Payload: []byte("real2")},
			},
		},
	}

	view := func(d *session.DecodedFetchObject) string {
		return fmt.Sprintf("{G=%d O=%d Sub=%d Pri=%d payload=%q endOfNonExistentRange=%t}",
			d.GroupID, d.ObjectID, d.SubgroupID, d.PublisherPriority, d.Payload, d.EndOfNonExistentRange)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			in, writeErr := sendFetch(t, cli, srv, tc.written...)
			in.GroupOrder = tc.order

			for i := range tc.want {
				got, err := in.ReadDecoded()
				if err != nil {
					t.Fatalf("ReadDecoded #%d: %v", i, err)
				}
				if g, w := view(got), view(&tc.want[i]); g != w {
					t.Errorf("obj #%d: got %s, want %s", i, g, w)
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

// TestIncomingFetchStream_ReadDecoded_FirstObjectViolations: a first Object
// missing a delta, or referencing any prior-Object field, closes the session
// with PROTOCOL_VIOLATION (§11.4.4.1).
func TestIncomingFetchStream_ReadDecoded_FirstObjectViolations(t *testing.T) {
	tests := []struct {
		name  string
		first *message.FetchObject
	}{
		{
			name: "missing group and object id deltas",
			first: &message.FetchObject{
				SerializationFlags: uint64(message.FetchSubgroupIDZero),
				ObjectPayload:      []byte("x"),
			},
		},
		{
			name: "missing group id delta",
			first: &message.FetchObject{
				SerializationFlags: message.FetchFlagObjectIDDelta |
					uint64(message.FetchSubgroupIDZero),
				ObjectIDDelta: 3,
				ObjectPayload: []byte("x"),
			},
		},
		{
			name: "missing object id delta",
			first: &message.FetchObject{
				SerializationFlags: message.FetchFlagGroupIDDelta |
					message.FetchFlagPriority |
					uint64(message.FetchSubgroupIDZero),
				GroupIDDelta:  5,
				ObjectPayload: []byte("x"),
			},
		},
		{
			name: "references prior priority",
			first: &message.FetchObject{
				SerializationFlags: message.FetchFlagGroupIDDelta |
					message.FetchFlagObjectIDDelta |
					uint64(message.FetchSubgroupIDZero),
				GroupIDDelta:  5,
				ObjectIDDelta: 2,
				ObjectPayload: []byte("x"),
			},
		},
		{
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
			in, writeErr := sendFetch(t, cli, srv, tt.first)

			if _, err := in.ReadDecoded(); err == nil {
				t.Errorf("ReadDecoded: expected PROTOCOL_VIOLATION error, got nil")
			}
			requireClosedProtocolViolation(t, srv)
			if err := <-writeErr; err != nil {
				t.Errorf("writer: %v", err)
			}
		})
	}
}

// TestIncomingFetchStream_ReadDecoded_MarkerFirst pins the §11.4.4.2 rules
// when an End-of-Range marker is the FIRST element on the stream: the marker
// supplies the prior Group/Object IDs, so the following object may omit the
// deltas — but with no prior ACTUAL object it must not reference the prior
// Subgroup ID or Priority.
func TestIncomingFetchStream_ReadDecoded_MarkerFirst(t *testing.T) {
	marker := &message.FetchObject{
		SerializationFlags: message.FetchEndOfNonExistentRange,
		GroupIDDelta:       3, // absolute Group ID
		ObjectIDDelta:      6, // absolute Object ID
	}

	t.Run("object after leading marker uses it as prior", func(t *testing.T) {
		cli, srv := openPair(t)
		in, writeErr := sendFetch(t, cli, srv, marker, &message.FetchObject{
			// No deltas: prior = the marker → {3, 7}. Subgroup and priority
			// are spelled out (no prior actual object exists).
			SerializationFlags: message.FetchFlagPriority |
				uint64(message.FetchSubgroupIDExplicit),
			SubgroupID:        2,
			PublisherPriority: 5,
			ObjectPayload:     []byte("after"),
		})

		if m, err := in.ReadDecoded(); err != nil || !m.EndOfNonExistentRange {
			t.Fatalf("marker read: %v %+v", err, m)
		}
		obj, err := in.ReadDecoded()
		if err != nil {
			t.Fatalf("object after leading marker: %v", err)
		}
		if obj.GroupID != 3 || obj.ObjectID != 7 {
			t.Errorf("decoded {%d,%d}, want {3,7} (marker as prior)", obj.GroupID, obj.ObjectID)
		}
		if err := <-writeErr; err != nil {
			t.Errorf("writer: %v", err)
		}
	})

	violations := []struct {
		name string
		next *message.FetchObject
	}{
		{
			name: "prior-subgroup mode with no prior object is a violation",
			next: &message.FetchObject{
				SerializationFlags: message.FetchFlagPriority |
					uint64(message.FetchSubgroupIDPrior),
				PublisherPriority: 5,
				ObjectPayload:     []byte("bad"),
			},
		},
		{
			name: "absent priority with no prior object is a violation",
			next: &message.FetchObject{
				SerializationFlags: uint64(message.FetchSubgroupIDZero),
				ObjectPayload:      []byte("bad"),
			},
		},
	}
	for _, tc := range violations {
		t.Run(tc.name, func(t *testing.T) {
			cli, srv := openPair(t)
			in, writeErr := sendFetch(t, cli, srv, marker, tc.next)

			if _, err := in.ReadDecoded(); err != nil {
				t.Fatalf("marker read: %v", err)
			}
			if _, err := in.ReadDecoded(); err == nil {
				t.Error("expected a violation, got nil")
			}
			requireClosedProtocolViolation(t, srv)
			if err := <-writeErr; err != nil {
				t.Errorf("writer: %v", err)
			}
		})
	}
}
