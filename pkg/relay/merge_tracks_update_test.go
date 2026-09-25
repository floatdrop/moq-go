package relay

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestMergeTracksUpdate: a type the update names replaces every stored
// parameter of that type; other types are kept (§10.9); a zero-length Range
// Filter removes that filter type (§5.1.4); TRACK_NAMESPACE_PREFIX and
// AUTHORIZATION_TOKEN are not kept.
func TestMergeTracksUpdate(t *testing.T) {
	objIDs := func(start uint64) message.Parameter {
		return message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: start, End: start}},
		})
	}
	prio := func(start uint64) message.Parameter {
		return message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamPriorityFilter, Ranges: []message.Range{{Start: start, End: start}},
		})
	}
	stored := message.Parameters{message.ForwardParam(true), objIDs(1), prio(2)}
	got := mergeTracksUpdate(stored, message.Parameters{
		message.ForwardParam(false),
		message.BytesParam(message.ParamObjectIDFilter, nil),
		message.TrackNamespacePrefixParam(wire.TrackNamespace{[]byte("x")}),
		message.BytesParam(message.ParamAuthorizationToken, []byte{0}),
	})
	want := message.Parameters{prio(2), message.ForwardParam(false)}
	if len(got) != len(want) {
		t.Fatalf("merged = %+v, want %+v", got, want)
	}
	for i := range want {
		if got[i].Type != want[i].Type || got[i].Byte != want[i].Byte || string(got[i].Bytes) != string(want[i].Bytes) {
			t.Fatalf("merged[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
	if len(stored) != 3 || stored[0].Byte != 1 {
		t.Fatalf("merge modified the stored parameters: %+v", stored)
	}
}
