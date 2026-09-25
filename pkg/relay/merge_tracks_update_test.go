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

	// A filter type named in the update replaces every SetID of it.
	trackProp := func(set uint8, v uint64) message.Parameter {
		return message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamTrackPropertyFilter, SetID: set, PropertyType: 0x40,
			Ranges: []message.Range{{Start: v, End: v}},
		})
	}
	got = mergeTracksUpdate(message.Parameters{trackProp(0, 1), trackProp(1, 2)},
		message.Parameters{trackProp(2, 3)})
	set, err := message.RangeFiltersFromParams(got)
	if err != nil || len(got) != 1 || !set.MatchesTrack(message.AppendTrackProperties(
		[]wire.KVPair{{Type: 0x40, IntVal: 3}})) {
		t.Fatalf("merged filters = %+v (%v), want only SetID 2's", got, err)
	}
}
