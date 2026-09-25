package relay_test

import (
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestSubscribe_FillInheritsRangeFilters pins §5.1.3: "The fill fetch stream
// inherits the subscription's parameters, including subscriber priority,
// range filters and authorization; parameters carried inside FILL_PARAMETERS
// override them for the fill fetch stream." A Range Filter inside
// FILL_PARAMETERS overrides the subscription's filter of the same type, as a
// REQUEST_UPDATE would (§5.1.4): non-zero replaces it, zero-length removes it.
func TestSubscribe_FillInheritsRangeFilters(t *testing.T) {
	t.Parallel()
	objectIDs := func(ranges ...message.Range) message.Parameter {
		return message.RangeFilterParam(&message.RangeFilter{Type: message.ParamObjectIDFilter, Ranges: ranges})
	}
	for _, tc := range []struct {
		name  string
		inner message.Parameters // besides the whole-track Location filter
		want  []decodedFetchObject
	}{
		{"inherited", nil, []decodedFetchObject{{group: 0, object: 1}}},
		{"overridden", message.Parameters{objectIDs(message.Range{Start: 2, End: 2})},
			[]decodedFetchObject{{group: 0, object: 2}}},
		{"removed", message.Parameters{message.BytesParam(message.ParamObjectIDFilter, nil)},
			[]decodedFetchObject{{group: 0, object: 0}, {group: 0, object: 1}, {group: 0, object: 2}, {group: 1, object: 0}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pubSess, _, alias := publishAndCache(t)
			publishObjects(t, pubSess, alias, 0, 3)
			publishObjects(t, pubSess, alias, 1, 1)
			time.Sleep(50 * time.Millisecond)

			subSess := dialAnotherClient(t, pubSess)
			inner := append(message.Parameters{message.UnfilteredFilter()}, tc.inner...)
			sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{
				Namespace: wire.TrackNamespace{[]byte("video")},
				Name:      []byte("cam1"),
				Parameters: message.Parameters{
					message.NextObjectFilter(),
					objectIDs(message.Range{Start: 1, End: 1}),
					message.FillParametersParam(inner),
				},
			})
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			t.Cleanup(func() { sub.Close() })

			ds, err := subSess.AcceptDataStream(t.Context())
			if err != nil {
				t.Fatalf("AcceptDataStream: %v", err)
			}
			fs, ok := ds.(*session.IncomingFetchStream)
			if !ok {
				t.Fatalf("got %T, want the fill stream", ds)
			}
			got := decodeFetchStream(t, fs, message.GroupOrderAscending)
			ids := func(objs []decodedFetchObject) [][2]uint64 {
				var out [][2]uint64
				for _, o := range objs {
					out = append(out, [2]uint64{o.group, o.object})
				}
				return out
			}
			if !slices.Equal(ids(got), ids(tc.want)) {
				t.Fatalf("fill delivered %v, want %v", ids(got), ids(tc.want))
			}
		})
	}
}
