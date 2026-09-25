package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §5.1.4: "When Length is 0, there is no filter and no further fields are
// present. This can be used in REQUEST_UPDATE to remove a filter." and "In
// REQUEST_UPDATE, Length of 0 removes the filter; non-zero replaces it
// entirely. If a filter parameter is omitted from REQUEST_UPDATE, it is
// unchanged."

// TestSubscribe_ZeroLengthRangeFilterIsNoFilter: on a SUBSCRIBE a zero-length
// Range Filter is no filter, not a malformed one.
func TestSubscribe_ZeroLengthRangeFilterIsNoFilter(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1Req(t, subSess, message.BytesParam(message.ParamObjectIDFilter, nil))
}

// TestRequestUpdate_ZeroLengthRemovesOneRangeFilterType: an update removing
// OBJECTID_FILTER lets every Object through again, while the SUBGROUP_FILTER it
// does not name still holds.
func TestRequestUpdate_ZeroLengthRemovesOneRangeFilterType(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess,
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 1, End: 1}},
		}),
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamSubgroupFilter, Ranges: []message.Range{{Start: 0, End: 0}},
		}),
	)
	if _, err := subSess.UpdateRequest(t.Context(), subReq,
		message.Parameters{message.BytesParam(message.ParamObjectIDFilter, nil)}); err != nil {
		t.Fatalf("UpdateRequest removing OBJECTID_FILTER: %v", err)
	}

	// Subgroup 0 of Group 0: three Objects, all of which now pass. Subgroup 1
	// (in Group 1, so its Object IDs do not collide): still outside the
	// SUBGROUP_FILTER.
	for _, sgID := range []uint64{0, 1} {
		sg, err := pub.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, GroupID: sgID, SubgroupID: sgID,
		})
		if err != nil {
			t.Fatalf("OpenSubgroup %d: %v", sgID, err)
		}
		go func() {
			for range 3 {
				if sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}) != nil {
					return
				}
			}
			_ = sg.Close()
		}()
	}

	acceptCtx, cancelAccept := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancelAccept()
	ds, err := subSess.AcceptDataStream(acceptCtx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v (no stream for subgroup 0)", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	if in.Header.SubgroupID != 0 {
		t.Fatalf("got subgroup %d; the SUBGROUP_FILTER the update did not name admits only 0", in.Header.SubgroupID)
	}
	n := 0
	for {
		if _, err := in.ReadObject(); err != nil {
			break
		}
		n++
	}
	if n != 3 {
		t.Fatalf("subgroup 0 carried %d Objects after OBJECTID_FILTER was removed, want 3", n)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if ds, err := subSess.AcceptDataStream(ctx); err == nil {
		t.Fatalf("got a second data stream (%T); subgroup 1 is outside the kept SUBGROUP_FILTER", ds)
	}
}

// TestRequestUpdate_RangeFilterLimitCountsMergedSet: MAX_FILTER_RANGES "limits
// the total number of Ranges allowed in all Range Filter parameters for a
// given subscription" (§5.1.4). After an update merges into the filters the
// subscription keeps, the limit applies to the merged set, not to the update
// alone.
func TestRequestUpdate_RangeFilterLimitCountsMergedSet(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{MaxFilterRanges: 2})
	defer teardown()
	publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subReq := subscribeCam1Req(t, subSess,
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamSubgroupFilter, Ranges: []message.Range{{Start: 0, End: 0}},
		}),
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamPriorityFilter, Ranges: []message.Range{{Start: 0, End: 10}},
		}),
	)
	// One range on its own, a third one merged.
	_, err := subSess.UpdateRequest(t.Context(), subReq, message.Parameters{
		message.RangeFilterParam(&message.RangeFilter{
			Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 1, End: 1}},
		}),
	})
	requireRejectedWithCode(t, err, moqt.RequestInvalidFilter)
}
