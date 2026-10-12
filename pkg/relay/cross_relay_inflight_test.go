package relay_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// TestCrossRelay_FetchWaitsForObjectArrivingUpstream: relay B is receiving
// {0,2} from its publisher, its payload half sent. B reports it as Largest
// Object (§3.1.4), so relay A, subscribing through B, does too; a FETCH on A
// up to it stitches from B (§7.4), and B, which is receiving that Object,
// waits for it rather than marking it unknown (§3.2). The subscriber on A gets
// every Object, {0,2} in full.
func TestCrossRelay_FetchWaitsForObjectArrivingUpstream(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	ctx := t.Context()
	relayA, relayB := startRelayPair(ctx, store)
	defer relayA.stop(t)
	defer relayB.stop(t)

	pubSess, _ := publishOnRelay(t, relayB, "catalog", 7)
	sg, err := openSubgroupWaiting(t, pubSess, subgroupHeader(7, 0))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for range 2 {
		if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
	}
	if _, err := sg.Write([]byte{0x00, 0x04, 'p', 'q'}); err != nil { // {0,2}, half
		t.Fatalf("Write {0,2} header: %v", err)
	}
	waitRelayLargest(t, pubSess, ns("video"), []byte("catalog"), 0, 2)

	subSess := dialClient(t, relayA)
	subReq, err := subSess.Subscribe(ctx, &message.Subscribe{Namespace: ns("video"), Name: []byte("catalog")})
	if err != nil {
		t.Fatalf("cross-relay Subscribe: %v", err)
	}
	defer subReq.Close()
	if p, ok := subReq.OK.Parameters.Find(message.ParamLargestObject); !ok || p.Group != 0 || p.Object != 2 {
		t.Fatalf("A's SUBSCRIBE_OK LARGEST_OBJECT = %+v (present %t), want {0,2}", p, ok)
	}

	go func() {
		time.Sleep(200 * time.Millisecond)
		_, _ = sg.Write([]byte{'r', 's'})
		_ = sg.Close()
	}()
	fetchReq, err := subSess.Fetch(ctx, &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("catalog"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 2}),
			message.FillTimeoutParam(3 * time.Second),
		},
	})
	if err != nil {
		t.Fatalf("FETCH: %v", err)
	}
	defer fetchReq.Close()
	elems := readFetchResponse(t, subSess, message.GroupOrderAscending, 5*time.Second)
	if len(elems) != 3 {
		t.Fatalf("FETCH returned %d elements, want Objects {0,0}..{0,2}: %+v", len(elems), elems)
	}
	for i, e := range elems {
		if e.IsEndOfRange() || e.ObjectID != uint64(i) {
			t.Fatalf("element %d = %+v, want Object {0,%d}", i, e, i)
		}
	}
	if !bytes.Equal(elems[2].Payload, []byte("pqrs")) {
		t.Fatalf("{0,2} payload = %q, want %q", elems[2].Payload, "pqrs")
	}
}
