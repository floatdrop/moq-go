package relay_test

import (
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// The relay dedups Objects from redundant upstreams against the last 32
// Groups (§9.3). Past that it forwards an Object only above every one it
// forwarded in its Subgroup; one it drops leaves its stream missing an Object,
// so that stream is reset, not FINed (§11.4.3).

// readSubgroupsConcurrently reads every subgroup stream sess accepts to its
// end, each on its own goroutine, so an open stream does not hold up the
// others.
func readSubgroupsConcurrently(t *testing.T, sess *session.Session) <-chan subgroupRead {
	t.Helper()
	out := make(chan subgroupRead, 64)
	go func() {
		for {
			ds, err := sess.AcceptDataStream(t.Context())
			if err != nil {
				return
			}
			in, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				continue
			}
			go func() {
				r := subgroupRead{header: in.Header}
				for {
					o, err := in.ReadDecoded()
					if err != nil {
						r.end = err
						out <- r
						return
					}
					r.ids = append(r.ids, o.ObjectID)
				}
			}()
		}
	}()
	return out
}

// awaitGroupStream returns the first stream of group read from reads,
// failing after 5s.
func awaitGroupStream(t *testing.T, reads <-chan subgroupRead, group uint64) subgroupRead {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case r := <-reads:
			if r.header.GroupID == group {
				return r
			}
		case <-deadline:
			t.Fatalf("no stream of Group %d ended within 5s", group)
		}
	}
}

// writeOpenGroup0 opens Group 0's subgroup (END_OF_GROUP set, so its FIN also
// ends the Group) and writes Objects ids, leaving it open.
func writeOpenGroup0(
	t *testing.T,
	pubSess *session.Session,
	alias uint64,
	ids ...uint64,
) *session.OutgoingSubgroupStream {
	t.Helper()
	hdr := subgroupHeader(alias, 0)
	hdr.EndOfGroup = true
	sg, err := openSubgroupWaiting(t, pubSess, hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	writeIDs(t, sg, -1, ids...)
	return sg
}

// writeIDs writes Objects ids (ascending) on sg after Object prev, -1 for
// none (§11.4.2 delta encoding).
func writeIDs(t *testing.T, sg *session.OutgoingSubgroupStream, prev int64, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		delta := id
		if prev >= 0 {
			delta = id - uint64(prev) - 1
		}
		prev = int64(id)
		if err := sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: delta, Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObject(%d): %v", id, err)
		}
	}
}

// publishGroups publishes one Object in each of Groups 1..n, waiting for each
// to reach the subscriber.
func publishGroups(t *testing.T, pubSess *session.Session, alias uint64, reads <-chan subgroupRead, n uint64) {
	t.Helper()
	for g := uint64(1); g <= n; g++ {
		publishObjects(t, pubSess, alias, g, 1)
		awaitGroupStream(t, reads, g)
	}
}

// TestFanout_GroupIDJumpKeepsOpenSubgroup: Group IDs need not be consecutive
// (§2.3.1, §12.8), so a jump to Group 100 while Group 0's stream is still open
// does not age Group 0 out; its later Objects are forwarded.
func TestFanout_GroupIDJumpKeepsOpenSubgroup(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	reads := readSubgroupsConcurrently(t, subSess)

	sg := writeOpenGroup0(t, pubSess, alias, 0, 1)
	publishObjects(t, pubSess, alias, 100, 1)
	awaitGroupStream(t, reads, 100)
	writeIDs(t, sg, 1, 2)
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := awaitGroupStream(t, reads, 0)
	if !slices.Equal(r.ids, []uint64{0, 1, 2}) || !errors.Is(r.end, io.EOF) {
		t.Fatalf("Group 0 stream carried %v and ended with %v, want [0 1 2] and a FIN", r.ids, r.end)
	}
}

// TestFanout_AgedOutNextObjectForwarded: once 32 newer Groups push Group 0
// out of the dedup window, the next Object on its open stream is still
// forwarded: it is above every Object forwarded in its Subgroup, so it
// repeats none (§9.4: "MUST NOT reorder or drop objects received on a
// multi-object stream").
func TestFanout_AgedOutNextObjectForwarded(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	reads := readSubgroupsConcurrently(t, subSess)

	sg := writeOpenGroup0(t, pubSess, alias, 0, 1)
	publishGroups(t, pubSess, alias, reads, 32)
	writeIDs(t, sg, 1, 2)
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := awaitGroupStream(t, reads, 0)
	if !slices.Equal(r.ids, []uint64{0, 1, 2}) || !errors.Is(r.end, io.EOF) {
		t.Fatalf("Group 0 stream carried %v and ended with %v, want [0 1 2] and a FIN", r.ids, r.end)
	}
}

// TestFanout_AgedOutObjectResetsStream: an aged-out Object below one already
// forwarded in its Subgroup (here Object 1, from a second stream, after 0 and
// 2) may be a repeat the relay can no longer detect, so it is dropped; the
// stream missing it is reset rather than FINed, which with END_OF_GROUP would
// say it does not exist (§11.4.2, §11.4.3). A subscription whose Start
// Location is past the dropped Object was not owed it, so its stream FINs.
func TestFanout_AgedOutObjectResetsStream(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	reads := readSubgroupsConcurrently(t, subSess)
	lateSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, lateSess, message.AbsoluteStartFilter(message.Location{Group: 0, Object: 2}))
	lateReads := readSubgroupsConcurrently(t, lateSess)
	// Its OBJECTID_FILTER omits Object 1, which resets its stream as it would
	// had Object 1 been forwarded (§11.4.3).
	filteredSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, filteredSess, message.RangeFilterParam(&message.RangeFilter{
		Type: message.ParamObjectIDFilter, Ranges: []message.Range{{Start: 0, End: 0}, {Start: 2, End: 2}},
	}))
	filteredReads := readSubgroupsConcurrently(t, filteredSess)

	first := writeOpenGroup0(t, pubSess, alias, 0, 2)
	publishGroups(t, pubSess, alias, reads, 32)
	hdr := subgroupHeader(alias, 0)
	hdr.EndOfGroup, hdr.ReplayingSubgroup = true, true
	second, err := openSubgroupWaiting(t, pubSess, hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	writeIDs(t, second, -1, 1)
	for _, sg := range []*session.OutgoingSubgroupStream{second, first} {
		if err := sg.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	r := awaitGroupStream(t, reads, 0)
	if !slices.Equal(r.ids, []uint64{0, 2}) || errors.Is(r.end, io.EOF) {
		t.Fatalf("Group 0 stream carried %v and ended with %v, want [0 2] and a reset", r.ids, r.end)
	}
	late := awaitGroupStream(t, lateReads, 0)
	if !slices.Equal(late.ids, []uint64{2}) || !errors.Is(late.end, io.EOF) {
		t.Fatalf("Group 0 stream from {0, 2} carried %v and ended with %v, want [2] and a FIN", late.ids, late.end)
	}
	filtered := awaitGroupStream(t, filteredReads, 0)
	if !slices.Equal(filtered.ids, []uint64{0, 2}) || errors.Is(filtered.end, io.EOF) {
		t.Fatalf("filtered Group 0 stream carried %v and ended with %v, want [0 2] and a reset",
			filtered.ids, filtered.end)
	}
}

// TestFanout_AgedOutRepeatDropped: an aged-out Object this Subgroup already
// forwarded, here Objects 1 and 2 again from a second stream, is a redundant
// copy (§9.3): dropped without resetting the streams that got the first.
func TestFanout_AgedOutRepeatDropped(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	reads := readSubgroupsConcurrently(t, subSess)

	first := writeOpenGroup0(t, pubSess, alias, 0, 1, 2)
	publishGroups(t, pubSess, alias, reads, 32)
	hdr := subgroupHeader(alias, 0)
	hdr.EndOfGroup, hdr.ReplayingSubgroup = true, true
	second, err := openSubgroupWaiting(t, pubSess, hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	writeIDs(t, second, -1, 1, 2)
	if err := second.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	writeIDs(t, first, 2, 3)
	if err := first.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	r := awaitGroupStream(t, reads, 0)
	if !slices.Equal(r.ids, []uint64{0, 1, 2, 3}) || !errors.Is(r.end, io.EOF) {
		t.Fatalf("Group 0 stream carried %v and ended with %v, want [0 1 2 3] and a FIN", r.ids, r.end)
	}
}
