package relay_test

import (
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §11.4.3: the relay forwards an Object on an existing subgroup stream only if
// it is "the next Object", and otherwise resets the stream and opens another.

// collectStreams reads events until objects Objects have arrived and every
// stream they came on has ended.
func collectStreams(t *testing.T, events <-chan objEvent, objects int) []objEvent {
	t.Helper()
	var (
		seen    []objEvent
		got     int
		streams int
		ended   int
	)
	deadline := time.After(3 * time.Second)
	for got < objects || ended < streams {
		select {
		case ev := <-events:
			if ev.stream == 0 {
				t.Fatalf("AcceptDataStream: %v", ev.err)
			}
			seen = append(seen, ev)
			streams = max(streams, ev.stream)
			if ev.err != nil {
				ended++
			} else {
				got++
			}
		case <-deadline:
			t.Fatalf("got %d/%d Objects, %d/%d streams ended: %v", got, objects, ended, streams, layout(seen))
		}
	}
	return seen
}

// finned reports whether stream ended with a FIN in events.
func finned(events []objEvent, stream int) bool {
	for _, ev := range events {
		if ev.stream == stream && ev.err != nil {
			return errors.Is(ev.err, io.EOF)
		}
	}
	return false
}

// TestFanout_NextObject_GapOnOneUpstreamStaysOnStream: Objects read in turn
// from one upstream stream are each the next Object (§11.4.3, second rule),
// so a publisher skipping IDs costs no reset.
func TestFanout_NextObject_GapOnOneUpstreamStaysOnStream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess)
	events := make(chan objEvent, 16)
	go readSubgroups(t.Context(), subSess, events)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for _, id := range []uint64{0, 2, 5} {
		if err := sg.WriteObjectAt(id, &message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObjectAt %d: %v", id, err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seen := collectStreams(t, events, 3)
	if got, want := layout(seen), [][]uint64{{0, 2, 5}}; !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("streams = %v, want %v", got, want)
	}
	if !finned(seen, 1) {
		t.Error("stream ended with a reset, want a FIN: no Object was omitted")
	}
}

// TestFanout_NextObject_FilteredObjectsBetween: Objects between two forwarded
// ones that did not pass the subscriber's filters keep the second the next
// Object (§11.4.3).
func TestFanout_NextObject_FilteredObjectsBetween(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	pub := publishVideoTrack(t, pubSess, "cam1", 1)
	subSess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, subSess, message.RangeFilterParam(&message.RangeFilter{
		Type:   message.ParamObjectIDFilter,
		Ranges: []message.Range{{Start: 0, End: 0}, {Start: 2, End: 3}},
	}))
	events := make(chan objEvent, 16)
	go readSubgroups(t.Context(), subSess, events)

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit})
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	for range 4 {
		if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
			t.Fatalf("WriteObject: %v", err)
		}
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	seen := collectStreams(t, events, 3)
	if got, want := layout(seen), [][]uint64{{0, 2, 3}}; !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("streams = %v, want %v", got, want)
	}
	if finned(seen, 1) {
		t.Error("stream ended with a FIN, want a reset: Object 1 was omitted")
	}
}

// twoPublishers PUBLISHes video/cam1 from two sessions, subscribes a third, and
// opens Subgroup 0 of Group 0, with Object Properties, from each publisher.
func twoPublishers(t *testing.T) (a, b *session.OutgoingSubgroupStream, events <-chan objEvent) {
	t.Helper()
	pubA, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	pubB := dialAnotherClient(t, pubA)
	subSess := dialAnotherClient(t, pubA)
	aPub := publishVideoTrack(t, pubA, "cam1", 1)
	bPub := publishVideoTrack(t, pubB, "cam1", 2)
	subscribeCam1(t, subSess)
	ch := make(chan objEvent, 16)
	go readSubgroups(t.Context(), subSess, ch)

	// Properties on both: the merged stream takes the first one's header.
	hdr := message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDExplicit, Properties: true}
	a, err := aPub.OpenSubgroup(hdr)
	if err != nil {
		t.Fatalf("A OpenSubgroup: %v", err)
	}
	b, err = bPub.OpenSubgroup(hdr)
	if err != nil {
		t.Fatalf("B OpenSubgroup: %v", err)
	}
	return a, b, ch
}

// writeAndAwait writes the Object at id on sg and waits for the subscriber to
// receive it, so the next write is ordered after it at the relay.
func writeAndAwait(
	t *testing.T,
	sg *session.OutgoingSubgroupStream,
	id uint64,
	props []byte,
	events <-chan objEvent,
	streams *[]objEvent,
) {
	t.Helper()
	if err := sg.WriteObjectAt(id, &message.SubgroupObject{Properties: props, Payload: []byte("x")}); err != nil {
		t.Fatalf("WriteObjectAt %d: %v", id, err)
	}
	for {
		select {
		case ev := <-events:
			*streams = append(*streams, ev)
			if ev.err == nil && ev.absID == id {
				return
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Object %d not forwarded", id)
		}
	}
}

// layout is the Object IDs of each stream in events.
func layout(events []objEvent) [][]uint64 {
	var out [][]uint64
	for _, ev := range events {
		if ev.err != nil {
			continue
		}
		for len(out) < ev.stream {
			out = append(out, nil)
		}
		out[ev.stream-1] = append(out[ev.stream-1], ev.absID)
	}
	return out
}

func priorObjectIDGap(gap uint64) []byte {
	return message.AppendTrackProperties([]wire.KVPair{{Type: message.PropertyPriorObjectIDGap, IntVal: gap}})
}

// TestFanout_NextObject_AcrossUpstreams: with two upstreams feeding one
// Subgroup (§9.3), an Object from the other upstream after a gap is the next
// Object only if its Prior Object ID Gap (§12.9) covers the gap (§11.4.3,
// third rule).
func TestFanout_NextObject_AcrossUpstreams(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
		want  [][]uint64
	}{
		{"no gap property", nil, [][]uint64{{0}, {3}}},
		{"gap short of the previous Object", priorObjectIDGap(1), [][]uint64{{0}, {3}}},
		{"gap reaching the previous Object", priorObjectIDGap(2), [][]uint64{{0, 3}}},
		// A gap covering an Object received before is not trusted. §12.9
		// makes that a malformed track, which the relay does not detect.
		{"gap covering the previous Object", priorObjectIDGap(3), [][]uint64{{0}, {3}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			a, b, events := twoPublishers(t)
			var seen []objEvent
			writeAndAwait(t, a, 0, nil, events, &seen)
			writeAndAwait(t, b, 3, tc.props, events, &seen)
			if got := layout(seen); !slices.EqualFunc(got, tc.want, slices.Equal) {
				t.Fatalf("streams = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestFanout_NextObject_DuplicateBetweenBreaksRun: an upstream's Object that
// lost the §2.1 dedup claim was not sent on the downstream stream, so the
// upstream's following Object is not the next Object after its earlier one
// (§11.4.3: "with no other Objects in between").
func TestFanout_NextObject_DuplicateBetweenBreaksRun(t *testing.T) {
	t.Parallel()
	a, b, events := twoPublishers(t)
	var seen []objEvent
	writeAndAwait(t, a, 1, nil, events, &seen)
	writeAndAwait(t, b, 0, nil, events, &seen)
	// B's 1 duplicates A's and is dropped.
	if err := b.WriteObjectAt(1, &message.SubgroupObject{Payload: []byte("x")}); err != nil {
		t.Fatalf("WriteObjectAt 1: %v", err)
	}
	writeAndAwait(t, b, 3, nil, events, &seen)
	if got, want := layout(seen), [][]uint64{{1}, {0}, {3}}; !slices.EqualFunc(got, want, slices.Equal) {
		t.Fatalf("streams = %v, want %v", got, want)
	}
}
