package relay_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// subgroupCapture is one outbound SUBGROUP_HEADER stream a subscriber
// received, with its decoded header and object IDs.
type subgroupCapture struct {
	Header  message.SubgroupHeader
	Objects []uint64
	Err     error // terminal read error (io.EOF on FIN)
}

// captureSubgroups accepts n subgroup streams on sess and reads each to
// completion.
func captureSubgroups(t *testing.T, sess *session.Session, n int) []subgroupCapture {
	t.Helper()
	ch := make(chan subgroupCapture, n)
	go func() {
		for range n {
			ds, err := sess.AcceptDataStream(t.Context())
			if err != nil {
				ch <- subgroupCapture{Err: err}
				return
			}
			sg, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				ch <- subgroupCapture{Err: errors.New("not a subgroup stream")}
				return
			}
			go func() {
				c := subgroupCapture{Header: sg.Header}
				for {
					obj, err := sg.ReadDecoded()
					if err != nil {
						c.Err = err
						ch <- c
						return
					}
					c.Objects = append(c.Objects, obj.ObjectID)
				}
			}()
		}
	}()
	out := make([]subgroupCapture, 0, n)
	deadline := time.After(5 * time.Second)
	for len(out) < n {
		select {
		case c := <-ch:
			out = append(out, c)
		case <-deadline:
			t.Fatalf("received %d/%d subgroup streams before deadline: %+v", len(out), n, out)
		}
	}
	return out
}

// firstObjectTopology wires publisher → relay → subscriber, with the
// subscriber's SUBSCRIBE carrying params. It returns the publisher's
// Publication (alias 7 registered) and the subscriber session.
func firstObjectTopology(
	t *testing.T,
	params message.Parameters,
) (pub *session.Publication, sub *session.Session) {
	t.Helper()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	pub, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		TrackAlias: 7,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { _ = pub.Close() })

	sub = dialAnotherClient(t, pubSess)
	subReq, err := sub.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  ns("video"),
		Name:       []byte("cam1"),
		Parameters: params,
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = subReq.Close() })
	return pub, sub
}

// writeSubgroupObjects opens one subgroup on the publisher and writes the
// given absolute object IDs (ascending), then FINs.
func writeSubgroupObjects(t *testing.T, pub *session.Publication, hdr message.SubgroupHeader, ids []uint64) {
	t.Helper()
	sg, err := pub.OpenSubgroup(hdr)
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	prev, has := uint64(0), false
	for _, id := range ids {
		obj := &message.SubgroupObject{Payload: []byte{byte('a' + id)}}
		if !has {
			obj.ObjectIDDelta = id
		} else {
			obj.ObjectIDDelta = id - prev - 1
		}
		if err := sg.WriteObject(obj); err != nil {
			t.Fatalf("WriteObject(%d): %v", id, err)
		}
		prev, has = id, true
	}
	if err := sg.Close(); err != nil {
		t.Fatalf("subgroup Close: %v", err)
	}
}

// TestFanout_FirstObjectBitOnPlainForward pins the §11.4.2 baseline: a
// forwarded subgroup that begins with the subgroup's true first object
// carries the FIRST_OBJECT bit (ReplayingSubgroup false).
func TestFanout_FirstObjectBitOnPlainForward(t *testing.T) {
	t.Parallel()
	pub, sub := firstObjectTopology(t, nil)
	writeSubgroupObjects(t, pub, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: 7, GroupID: 0,
	}, []uint64{0, 1})

	caps := captureSubgroups(t, sub, 1)
	c := caps[0]
	if c.Header.ReplayingSubgroup {
		t.Error("forwarded subgroup starting at the true first object must set FIRST_OBJECT")
	}
	if len(c.Objects) != 2 || c.Objects[0] != 0 {
		t.Errorf("objects = %v, want [0 1]", c.Objects)
	}
}

// TestFanout_FirstObjectBitClearedForFilteredHead: a stream starting
// mid-subgroup because of the filter does not claim FIRST_OBJECT.
func TestFanout_FirstObjectBitClearedForFilteredHead(t *testing.T) {
	t.Parallel()
	filter := &message.LocationFilter{Fields: 2, StartGroup: 0, StartObject: 2}
	pub, sub := firstObjectTopology(t,
		message.Parameters{message.LocationFilterParam(filter)})

	writeSubgroupObjects(t, pub, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: 7, GroupID: 0,
	}, []uint64{0, 1, 2, 3})

	caps := captureSubgroups(t, sub, 1)
	c := caps[0]
	if !c.Header.ReplayingSubgroup {
		t.Error("stream starting at a filtered-forward object must clear FIRST_OBJECT")
	}
	if len(c.Objects) == 0 || c.Objects[0] != 2 {
		t.Errorf("objects = %v, want [2 3]", c.Objects)
	}
}

// TestFanout_FirstObjectBitNotInvented: an inbound replay stream (FIRST_OBJECT
// clear) is not forwarded with the bit set.
func TestFanout_FirstObjectBitNotInvented(t *testing.T) {
	t.Parallel()
	pub, sub := firstObjectTopology(t, nil)
	writeSubgroupObjects(t, pub, message.SubgroupHeader{
		SubgroupIDMode:    message.SubgroupIDImplicitZero,
		TrackAlias:        7,
		GroupID:           0,
		ReplayingSubgroup: true, // upstream itself is replaying
	}, []uint64{4, 5})

	caps := captureSubgroups(t, sub, 1)
	if !caps[0].Header.ReplayingSubgroup {
		t.Error("relay invented FIRST_OBJECT for a subgroup the upstream marked as a replay")
	}
	if errors.Is(caps[0].Err, io.EOF) == false {
		t.Errorf("stream end: %v, want io.EOF", caps[0].Err)
	}
}

// TestFanout_ResolvesImplicitFirstObjectSubgroupID: a mode-0b01 header's
// Subgroup ID comes from its first Object (§11.4.2), in the forwarded header
// and in the cache a FETCH serves.
func TestFanout_ResolvesImplicitFirstObjectSubgroupID(t *testing.T) {
	t.Parallel()
	pub, sub := firstObjectTopology(t, nil)
	writeSubgroupObjects(t, pub, message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitFirstObject,
		TrackAlias:     7,
		GroupID:        0,
	}, []uint64{5, 6})

	caps := captureSubgroups(t, sub, 1)
	c := caps[0]
	if c.Header.SubgroupIDMode != message.SubgroupIDExplicit || c.Header.SubgroupID != 5 {
		t.Errorf("forwarded header mode=%v id=%d, want explicit Subgroup ID 5",
			c.Header.SubgroupIDMode, c.Header.SubgroupID)
	}
	if len(c.Objects) != 2 || c.Objects[0] != 5 {
		t.Errorf("objects = %v, want [5 6]", c.Objects)
	}

	// The cache must file the objects under subgroup 5 too: FETCH the range
	// back and check the decoded Subgroup IDs.
	fetchReq, err := sub.Fetch(t.Context(), &message.Fetch{
		Namespace: ns("video"),
		Name:      []byte("cam1"),
		Parameters: message.Parameters{
			fetchRangeFilter(message.Location{}, message.Location{Group: 0, Object: 6}),
		},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fetchReq.Close()

	ds, err := sub.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream returned %T", ds)
	}
	seen := 0
	for {
		o, err := fs.ReadDecoded()
		if err != nil {
			break // io.EOF on FIN
		}
		if o.IsEndOfRange() {
			continue
		}
		if o.SubgroupID != 5 {
			t.Errorf("FETCH object {%d,%d} has SubgroupID %d, want 5",
				o.GroupID, o.ObjectID, o.SubgroupID)
		}
		seen++
	}
	if seen != 2 {
		t.Errorf("FETCH returned %d objects, want 2", seen)
	}
}

// TestFanout_ImplicitFirstObjectEdgeStreams: a terminal-status first Object on
// a mode-0b01 stream still enforces §11.4.3, and an empty 0b01 stream leaves no
// state behind.
func TestFanout_ImplicitFirstObjectEdgeStreams(t *testing.T) {
	t.Parallel()
	pub, sub := firstObjectTopology(t, nil)

	// An empty 0b01 stream: FIN before any object.
	empty, err := pub.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitFirstObject,
		TrackAlias:     7,
		GroupID:        0,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup(empty): %v", err)
	}
	if err := empty.Close(); err != nil {
		t.Fatalf("empty Close: %v", err)
	}

	// A 0b01 stream whose FIRST object is an EndOfGroup marker (empty
	// payload, status 0x3) at ID 4, followed by a §11.4.3-violating second
	// object. The marker must resolve the Subgroup ID (4), be forwarded,
	// and set the terminal latch so the violation resets the stream.
	term, err := pub.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitFirstObject,
		TrackAlias:     7,
		GroupID:        1,
	})
	if err != nil {
		t.Fatalf("OpenSubgroup(terminal): %v", err)
	}
	if err := term.WriteObject(&message.SubgroupObject{
		ObjectIDDelta: 4, // absolute: first object
		ObjectStatus:  message.ObjectStatusEndOfGroup,
	}); err != nil {
		t.Fatalf("write terminal marker: %v", err)
	}
	// The subscriber reads the forwarded marker before the violation is
	// sent: the relay ends the whole track over it (§2.4.2), resetting the
	// stream, so a marker still queued then would never arrive.
	ds, err := sub.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	in, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	if in.Header.GroupID != 1 || in.Header.SubgroupID != 4 ||
		in.Header.SubgroupIDMode != message.SubgroupIDExplicit {
		t.Errorf("header = group %d subgroup %d mode %v, want group 1, explicit subgroup 4",
			in.Header.GroupID, in.Header.SubgroupID, in.Header.SubgroupIDMode)
	}
	if obj, err := in.ReadDecoded(); err != nil || obj.ObjectID != 4 {
		t.Fatalf("first forwarded Object = %+v, %v; want the terminal marker at 4", obj, err)
	}

	// §11.4.3 violation: an object after the terminal marker. The relay may
	// reset the inbound stream while this write is in flight; an error here
	// is acceptable.
	_ = term.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("x")})

	obj, err := in.ReadDecoded()
	switch {
	case err == nil:
		t.Errorf("forwarded Object %d after the terminal marker", obj.ObjectID)
	case errors.Is(err, io.EOF):
		t.Error("outbound stream FIN'd; a post-terminal violation must reset it")
	}
}
