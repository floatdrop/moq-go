package relay_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
)

// Helpers that read what the relay sends on data streams.

// awaitSubgroupObject reports whether the next data stream sess accepts within
// the deadline is a subgroup stream carrying an Object.
func awaitSubgroupObject(t *testing.T, sess *session.Session, within time.Duration) bool {
	t.Helper()
	got := make(chan bool, 1)
	go func() {
		ds, err := sess.AcceptDataStream(t.Context())
		if err != nil {
			got <- false
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			got <- false
			return
		}
		_, err = sg.ReadObject()
		got <- err == nil
	}()
	select {
	case ok := <-got:
		return ok
	case <-time.After(within):
		return false
	}
}

// awaitObjectOn reads the first Object of a subgroup stream on alias, skipping
// streams for other aliases and failing after 2s.
func awaitObjectOn(t *testing.T, sess *session.Session, alias uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			t.Fatalf("no Object delivered on alias %d: %v", alias, err)
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok || sg.Header.TrackAlias != alias {
			continue
		}
		if _, err := sg.ReadObject(); err != nil {
			t.Fatalf("ReadObject: %v", err)
		}
		return
	}
}

// tryAcceptDataStream waits up to d for a data stream, reporting whether one
// arrived.
func tryAcceptDataStream(t *testing.T, sess *session.Session, d time.Duration) (session.DataStream, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	ds, err := sess.AcceptDataStream(ctx)
	if err != nil {
		return nil, false
	}
	return ds, true
}

// subgroupRead is one subgroup stream as a subscriber read it to its end.
type subgroupRead struct {
	header   message.SubgroupHeader
	ids      []uint64 // absolute Object IDs (§11.4.2)
	payloads []string
	end      error // io.EOF for a FIN, otherwise a reset
	err      error // no subgroup stream was accepted
}

// readNextSubgroup reads the next subgroup stream sess accepts to its end, off
// the test goroutine so it can start before the Objects are published.
func readNextSubgroup(t *testing.T, sess *session.Session) <-chan subgroupRead {
	t.Helper()
	out := make(chan subgroupRead, 1)
	go func() {
		ds, err := sess.AcceptDataStream(t.Context())
		if err != nil {
			out <- subgroupRead{err: fmt.Errorf("AcceptDataStream: %w", err)}
			return
		}
		in, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			out <- subgroupRead{err: fmt.Errorf("AcceptDataStream = %T, want a subgroup stream", ds)}
			return
		}
		r := subgroupRead{header: in.Header}
		for {
			o, err := in.ReadDecoded()
			if err != nil {
				r.end = err
				out <- r
				return
			}
			r.ids = append(r.ids, o.ObjectID)
			r.payloads = append(r.payloads, string(o.Payload))
		}
	}()
	return out
}

// awaitSubgroupRead waits up to 5s for [readNextSubgroup]'s stream to end.
func awaitSubgroupRead(t *testing.T, reads <-chan subgroupRead) subgroupRead {
	t.Helper()
	select {
	case r := <-reads:
		if r.err != nil {
			t.Fatal(r.err)
		}
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("no subgroup stream reached its end within 5s")
		return subgroupRead{}
	}
}

// readUntilEnd reads the subscriber's next subgroup stream to its end and
// returns the Object IDs it carried and how it ended.
func readUntilEnd(t *testing.T, subSess *session.Session) (ids []uint64, end error) {
	t.Helper()
	r := awaitSubgroupRead(t, readNextSubgroup(t, subSess))
	return r.ids, r.end
}

// objEvent is one Object, or the end of a stream (or of accepting), as
// [readSubgroups] emits it.
type objEvent struct {
	stream int    // 1-based index of the outbound stream it arrived on
	absID  uint64 // §11.4.2 delta resolved to an absolute Object ID
	err    error  // non-nil marks a stream end (io.EOF = FIN, else reset) or accept error
}

// readSubgroups emits every Object of every subgroup stream sub accepts, with
// its absolute Object ID, and each stream's end as an event with err set
// (io.EOF for a FIN). It returns when AcceptDataStream fails.
func readSubgroups(ctx context.Context, sub *session.Session, out chan<- objEvent) {
	streamIdx := 0
	for {
		ds, err := sub.AcceptDataStream(ctx)
		if err != nil {
			out <- objEvent{err: err}
			return
		}
		sg, ok := ds.(*session.IncomingSubgroupStream)
		if !ok {
			continue
		}
		streamIdx++
		idx := streamIdx
		var (
			prev uint64
			have bool
		)
		for {
			obj, err := sg.ReadObject()
			if err != nil {
				out <- objEvent{stream: idx, err: err}
				break
			}
			var absID uint64
			if !have {
				absID = obj.ObjectIDDelta
				have = true
			} else {
				absID = prev + obj.ObjectIDDelta + 1
			}
			prev = absID
			out <- objEvent{stream: idx, absID: absID}
		}
	}
}

// drainAll reads and discards every data stream on sess until ctx ends, so the
// relay never blocks on an unread subscriber.
func drainAll(ctx context.Context, sess *session.Session) {
	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			return
		}
		switch s := ds.(type) {
		case *session.IncomingSubgroupStream:
			for {
				if _, err := s.ReadObject(); err != nil {
					break
				}
			}
		case *session.IncomingFetchStream:
			for {
				if _, err := s.ReadObject(); err != nil {
					break
				}
			}
		}
	}
}
