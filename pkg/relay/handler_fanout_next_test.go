package relay

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// forwarded is one Object as the subscriber received it, or with end set, the
// end of the stream it came on.
type forwarded struct {
	stream int // 1-based index of the subgroup stream it came on
	id     uint64
	replay bool  // the stream's header had FIRST_OBJECT clear (§11.4.2)
	end    error // io.EOF for a FIN
}

// readForwarded emits every Object and stream end of every subgroup stream srv
// accepts.
func readForwarded(ctx context.Context, srv *session.Session) <-chan forwarded {
	out := make(chan forwarded, 16)
	go func() {
		for stream := 1; ; stream++ {
			ds, err := srv.AcceptDataStream(ctx)
			if err != nil {
				return
			}
			sg, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				return
			}
			for {
				if _, err := sg.ReadObject(); err != nil {
					out <- forwarded{stream: stream, end: err}
					break
				}
				out <- forwarded{stream: stream, id: sg.ObjectID(), replay: sg.Header.ReplayingSubgroup}
			}
		}
	}()
	return out
}

// awaitForwarded waits for the next Object or stream end the subscriber
// receives.
func awaitForwarded(t *testing.T, in <-chan forwarded) forwarded {
	t.Helper()
	select {
	case f := <-in:
		return f
	case <-time.After(2 * time.Second):
		t.Fatal("nothing forwarded")
		return forwarded{}
	}
}

// TestSubgroupWriter_RelayDropBreaksRun: an Object the relay dropped was not
// sent, so the upstream's next Object is not "the next Object" after the one
// before it (§11.4.3: "with no other Objects in between"). It goes on a new
// stream, without FIRST_OBJECT (§11.4.2), and both streams end with a reset,
// since an Object was omitted.
func TestSubgroupWriter_RelayDropBreaksRun(t *testing.T) {
	t.Parallel()
	src := new(session.IncomingSubgroupStream) // identity only
	for _, tc := range []struct {
		name string
		// Objects 0, 1 and 2 are queued before the writer starts; 2 overflows
		// a queue of 2, and expires with a maxAge.
		queue  int
		maxAge time.Duration
		want   []forwarded // Objects then stream ends, in order
	}{
		{"nothing dropped", 3, 0, []forwarded{
			{stream: 1, id: 0}, {stream: 1, id: 1}, {stream: 1, id: 2}, {stream: 1, id: 4},
			{stream: 1, end: io.EOF},
		}},
		{"queue overflow", 2, 0, []forwarded{
			{stream: 1, id: 0}, {stream: 1, id: 1}, {stream: 1, end: errReset},
			{stream: 2, id: 4, replay: true}, {stream: 2, end: errReset},
		}},
		{"expired", 3, time.Nanosecond, []forwarded{
			{stream: 1, id: 0}, {stream: 1, id: 1}, {stream: 1, end: errReset},
			{stream: 2, id: 4, replay: true}, {stream: 2, end: errReset},
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cli, srv := sessiontest.NewSessionPair(t)
			got := readForwarded(t.Context(), srv)
			w := newWedgeableWriter(t, cli)
			w.inbox = make(chan fwdObject, tc.queue)
			offer := func(seq, id uint64, maxAge time.Duration) {
				take, follows := w.admit(inboundPos{src: src, seq: seq}, w.hdr, id, nil)
				if take {
					w.publish(fwdObject{
						obj:         &message.SubgroupObject{Payload: []byte("x")},
						absID:       id,
						first:       id == 0,
						follows:     follows,
						maxCacheAge: maxAge,
					})
				}
			}

			// Upstream Objects 0, 1, 2 and 4, read in turn.
			offer(1, 0, 0)
			offer(2, 1, 0)
			offer(3, 2, tc.maxAge)
			go w.run()
			var seen []forwarded
			for {
				f := awaitForwarded(t, got)
				seen = append(seen, f)
				if f.end == nil && f.id == 1 {
					break
				}
			}
			offer(4, 4, 0)
			for {
				f := awaitForwarded(t, got)
				seen = append(seen, f)
				if f.end == nil && f.id == 4 {
					break
				}
			}
			w.close(false, 0)
			joinOrFatal(t, w)
			seen = append(seen, awaitForwarded(t, got))

			for i := range seen {
				switch end := seen[i].end; {
				case errors.Is(end, io.EOF):
					seen[i].end = io.EOF
				case end != nil:
					seen[i].end = errReset
				}
			}
			if !slices.Equal(seen, tc.want) {
				t.Fatalf("forwarded %v,\nwant      %v", seen, tc.want)
			}
		})
	}
}

// errReset stands for any stream reset in [TestSubgroupWriter_RelayDropBreaksRun].
var errReset = errors.New("reset")
