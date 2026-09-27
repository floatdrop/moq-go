package session_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/sessiontest"
)

// drainDataStreams accepts sess's data streams for a second and reads each to
// its end, reporting per Group ID whether the stream ended cleanly (FIN) or
// not. A stream that has not ended a second later is missing from the report.
func drainDataStreams(t *testing.T, sess *session.Session) <-chan map[uint64]bool {
	t.Helper()
	out := make(chan map[uint64]bool, 1)
	go func() {
		var (
			mu    sync.Mutex
			clean = map[uint64]bool{}
			wg    sync.WaitGroup
		)
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		for {
			ds, err := sess.AcceptDataStream(ctx)
			if err != nil {
				break
			}
			sg, ok := ds.(*session.IncomingSubgroupStream)
			if !ok {
				continue
			}
			wg.Go(func() {
				for {
					if _, err := sg.ReadDecoded(); err != nil {
						mu.Lock()
						clean[sg.Header.GroupID] = errors.Is(err, io.EOF)
						mu.Unlock()
						return
					}
				}
			})
		}
		finished := make(chan struct{})
		go func() { wg.Wait(); close(finished) }()
		select {
		case <-finished:
		case <-time.After(time.Second):
		}
		mu.Lock()
		out <- maps.Clone(clean)
		mu.Unlock()
	}()
	return out
}

// publishDoneOn serves sub's broker and returns the PUBLISH_DONE it reads.
func publishDoneOn(t *testing.T, sub *session.Subscription) <-chan *message.PublishDone {
	t.Helper()
	got := make(chan *message.PublishDone, 1)
	b := sub.Broker()
	go func() {
		_ = b.Serve(t.Context(), func(m message.Message) bool {
			if d, ok := m.(*message.PublishDone); ok {
				got <- d
				return false
			}
			return true
		})
	}()
	return got
}

func subgroupHeader(group uint64) message.SubgroupHeader {
	return message.SubgroupHeader{SubgroupIDMode: message.SubgroupIDImplicitZero, GroupID: group}
}

// TestPublicationDoneResetsOpenSubgroups: Done resets the subgroups still open
// with CANCELLED, since PUBLISH_DONE MUST NOT be sent "until it has closed all
// streams it will ever open" (§10.12), leaves a FINished one alone, counts both
// in the Stream Count, and refuses further writes.
func TestPublicationDoneResetsOpenSubgroups(t *testing.T) {
	cli, srv := openPair(t)
	sub, pub := subscribePair(t, cli, srv)
	done := publishDoneOn(t, sub)
	streams := drainDataStreams(t, cli)

	open, err := pub.OpenSubgroup(subgroupHeader(1))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := open.WriteObject(&message.SubgroupObject{Payload: []byte("a")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	finished, err := pub.OpenSubgroup(subgroupHeader(2))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := finished.WriteObject(&message.SubgroupObject{Payload: []byte("b")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := finished.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if err := pub.Done(moqt.PublishDoneTrackEnded, ""); err != nil {
		t.Fatalf("Done: %v", err)
	}
	if err := open.WriteObject(
		&message.SubgroupObject{Payload: []byte("c")},
	); !errors.Is(
		err,
		session.ErrPublicationEnded,
	) {
		t.Errorf("WriteObject after Done = %v, want ErrPublicationEnded", err)
	}
	select {
	case d := <-done:
		if d.StreamCount != 2 {
			t.Errorf("PUBLISH_DONE Stream Count = %d, want 2", d.StreamCount)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH_DONE")
	}
	clean := <-streams
	if ok, seen := clean[1]; !seen || ok {
		t.Errorf("open subgroup: seen %v, ended cleanly %v; want reset", seen, ok)
	}
	if ok, seen := clean[2]; !seen || !ok {
		t.Errorf("finished subgroup: seen %v, ended cleanly %v; want its FIN", seen, ok)
	}
}

// TestPublicationDoneKeepsFINOfDeliveryTimeoutCopy: a subgroup FINished
// through the copy WithDeliveryTimeouts returns is as finished as one closed
// directly, so Done does not reset it: a publisher that delivered every Object
// "MUST close the stream with a FIN" (§11.4.3).
func TestPublicationDoneKeepsFINOfDeliveryTimeoutCopy(t *testing.T) {
	cli, srv := openPair(t)
	sub, pub := subscribePair(t, cli, srv)
	done := publishDoneOn(t, sub)
	streams := drainDataStreams(t, cli)

	sg, err := pub.OpenSubgroup(subgroupHeader(1))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	timed := sg.WithDeliveryTimeouts(message.DeliveryTimeouts{}, message.DeliveryTimeouts{})
	if err := timed.WriteObject(&message.SubgroupObject{Payload: []byte("a")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := timed.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if n := pub.OpenSubgroupsForTest(); n != 0 {
		t.Errorf("%d subgroups still tracked as open after their FIN; Done would reset them", n)
	}
	if ok, seen := (<-streams)[1]; !seen || !ok {
		t.Fatalf("subgroup: seen %v, ended cleanly %v; want its FIN", seen, ok)
	}
	if err := pub.Done(moqt.PublishDoneTrackEnded, ""); err != nil {
		t.Fatalf("Done: %v", err)
	}
	select {
	case d := <-done:
		if d.StreamCount != 1 {
			t.Errorf("PUBLISH_DONE Stream Count = %d, want 1", d.StreamCount)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH_DONE")
	}
}

// TestPublicationDoneStreamCountExact: however OpenSubgroup races Done, the
// Stream Count is "the number of data streams the publisher opened" (§10.12):
// every OpenSubgroup that returned a stream. The subscriber never sees more;
// it can see fewer, when Done's reset loses a header it already wrote, which
// §10.12 has subscribers allow for.
func TestPublicationDoneStreamCountExact(t *testing.T) {
	cli, srv := openPair(t)
	sub, pub := subscribePair(t, cli, srv)
	done := publishDoneOn(t, sub)
	streams := drainDataStreams(t, cli)

	var (
		mu     sync.Mutex
		opened uint64
		wg     sync.WaitGroup
	)
	start := make(chan struct{})
	for w := range uint64(8) {
		wg.Go(func() {
			<-start
			for i := uint64(0); ; i++ {
				sg, err := pub.OpenSubgroup(subgroupHeader(w*1000 + i))
				if errors.Is(err, session.ErrPublicationEnded) {
					return
				}
				if errors.Is(err, session.ErrNoStreamCredit) {
					continue // the subscriber has not read enough streams yet
				}
				if err != nil {
					t.Errorf("OpenSubgroup: %v", err)
					return
				}
				mu.Lock()
				opened++
				mu.Unlock()
				_ = sg.Close()
			}
		})
	}
	close(start)
	time.Sleep(5 * time.Millisecond)
	if err := pub.Done(moqt.PublishDoneTrackEnded, ""); err != nil {
		t.Fatalf("Done: %v", err)
	}
	wg.Wait()
	select {
	case d := <-done:
		if d.StreamCount != opened {
			t.Errorf("PUBLISH_DONE Stream Count = %d, but %d subgroups opened", d.StreamCount, opened)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH_DONE")
	}
	if got := uint64(len(<-streams)); got > opened {
		t.Errorf("subscriber saw %d subgroup streams, more than the %d opened", got, opened)
	}
}

// reliableRecConn hands out uni streams that implement
// [session.ReliableResetStream] and record, in order, each reliable boundary
// and reset they see.
type reliableRecConn struct {
	session.Conn

	mu     sync.Mutex
	events []string
}

func (c *reliableRecConn) OpenUniStream() (session.SendStream, error) {
	s, err := c.Conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	return &reliableRecStream{SendStream: s, c: c}, nil
}

func (c *reliableRecConn) record(e string) {
	c.mu.Lock()
	c.events = append(c.events, e)
	c.mu.Unlock()
}

type reliableRecStream struct {
	session.SendStream

	c *reliableRecConn
}

func (s *reliableRecStream) SetReliableBoundary() { s.c.record("reliable") }

func (s *reliableRecStream) CancelWrite(code uint64) {
	s.c.record("reset")
	s.SendStream.CancelWrite(code)
}

// TestPublicationDoneResetKeepsHeaderReliable: before Done resets an open
// subgroup it marks what was written as reliable, so over RESET_STREAM_AT the
// header still reaches the subscriber, which can then "accurately account for
// reset data streams when handling PUBLISH_DONE" (§11.4.3).
func TestPublicationDoneResetKeepsHeaderReliable(t *testing.T) {
	cliConn, rawSrv := sessiontest.NewConnPair()
	srvConn := &reliableRecConn{Conn: rawSrv}
	cli, srv := openSessions(t, cliConn, srvConn, nil, nil)
	sub, pub := subscribePair(t, cli, srv)
	done := publishDoneOn(t, sub)
	drainDataStreams(t, cli)

	sg, err := pub.OpenSubgroup(subgroupHeader(1))
	if err != nil {
		t.Fatalf("OpenSubgroup: %v", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("a")}); err != nil {
		t.Fatalf("WriteObject: %v", err)
	}
	if err := pub.Done(moqt.PublishDoneTrackEnded, ""); err != nil {
		t.Fatalf("Done: %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("no PUBLISH_DONE")
	}
	srvConn.mu.Lock()
	events := slices.Clone(srvConn.events)
	srvConn.mu.Unlock()
	if want := []string{"reliable", "reset"}; !slices.Equal(events, want) {
		t.Fatalf("subgroup stream saw %v, want %v", events, want)
	}
}
