package conference

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/memconn"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// receiveCalls counts the participant's calls into ReceiveDatagram: once the
// n+1-th has begun, the n-th datagram has been handled.
type receiveCalls struct {
	session.Conn

	calls atomic.Int64
}

func (c *receiveCalls) ReceiveDatagram(ctx context.Context) ([]byte, error) {
	c.calls.Add(1)
	return c.Conn.ReceiveDatagram(ctx)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(5 * time.Second); !cond(); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
	}
}

// After a participant cancels a malformed track (§12.1), Objects can still
// arrive (§3.1.3): it discards them, stops reading the malformed stream, and
// does not blame the publisher for resetting a stream its own cancel ended.
// The test plays the publisher, so each of those arrives after the cancel.
func TestParticipantAfterCancellingAMalformedTrack(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	cliConn, srvConn := memconn.Pair(0)
	counted := &receiveCalls{Conn: cliConn}

	r := &run{cfg: Config{Dial: func(ctx context.Context) (*session.Session, error) {
		return session.Client(ctx, counted)
	}}.withDefaults(), epoch: time.Now(), room: "test"}
	r.speakers = []*speaker{{index: 0}}
	r.win.Store(&window{start: 0, end: time.Hour}) // everything counts
	ctx, cancel := context.WithCancel(ctx)
	defer r.teardown(cancel)

	// The publisher completes SETUP and answers the participant's two
	// SUBSCRIBEs while the participant joins.
	var pubs sync.Map // track name -> *session.Publication
	srvCh := make(chan *session.Session, 1)
	go func() {
		srv, err := session.Server(ctx, srvConn)
		if err != nil {
			t.Error(err)
			close(srvCh)
			return
		}
		srvCh <- srv
		for range 2 {
			req, err := srv.AcceptRequest(ctx)
			if err != nil {
				return
			}
			sub, _ := req.First.(*message.Subscribe)
			pub, err := req.AcceptSubscribe(nil)
			if err != nil {
				t.Error(err)
				return
			}
			pubs.Store(string(sub.Name), pub)
		}
	}()
	p := &participant{run: r, index: 1}
	p.join(ctx)
	if p.joinErr != nil {
		t.Fatal(p.joinErr)
	}
	srv := <-srvCh
	defer srv.Close(moqt.SessionNoError, "")
	alias := func(name string) uint64 {
		v, _ := pubs.Load(name)
		pub, _ := v.(*session.Publication)
		return pub.TrackAlias()
	}
	video, audio := alias(VideoTrack), alias(AudioTrack)
	stamped := func() []byte {
		b := make([]byte, 16)
		stamp(b, r.now())
		return b
	}
	gap := message.AppendTrackProperties([]wire.KVPair{{Type: message.PropertyPriorObjectIDGap, IntVal: 5}})
	received := func(kind mediaKind) int64 {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.received[kind]
	}

	// A well-formed Object of each kind arrives and counts.
	first, err := srv.OpenSubgroup(message.SubgroupHeader{TrackAlias: video, GroupID: 0})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.WriteObjectAt(0, &message.SubgroupObject{Payload: stamped()}); err != nil {
		t.Fatal(err)
	}
	if err := srv.SendDatagram(
		&message.ObjectDatagram{TrackAlias: audio, GroupID: 0, ObjectPayload: stamped()},
	); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the first Objects", func() bool { return received(kindVideo) == 1 && received(kindAudio) == 1 })

	// Then each track turns malformed (§10.9: a Prior Object ID Gap beyond
	// the Object ID), and the participant cancels both.
	bad, err := srv.OpenSubgroup(message.SubgroupHeader{TrackAlias: video, GroupID: 1, Properties: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := bad.WriteObjectAt(0, &message.SubgroupObject{Properties: gap, Payload: stamped()}); err != nil {
		t.Fatal(err)
	}
	if err := srv.SendDatagram(&message.ObjectDatagram{
		Type: message.DatagramPropertiesBit, TrackAlias: audio, GroupID: 1, Properties: gap, ObjectPayload: stamped(),
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "both cancels", func() bool { return p.isCancelled(video) && p.isCancelled(audio) })

	// The publisher resets the stream the cancel ended: not a reset to blame.
	first.Cancel(moqt.StreamResetCancelled)
	// It STOP_SENDING'd the malformed stream rather than leave it unread:
	// writes to it fail once that lands.
	stopSendingArrives(t, "the malformed stream", bad)
	// Late Objects: a new stream, which it cancels unread, and a datagram,
	// which it drops.
	late, err := srv.OpenSubgroup(message.SubgroupHeader{TrackAlias: video, GroupID: 2})
	if err != nil {
		t.Fatal(err)
	}
	stopSendingArrives(t, "the late stream", late)
	// Two datagrams were handled, so the receive loop waits in its third
	// call; it begins a fourth once it has handled the late one.
	eventually(t, "the receive loop to wait", func() bool { return counted.calls.Load() == 3 })
	if err := srv.SendDatagram(
		&message.ObjectDatagram{TrackAlias: audio, GroupID: 2, ObjectPayload: stamped()},
	); err != nil {
		t.Fatal(err)
	}
	eventually(t, "the late datagram handled", func() bool { return counted.calls.Load() == 4 })

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.received != [2]int64{1, 1} {
		t.Errorf(
			"received video %d, audio %d; want only the Objects before the cancel",
			p.received[kindVideo],
			p.received[kindAudio],
		)
	}
	if p.videoResets != 0 || p.malformed != 2 {
		t.Errorf("video resets %d, malformed %d; want 0 and 2", p.videoResets, p.malformed)
	}
}

// stopSendingArrives writes to sg until a write fails, as one does once the
// reader's STOP_SENDING lands; a reader that never sends it lets the writes
// fill the stream and block, which fails the test instead of hanging it.
func stopSendingArrives(t *testing.T, what string, sg *session.OutgoingSubgroupStream) {
	t.Helper()
	failed := make(chan struct{})
	go func() {
		for id := uint64(1); ; id++ {
			if sg.WriteObjectAt(id, &message.SubgroupObject{Payload: make([]byte, 1024)}) != nil {
				close(failed)
				return
			}
		}
	}()
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s: writes still succeed; the participant sent no STOP_SENDING", what)
	}
}
