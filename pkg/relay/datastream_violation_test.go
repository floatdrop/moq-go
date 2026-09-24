package relay_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// dialRaw connects a client session to the relay behind l and also returns its
// conn, so a test can open data streams the session API would never produce.
func dialRaw(t *testing.T, l *pipeListener) (*session.Session, session.Conn) {
	t.Helper()
	conn, err := l.Dial()
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	sess, err := session.Client(t.Context(), conn)
	if err != nil {
		t.Fatalf("session.Client: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close(moqt.SessionNoError, "") })
	return sess, conn
}

// TestRelay_UnknownDataStreamTypeClosesSession pins §3.4 at the relay: "An
// endpoint that receives an unknown stream type MUST close the session." The
// relay used to stop accepting data streams and datagrams on that session
// without closing it, leaving the peer connected to a relay that silently
// ignored everything it sent.
func TestRelay_UnknownDataStreamTypeClosesSession(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	_, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	peer, conn := dialRaw(t, l)
	uni, err := conn.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	// 0x01 is none of SUBGROUP_HEADER, FETCH_HEADER or PADDING.
	if _, err := uni.Write([]byte{0x01}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = uni.Close()

	select {
	case <-peer.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("relay left the session open after an unknown data stream type")
	}
}

// TestRelay_AbortedDataStreamHeaderKeepsServing pins §11.4.1: "Early
// termination of a unidirectional stream does not affect the MOQT application
// state." A publisher's data stream that ends before its header is complete
// must not stop the relay from forwarding that publisher's later streams.
func TestRelay_AbortedDataStreamHeaderKeepsServing(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	subSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	pubSess, conn := dialRaw(t, l)
	const alias = uint64(7)
	pubReq, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video")},
		Name:       []byte("cam1"),
		TrackAlias: alias,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { pubReq.Close() })

	subReq, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace: wire.TrackNamespace{[]byte("video")},
		Name:      []byte("cam1"),
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { subReq.Close() })

	// SUBGROUP_HEADER type, then FIN before the Track Alias.
	uni, err := conn.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, err := uni.Write([]byte{0x10}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = uni.Close()

	// From a goroutine: if the relay has stopped accepting streams, the
	// unbuffered pipe blocks the write, and the assertion below should report
	// that rather than the test hanging.
	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
			GroupID:        3,
		})
		if err != nil {
			return
		}
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
		_ = sg.Close()
	}()
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("relay stopped forwarding after a data stream ended mid-header")
	}
}

// TestRelay_FINMidObjectIsNotForwardedAsCleanEnd: a publisher's END_OF_GROUP
// subgroup stream that ends with a FIN in the middle of an object (§11.4) is
// torn, not complete. If the relay forwarded it as a clean FIN, the subscriber
// would conclude it holds the whole group. The forwarded stream must end in an
// error instead.
func TestRelay_FINMidObjectIsNotForwardedAsCleanEnd(t *testing.T) {
	t.Parallel()
	pubSess, alias := publishWithTrackProps(t, nil)
	subSess := subscribeCam1(t, pubSess)

	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
			GroupID:        3,
			EndOfGroup:     true,
		})
		if err != nil {
			return
		}
		_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("whole")})
		_, _ = sg.Write([]byte{0x00}) // next object: Object ID Delta only
		_ = sg.Close()
	}()

	// The torn object closes the publisher's session, which can beat the
	// relay's lazy downstream open — then no stream arrives at all, which is
	// fine: nothing was forwarded as complete.
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	ds, err := subSess.AcceptDataStream(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("got %T, want *session.IncomingSubgroupStream", ds)
	}
	// The complete first object may or may not arrive either: the relay can
	// reset the downstream stream before its writer drains. What must never
	// happen is a clean end.
	for range 2 {
		if _, err := sg.ReadObject(); err != nil {
			if errors.Is(err, io.EOF) {
				t.Fatalf("ReadObject = %v; the torn stream was forwarded as a clean end", err)
			}
			return
		}
	}
	t.Fatal("read two objects from a stream whose second object was torn")
}
