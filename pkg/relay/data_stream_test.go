package relay_test

import (
	"context"
	"errors"
	"io"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Inbound data streams at the relay: malformed or early ones, and the session
// errors they carry.

// TestRelay_UnknownDataStreamTypeClosesSession: an unknown data stream type
// closes the session (§3.4).
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

	requireSessionClosed(t, peer, "an unknown data stream type")
}

// TestRelay_AbortedDataStreamHeaderKeepsServing: a data stream that ends
// mid-header does not stop the relay forwarding the publisher's later streams
// (§11.4.1).
func TestRelay_AbortedDataStreamHeaderKeepsServing(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	subSess, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	pubSess, conn := dialRaw(t, l)
	const alias = uint64(7)
	publishVideoTrack(t, pubSess, "cam1", alias)
	subscribeCam1(t, subSess)

	// SUBGROUP_HEADER type, then FIN before the Track Alias.
	uni, err := conn.OpenUniStream()
	if err != nil {
		t.Fatalf("OpenUniStream: %v", err)
	}
	if _, err := uni.Write([]byte{0x10}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	_ = uni.Close()

	// From a goroutine, so a relay that stopped accepting streams fails the
	// assertion below rather than hanging the write.
	go sendObjects(pubSess, alias, 3, 1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("relay stopped forwarding after a data stream ended mid-header")
	}
}

// TestRelay_FINMidObjectIsNotForwardedAsCleanEnd: an END_OF_GROUP subgroup
// stream FIN'd mid-object (§11.4) is not forwarded with a clean FIN.
func TestRelay_FINMidObjectIsNotForwardedAsCleanEnd(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	subSess := newCam1Subscriber(t, pubSess)

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

// TestRelay_ObjectIDOverflowClosesSession: an Object ID delta past 2^64 - 1
// closes the session (§11.4.2).
func TestRelay_ObjectIDOverflowClosesSession(t *testing.T) {
	t.Parallel()
	pubSess, alias := newCam1Publisher(t, nil)
	_ = newCam1Subscriber(t, pubSess)

	go func() {
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
			GroupID:        3,
		})
		if err != nil {
			return
		}
		_ = sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: math.MaxUint64, Payload: []byte("a")})
		_ = sg.WriteObject(&message.SubgroupObject{ObjectIDDelta: 0, Payload: []byte("b")})
		_ = sg.Close()
	}()

	requireSessionClosed(t, pubSess, "an Object ID overflow")
}

// TestRelay_NonFirstRequestOpenerClosesSession: a request stream opened with a
// message not marked "First" in Table 5, here PUBLISH_STATE_NOTIFY (§10.10),
// closes the session.
func TestRelay_NonFirstRequestOpenerClosesSession(t *testing.T) {
	t.Parallel()
	l := newPipeListener()
	_, teardown := connectRelayOn(t, relay.Config{}, l)
	defer teardown()

	peer, conn := dialRaw(t, l)
	stream, err := conn.OpenStream()
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	go func() { _ = message.Marshal(stream, &message.PublishStateNotify{}) }()

	requireSessionClosed(t, peer, "a PUBLISH_STATE_NOTIFY opened a request stream")
}

// TestRelay_SubgroupBeforeSubscribeOKIsDelivered: a subgroup stream that
// arrives before the SUBSCRIBE_OK naming its alias is held briefly (§11.4.2)
// rather than abandoned. Not parallel: the hook is process-wide.
func TestRelay_SubgroupBeforeSubscribeOKIsDelivered(t *testing.T) {
	const alias = uint64(4242) // unique to this test
	waiting := make(chan struct{}, 1)
	defer relay.SetTestHookEarlyStreamWaiting(func(a uint64) {
		if a == alias {
			select {
			case waiting <- struct{}{}:
			default:
			}
		}
	})()

	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	video := ns("video")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	go func() {
		req, err := pubSess.AcceptRequest(t.Context())
		if err != nil {
			return
		}
		if _, ok := req.First.(*message.Subscribe); !ok {
			return
		}
		// Errors are ignored so the reply always goes out.
		if sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
		}); err == nil {
			go func() {
				_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")})
				_ = sg.Close()
			}()
		}
		select {
		case <-waiting:
		case <-time.After(time.Second):
		}
		_ = req.Reply(&message.SubscribeOK{TrackAlias: alias})
	}()

	subSess := dialAnotherClient(t, pubSess)
	if _, err := subSess.Subscribe(
		t.Context(),
		&message.Subscribe{Namespace: video, Name: []byte("cam1")},
	); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	// The object reaches the relay's cache. Live delivery to this subscriber
	// is not asserted: the object may be forwarded before the subscriber's
	// downstream is registered, and a subscription starts after the Largest
	// Object at that point.
	if !fetchesObject(t, dialAnotherClient(t, pubSess), video, []byte("cam1"), 2*time.Second) {
		t.Fatal("the subgroup that arrived before its SUBSCRIBE_OK never reached the relay's cache: " +
			"the relay abandoned it as an unknown Track Alias")
	}
}

// fetchesObject FETCHes the track until the response carries an Object with
// payload "x", reporting whether one did within the deadline.
func fetchesObject(
	t *testing.T,
	sess *session.Session,
	ns wire.TrackNamespace,
	name []byte,
	within time.Duration,
) bool {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		fr, err := sess.Fetch(t.Context(), &message.Fetch{Namespace: ns, Name: name})
		if err == nil {
			ds, err := sess.AcceptDataStream(t.Context())
			if err != nil {
				t.Fatalf("AcceptDataStream: %v", err)
			}
			fs, ok := ds.(*session.IncomingFetchStream)
			if !ok {
				t.Fatalf("AcceptDataStream = %T, want a FETCH stream", ds)
			}
			objs := decodeFetchStream(t, fs, message.GroupOrderAscending)
			_ = fr.Close()
			// Only the published Object counts, not an End of Range marker.
			if slices.ContainsFunc(objs, func(o decodedFetchObject) bool { return string(o.payload) == "x" }) {
				return true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

// TestRelay_EarlySubgroupWaitIsBounded: at most maxEarlyStreams subgroup
// streams per session wait for an unknown alias, and none past earlyAliasWait.
func TestRelay_EarlySubgroupWaitIsBounded(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	const (
		bogusAlias = uint64(999) // never bound by a SUBSCRIBE_OK or PUBLISH
		waiting    = 32          // relay.maxEarlyStreams
	)
	// writeFails writes one object on a fresh subgroup stream in the
	// background and reports when the write fails: the relay reset the stream.
	writeFails := func() <-chan struct{} {
		failed := make(chan struct{})
		sg, err := pubSess.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     bogusAlias,
		})
		if err != nil {
			t.Fatalf("OpenSubgroup: %v", err)
		}
		go func() {
			if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("x")}); err != nil {
				close(failed)
			}
		}()
		return failed
	}

	start := time.Now()
	held := make([]<-chan struct{}, waiting)
	for i := range held {
		held[i] = writeFails()
	}
	time.Sleep(100 * time.Millisecond) // let the relay start waiting on each

	// One more than the relay will hold is refused at once...
	select {
	case <-writeFails():
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("stream %d was held too; the relay must reset streams past its limit at once", waiting+1)
	}
	// ...while every one within the limit is still held.
	for i, failed := range held {
		select {
		case <-failed:
			t.Fatalf("held stream %d was reset before the wait ran out; the relay holds %d", i, waiting)
		default:
		}
	}
	// The held ones are released after the brief wait, not kept for good.
	for i, failed := range held {
		select {
		case <-failed:
		case <-time.After(5 * time.Second):
			t.Fatalf("held stream %d was still open %s after it arrived", i, time.Since(start))
		}
	}
	if d := time.Since(start); d < 500*time.Millisecond {
		t.Errorf("held streams were reset after %s; the relay should wait about a second for the alias", d)
	}
}
