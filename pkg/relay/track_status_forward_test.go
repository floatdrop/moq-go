package relay_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/discovery"
)

// A relay with no Established subscription "MAY forward TRACK_STATUS to one or
// more publishers" (§10.15). This one forwards it to the candidates SUBSCRIBE
// would try, and answers with the first TRACK_STATUS_OK or the refusal
// SUBSCRIBE would give.

// answerTrackStatus answers every TRACK_STATUS sess receives with reply, and
// counts them.
func answerTrackStatus(t *testing.T, sess *session.Session, reply func(*session.Request)) *atomic.Int32 {
	t.Helper()
	var n atomic.Int32
	go func() {
		for {
			req, err := sess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			if _, ok := req.First.(*message.TrackStatus); ok {
				n.Add(1)
				reply(req)
			}
		}
	}()
	return &n
}

func trackStatusCam1(t *testing.T, sess *session.Session, params ...message.Parameter) (*message.TrackStatusOK, error) {
	t.Helper()
	ts, err := sess.TrackStatus(t.Context(), &message.TrackStatus{
		Namespace: ns("video"), Name: []byte("cam1"), Parameters: params,
	})
	if err != nil {
		return nil, err
	}
	_ = ts.Close()
	return ts.OK, nil
}

// TestTrackStatus_ForwardsToNamespacePublisher: with only the namespace
// advertised, the publisher's TRACK_STATUS_OK is passed on: its Track
// Properties, unless INCLUDE_PROPERTIES is 0 (§10.2.21), and its
// LARGEST_OBJECT (§10.2.17), as a SUBSCRIBE_OK would carry them.
func TestTrackStatus_ForwardsToNamespacePublisher(t *testing.T) {
	t.Parallel()
	for _, include := range []bool{true, false} {
		t.Run(map[bool]string{true: "with properties", false: "INCLUDE_PROPERTIES=0"}[include], func(t *testing.T) {
			t.Parallel()
			pub, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			publishNS(t, pub, "video")
			asked := answerTrackStatus(t, pub, func(r *session.Request) {
				_ = r.AcceptTrackStatus(&message.TrackStatusOK{
					TrackProperties: opaqueProps("h265"),
					Parameters:      message.Parameters{message.LargestObjectParam(3, 4)},
				})
			})
			var params []message.Parameter
			if !include {
				params = append(params, message.IncludePropertiesParam(false))
			}
			ok, err := trackStatusCam1(t, dialAnotherClient(t, pub), params...)
			if err != nil {
				t.Fatalf("TrackStatus: %v", err)
			}
			if asked.Load() != 1 {
				t.Fatalf("the publisher was asked %d times, want 1", asked.Load())
			}
			wantProps := opaqueProps("h265")
			if !include {
				wantProps = nil
			}
			if !bytes.Equal(ok.TrackProperties, wantProps) {
				t.Errorf("TrackProperties = %x, want %x", ok.TrackProperties, wantProps)
			}
			if p, found := ok.Parameters.Find(message.ParamLargestObject); !found || p.Group != 3 || p.Object != 4 {
				t.Errorf("LARGEST_OBJECT = %+v (found %v), want {3, 4}", p, found)
			}
		})
	}
}

// TestTrackStatus_PassesUpstreamRefusal: a refusal from the publisher is
// answered as SUBSCRIBE would answer it: DOES_NOT_EXIST passed on, a code about
// the relay's own hop as INTERNAL_ERROR (§10.6.2).
func TestTrackStatus_PassesUpstreamRefusal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ upstream, want moqt.RequestErrorCode }{
		{moqt.RequestDoesNotExist, moqt.RequestDoesNotExist},
		{moqt.RequestUnauthorized, moqt.RequestInternalError},
	} {
		t.Run(fmt.Sprintf("%#x", uint64(tc.upstream)), func(t *testing.T) {
			t.Parallel()
			pub, teardown := connectRelay(t, relay.Config{})
			t.Cleanup(teardown)
			publishNS(t, pub, "video")
			answerTrackStatus(t, pub, func(r *session.Request) { _ = r.RejectError(tc.upstream, "no") })
			_, err := trackStatusCam1(t, dialAnotherClient(t, pub))
			requireRejectedWithCode(t, err, tc.want)
		})
	}
}

// TestTrackStatus_LeftoverEntryAsksUpstream: a track whose publisher left,
// with its Track Properties still on the relay's entry, has no Established
// subscription, so TRACK_STATUS is answered as SUBSCRIBE would be: with no
// publisher left to ask, DOES_NOT_EXIST.
func TestTrackStatus_LeftoverEntryAsksUpstream(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	pub := publishVideoTrackProps(t, pubSess, "cam1", 7, trackProp(0x10, 1))
	querySess := dialAnotherClient(t, pubSess)
	subscribeCam1(t, querySess) // keeps the entry once the publisher leaves
	if _, err := trackStatusCam1(t, querySess); err != nil {
		t.Fatalf("TrackStatus while published: %v", err)
	}
	_ = pub.Close()
	waitFor(t, 2*time.Second, func() bool {
		_, err := subscribeCam1Err(t, dialAnotherClient(t, pubSess))
		return err != nil
	}, "the track still has an upstream after its publisher left")

	_, err := trackStatusCam1(t, querySess)
	requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
}

// subscribeCam1Err SUBSCRIBEs to video/cam1, returning the result.
func subscribeCam1Err(t *testing.T, sess *session.Session) (*session.Subscription, error) {
	t.Helper()
	sub, err := sess.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("video"), Name: []byte("cam1")})
	if err == nil {
		t.Cleanup(func() { _ = sub.Close() })
	}
	return sub, err
}

// TestCrossRelay_TrackStatusForwardedToRemote: a relay Discovery resolves is a
// candidate as it is for SUBSCRIBE, and its answer is passed on.
func TestCrossRelay_TrackStatusForwardedToRemote(t *testing.T) {
	t.Parallel()
	store := discovery.NewMemoryStore()
	defer store.Close()
	relayA, relayB := startRelayPair(t.Context(), store)
	defer func() { relayA.stop(t); relayB.stop(t) }()

	pub := dialClient(t, relayB)
	publishNS(t, pub, "video")
	answerTrackStatus(t, pub, func(r *session.Request) {
		_ = r.AcceptTrackStatus(&message.TrackStatusOK{TrackProperties: opaqueProps("remote")})
	})
	ok, err := trackStatusCam1(t, dialClient(t, relayA))
	if err != nil {
		t.Fatalf("TrackStatus: %v", err)
	}
	if !bytes.Equal(ok.TrackProperties, opaqueProps("remote")) {
		t.Errorf("TrackProperties = %x, want the remote publisher's", ok.TrackProperties)
	}
}

// TestRelay_TrackStatusLoopStopsAtSecondHop: a peer relay that routes the
// relay's TRACK_STATUS straight back on the same session (§6.2 has no loop
// protection) is asked once: the routed-back request finds the relay's own
// still pending on that session and is not forwarded to it again.
func TestRelay_TrackStatusLoopStopsAtSecondHop(t *testing.T) {
	t.Parallel()
	peer, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	publishNS(t, peer, "video")
	var routed atomic.Int32
	asked := answerTrackStatus(t, peer, func(r *session.Request) {
		go func() {
			if routed.Add(1) > loopCap { // a relay that does not stop still ends
				_ = r.RejectError(moqt.RequestDoesNotExist, "loop cap")
				return
			}
			if _, err := trackStatusCam1(t, peer); err != nil {
				_ = r.RejectError(moqt.RequestDoesNotExist, "routed back: "+err.Error())
				return
			}
			_ = r.AcceptTrackStatus(nil)
		}()
	})
	start := time.Now()
	_, _ = trackStatusCam1(t, dialAnotherClient(t, peer))
	if n := asked.Load(); n != 1 {
		t.Fatalf("the relay forwarded TRACK_STATUS to the peer %d times, want 1", n)
	}
	// Not by waiting out a round the routed-back request waits on itself.
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("answered after %v: the routed-back request deadlocked with the round", d)
	}
}

// namespacePeer is a fresh session on the relay sess is on that
// PUBLISH_NAMESPACEs video and answers each TRACK_STATUS with reply.
func namespacePeer(t *testing.T, sess *session.Session, reply func(*session.Request)) *session.Session {
	t.Helper()
	p := dialAnotherClient(t, sess)
	answerTrackStatus(t, p, reply)
	publishNS(t, p, "video")
	return p
}

func acceptTrackStatusWith(props []byte, largest *message.Location) func(*session.Request) {
	return func(r *session.Request) {
		ok := &message.TrackStatusOK{TrackProperties: props}
		if largest != nil {
			ok.Parameters = message.Parameters{message.LargestObjectParam(largest.Group, largest.Object)}
		}
		_ = r.AcceptTrackStatus(ok)
	}
}

// TestTrackStatus_AsksEveryCandidate: every candidate is asked, as SUBSCRIBE
// subscribes on every matching publisher (§9.5), and the answers combine as a
// SUBSCRIBE_OK's would: the largest LARGEST_OBJECT (§10.2.17), the first
// answer's Track Properties (§9.6). A refusal from one does not keep another's
// answer from the subscriber.
func TestTrackStatus_AsksEveryCandidate(t *testing.T) {
	t.Parallel()
	t.Run("largest of two", func(t *testing.T) {
		t.Parallel()
		client, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		namespacePeer(t, client, acceptTrackStatusWith(opaqueProps("first"), &message.Location{Group: 3}))
		namespacePeer(t, client, acceptTrackStatusWith(opaqueProps("second"), &message.Location{Group: 7, Object: 2}))
		ok, err := trackStatusCam1(t, client)
		if err != nil {
			t.Fatalf("TrackStatus: %v", err)
		}
		if p, found := ok.Parameters.Find(message.ParamLargestObject); !found || p.Group != 7 || p.Object != 2 {
			t.Errorf("LARGEST_OBJECT = %+v (found %v), want the larger {7, 2}", p, found)
		}
		if !bytes.Equal(ok.TrackProperties, opaqueProps("first")) {
			t.Errorf("TrackProperties = %x, want the first answer's", ok.TrackProperties)
		}
	})
	t.Run("one refuses", func(t *testing.T) {
		t.Parallel()
		client, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		namespacePeer(t, client, func(r *session.Request) { _ = r.RejectError(moqt.RequestDoesNotExist, "no") })
		namespacePeer(t, client, acceptTrackStatusWith(opaqueProps("second"), nil))
		ok, err := trackStatusCam1(t, client)
		if err != nil {
			t.Fatalf("TrackStatus: %v", err)
		}
		if !bytes.Equal(ok.TrackProperties, opaqueProps("second")) {
			t.Errorf("TrackProperties = %x, want the answering publisher's", ok.TrackProperties)
		}
	})
}

// TestTrackStatus_UpstreamMandatoryPropertyRefused: an upstream
// TRACK_STATUS_OK with an unknown Mandatory Track Property is refused with
// UNSUPPORTED_EXTENSION, as a SUBSCRIBE_OK carrying it would be (§2.5.1).
func TestTrackStatus_UpstreamMandatoryPropertyRefused(t *testing.T) {
	t.Parallel()
	client, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	namespacePeer(t, client, acceptTrackStatusWith(mandatoryProps(), nil))
	_, err := trackStatusCam1(t, client)
	requireRejectedWithCode(t, err, moqt.RequestUnsupportedExtension)
}

// TestTrackStatus_DrainingCandidateGoingAway: a candidate that sent GOAWAY is
// sent nothing (§10.4), and the refusal is GOING_AWAY, as for SUBSCRIBE.
func TestTrackStatus_DrainingCandidateGoingAway(t *testing.T) {
	t.Parallel()
	client, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	draining := namespacePeer(t, client, func(r *session.Request) { _ = r.RejectError(moqt.RequestDoesNotExist, "no") })
	if err := draining.SendGoaway(0, ""); err != nil {
		t.Fatalf("SendGoaway: %v", err)
	}
	waitFor(t, 2*time.Second, func() bool {
		_, err := trackStatusCam1(t, client)
		rej, ok := errors.AsType[*session.RequestRejectedError](err)
		return ok && rej.Code == moqt.RequestGoingAway
	}, "TRACK_STATUS to a draining publisher never refused with GOING_AWAY")
}

// TestTrackStatus_SilentCandidateTimesOut: a candidate that does not answer
// counts as TIMEOUT once the upstream round trip's bound runs out (§13.6),
// and the requester is answered.
func TestTrackStatus_SilentCandidateTimesOut(t *testing.T) {
	defer relay.SetTrackStatusTimeout(200 * time.Millisecond)()
	client, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	namespacePeer(t, client, func(*session.Request) {}) // never answers
	_, err := trackStatusCam1(t, client)
	requireRejectedWithCode(t, err, moqt.RequestTimeout)
}

// TestTrackStatus_CancelReleasesRequester: the requester cancelling its
// TRACK_STATUS (§3.3.3) stops the relay waiting on the upstream round for it,
// freeing its place under MaxSubscriptionsPerSession (§13.1) before the round
// ends; the round itself, which other requesters may share, runs to its bound.
func TestTrackStatus_CancelReleasesRequester(t *testing.T) {
	t.Parallel()
	client, teardown := connectRelay(t, relay.Config{MaxSubscriptionsPerSession: 1})
	t.Cleanup(teardown)
	asked := make(chan struct{}, 2)
	namespacePeer(t, client, func(*session.Request) { asked <- struct{}{} }) // never answers
	ctx, cancel := context.WithCancel(t.Context())
	go func() {
		_, _ = client.TrackStatus(ctx, &message.TrackStatus{Namespace: ns("video"), Name: []byte("cam1")})
	}()
	select {
	case <-asked:
	case <-time.After(2 * time.Second):
		t.Fatal("the relay never forwarded the TRACK_STATUS")
	}
	cancel()
	waitFor(t, 2*time.Second, func() bool {
		// No publisher: DOES_NOT_EXIST at once, once the slot is free.
		_, err := client.Subscribe(t.Context(), &message.Subscribe{Namespace: ns("audio"), Name: []byte("mic")})
		rej, ok := errors.AsType[*session.RequestRejectedError](err)
		return !ok || rej.Code != moqt.RequestExcessiveLoad
	}, "the cancelled TRACK_STATUS still held the session's only subscription slot")
}

// sharedRound sets up a relay whose only candidate for video/cam1 holds its
// answer until release is closed, and returns two requesters on it and the
// number of times the candidate was asked. joined reports each request that
// joins or starts the forwarded round (not parallel-safe: a package hook).
func sharedRound(
	t *testing.T,
) (first, second *session.Session, release chan struct{}, asked *atomic.Int32, joined <-chan struct{}) {
	t.Helper()
	j := make(chan struct{}, 8)
	t.Cleanup(relay.SetTestHookTrackStatusJoined(func(track.FullTrackName) { j <- struct{}{} }))
	first, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)
	second = dialAnotherClient(t, first)
	release = make(chan struct{})
	asked = &atomic.Int32{}
	namespacePeer(t, first, func(r *session.Request) {
		asked.Add(1)
		go func() {
			<-release
			_ = r.AcceptTrackStatus(&message.TrackStatusOK{TrackProperties: opaqueProps("shared")})
		}()
	})
	return first, second, release, asked, j
}

func awaitJoin(t *testing.T, joined <-chan struct{}) {
	t.Helper()
	select {
	case <-joined:
	case <-time.After(2 * time.Second):
		t.Fatal("a TRACK_STATUS never joined the forwarded round")
	}
}

type trackStatusAnswer struct {
	ok  *message.TrackStatusOK
	err error
}

func trackStatusAsync(
	ctx context.Context,
	t *testing.T,
	sess *session.Session,
	params ...message.Parameter,
) <-chan trackStatusAnswer {
	t.Helper()
	out := make(chan trackStatusAnswer, 1)
	go func() {
		ts, err := sess.TrackStatus(
			ctx,
			&message.TrackStatus{Namespace: ns("video"), Name: []byte("cam1"), Parameters: params},
		)
		if err != nil {
			out <- trackStatusAnswer{err: err}
			return
		}
		_ = ts.Close()
		out <- trackStatusAnswer{ok: ts.OK}
	}()
	return out
}

// TestTrackStatus_ConcurrentRequestsShareOneRound: TRACK_STATUS requests for
// one track that arrive while a forwarded round is in flight wait for it
// rather than each going upstream, and each is answered with its own
// INCLUDE_PROPERTIES (§10.2.21).
func TestTrackStatus_ConcurrentRequestsShareOneRound(t *testing.T) {
	first, second, release, asked, joined := sharedRound(t)
	a := trackStatusAsync(t.Context(), t, first)
	awaitJoin(t, joined)
	b := trackStatusAsync(t.Context(), t, second, message.IncludePropertiesParam(false))
	awaitJoin(t, joined)
	close(release)

	withProps, withoutProps := <-a, <-b
	if withProps.err != nil || withoutProps.err != nil {
		t.Fatalf("TrackStatus: %v, %v", withProps.err, withoutProps.err)
	}
	if n := asked.Load(); n != 1 {
		t.Fatalf("the publisher was asked %d times, want 1", n)
	}
	if !bytes.Equal(withProps.ok.TrackProperties, opaqueProps("shared")) || len(withoutProps.ok.TrackProperties) != 0 {
		t.Fatalf("TrackProperties = %x and %x, want the publisher's and empty",
			withProps.ok.TrackProperties, withoutProps.ok.TrackProperties)
	}
}

// TestTrackStatus_LeaderCancelKeepsSharedRound: the requester whose request
// started a round cancelling it (§3.3.3) does not end the round for another
// requester sharing it.
func TestTrackStatus_LeaderCancelKeepsSharedRound(t *testing.T) {
	first, second, release, _, joined := sharedRound(t)
	ctx, cancel := context.WithCancel(t.Context())
	leader := trackStatusAsync(ctx, t, first)
	awaitJoin(t, joined)
	follower := trackStatusAsync(t.Context(), t, second)
	awaitJoin(t, joined)
	cancel()
	<-leader
	close(release)
	if got := <-follower; got.err != nil {
		t.Fatalf("the follower's TrackStatus failed after the leader cancelled: %v", got.err)
	}
}

// blockingDiscovery is a Discovery store whose FindNamespace blocks until its
// ctx ends, recording that it returned.
type blockingDiscovery struct {
	discovery.DiscoveryStore

	entered  chan struct{}
	returned atomic.Bool
}

func (b *blockingDiscovery) FindNamespace(
	ctx context.Context,
	_ wire.TrackNamespace,
) ([]discovery.NamespaceInfo, error) {
	select {
	case b.entered <- struct{}{}:
	default:
	}
	<-ctx.Done()
	b.returned.Store(true)
	return nil, ctx.Err()
}

// TestTrackStatus_StopEndsRound: a forwarded round is relay work, so Stop cuts
// it short and joins it, rather than returning while it still reaches into
// Discovery.
func TestTrackStatus_StopEndsRound(t *testing.T) {
	t.Parallel()
	mem := discovery.NewMemoryStore()
	defer mem.Close()
	store := &blockingDiscovery{DiscoveryStore: mem, entered: make(chan struct{}, 1)}
	tr := startTestRelay(t.Context(), relay.Config{
		Discovery: store, RelayAddr: "relay-A", Dialer: dialerTo(nil),
	})
	go func() { _, _ = trackStatusCam1(t, dialClient(t, tr)) }()
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the round never resolved Discovery candidates")
	}
	start := time.Now()
	tr.stop(t)
	if !store.returned.Load() {
		t.Fatal("Stop returned while the forwarded round was still in Discovery")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("Stop took %v: it waited the round out instead of cutting it short", d)
	}
}

// TestTrackStatus_ForwardingCountsAgainstSubscriptionCap: a TRACK_STATUS
// waiting on upstreams counts against MaxSubscriptionsPerSession (§13.1), so
// one more past the cap is refused with EXCESSIVE_LOAD.
func TestTrackStatus_ForwardingCountsAgainstSubscriptionCap(t *testing.T) {
	t.Parallel()
	client, teardown := connectRelay(t, relay.Config{MaxSubscriptionsPerSession: 1})
	t.Cleanup(teardown)
	asked := make(chan struct{}, 2)
	namespacePeer(t, client, func(*session.Request) { asked <- struct{}{} }) // never answers
	go func() { _, _ = trackStatusCam1(t, client) }()
	select {
	case <-asked:
	case <-time.After(2 * time.Second):
		t.Fatal("the first TRACK_STATUS was never forwarded")
	}
	_, err := trackStatusCam1(t, client)
	requireRejectedWithCode(t, err, moqt.RequestExcessiveLoad)
}
