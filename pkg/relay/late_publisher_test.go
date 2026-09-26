package relay_test

import (
	"sync"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// A PUBLISH_NAMESPACE matching existing subscriptions gets a SUBSCRIBE for each
// (§9.5).

// acceptedSubscribe is one SUBSCRIBE a test publisher accepted.
type acceptedSubscribe struct {
	track string // last namespace field + "/" + name
	alias uint64
	pub   *session.Publication
}

// acceptSubscribes answers every SUBSCRIBE on sess with SUBSCRIBE_OK and
// reports each one.
func acceptSubscribes(t *testing.T, sess *session.Session) <-chan acceptedSubscribe {
	t.Helper()
	got := make(chan acceptedSubscribe, 8)
	go func() {
		for {
			r, err := sess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			sub, ok := r.First.(*message.Subscribe)
			if !ok {
				continue
			}
			ok2 := &message.SubscribeOK{}
			pub, err := r.AcceptSubscribe(ok2)
			if err != nil {
				return
			}
			got <- acceptedSubscribe{
				track: string(sub.Namespace[len(sub.Namespace)-1]) + "/" + string(sub.Name),
				alias: ok2.TrackAlias,
				pub:   pub,
			}
		}
	}()
	return got
}

// publishNamespaceLate makes a new session on the same relay as anyOnRelay,
// has it send PUBLISH_NAMESPACE for ns, and returns it with the SUBSCRIBEs it
// receives.
func publishNamespaceLate(
	t *testing.T,
	anyOnRelay *session.Session,
	ns wire.TrackNamespace,
) (*session.Session, <-chan acceptedSubscribe) {
	t.Helper()
	late := dialAnotherClient(t, anyOnRelay)
	subs := acceptSubscribes(t, late)
	if _, err := late.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("late PublishNamespace: %v", err)
	}
	return late, subs
}

// awaitAcceptedSubscribe requires the next accepted SUBSCRIBE to be for want
// ("namespace/name") within 2s.
func awaitAcceptedSubscribe(t *testing.T, subs <-chan acceptedSubscribe, want string) acceptedSubscribe {
	t.Helper()
	select {
	case got := <-subs:
		if got.track != want {
			t.Fatalf("late publisher got SUBSCRIBE for %q, want %q", got.track, want)
		}
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("late publisher never received a SUBSCRIBE for the existing subscription")
	}
	return acceptedSubscribe{}
}

// requireNoSubscribe fails if a SUBSCRIBE is accepted within 300ms.
func requireNoSubscribe(t *testing.T, subs <-chan acceptedSubscribe) {
	t.Helper()
	select {
	case got := <-subs:
		t.Fatalf("publisher got an unexpected SUBSCRIBE for %q", got.track)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestRelay_LatePublishNamespaceJoinsOnDemandSubscription: a late namespace
// publisher is subscribed alongside the relay's on-demand upstream, and its
// Objects reach the subscriber.
func TestRelay_LatePublishNamespaceJoinsOnDemandSubscription(t *testing.T) {
	t.Parallel()
	video := ns("video")
	early, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := early.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("early PublishNamespace: %v", err)
	}
	earlySubs := acceptSubscribes(t, early)
	subSess := newCam1Subscriber(t, early)
	awaitAcceptedSubscribe(t, earlySubs, "video/cam1")

	late, lateSubs := publishNamespaceLate(t, early, video)
	got := awaitAcceptedSubscribe(t, lateSubs, "video/cam1")
	publishObjects(t, late, got.alias, 5, 1)
	if !awaitSubgroupObject(t, subSess, 2*time.Second) {
		t.Fatal("the late publisher's Object never reached the subscriber")
	}
}

// TestRelay_LatePublishNamespaceJoinsPublishedTrack: the existing upstream is
// a PUBLISH from another session.
func TestRelay_LatePublishNamespaceJoinsPublishedTrack(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	newCam1Subscriber(t, pubSess)
	_, lateSubs := publishNamespaceLate(t, pubSess, ns("video"))
	awaitAcceptedSubscribe(t, lateSubs, "video/cam1")
}

// TestRelay_LatePublishNamespaceSkipsTrackWithoutSubscribers: a published
// track nobody is subscribed to is not pulled from the late publisher — the
// on-demand upstream would have no downstream whose departure releases it.
func TestRelay_LatePublishNamespaceSkipsTrackWithoutSubscribers(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	_, lateSubs := publishNamespaceLate(t, pubSess, ns("video"))
	requireNoSubscribe(t, lateSubs)
}

// TestRelay_LatePublishNamespaceIgnoresOtherNamespaces: only subscriptions
// whose namespace the PUBLISH_NAMESPACE covers are sent to the new publisher.
func TestRelay_LatePublishNamespaceIgnoresOtherNamespaces(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	newCam1Subscriber(t, pubSess)
	_, lateSubs := publishNamespaceLate(t, pubSess, ns("audio"))
	requireNoSubscribe(t, lateSubs)
}

// acceptOneSubscribeRequest hands back the first request sess receives,
// unanswered.
func acceptOneSubscribeRequest(t *testing.T, sess *session.Session) <-chan *session.Request {
	t.Helper()
	got := make(chan *session.Request, 1)
	go func() {
		r, err := sess.AcceptRequest(t.Context())
		if err == nil {
			got <- r
		}
	}()
	return got
}

// awaitRequest returns the next request from reqs, failing after 2s.
func awaitRequest(t *testing.T, reqs <-chan *session.Request) *session.Request {
	t.Helper()
	select {
	case r := <-reqs:
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("late publisher never received a SUBSCRIBE")
	}
	return nil
}

// TestRelay_LatePublisherReleasedWhenSubscriberLeavesMidSubscribe: if the last
// subscriber leaves while the late publisher's SUBSCRIBE_OK is pending, that
// subscription is cancelled.
func TestRelay_LatePublisherReleasedWhenSubscriberLeavesMidSubscribe(t *testing.T) {
	t.Parallel()
	video := ns("video")
	early, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := early.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("early PublishNamespace: %v", err)
	}
	earlySubs := acceptSubscribes(t, early)
	subSess := dialAnotherClient(t, early)
	sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte("cam1")})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	earlyUp := awaitAcceptedSubscribe(t, earlySubs, "video/cam1")

	late := dialAnotherClient(t, early)
	lateReqs := acceptOneSubscribeRequest(t, late)
	if _, err := late.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("late PublishNamespace: %v", err)
	}
	r := awaitRequest(t, lateReqs)

	_ = sub.Close()
	// The relay has processed the departure once it cancels the early
	// publisher's on-demand subscription.
	select {
	case <-earlyUp.pub.Stream.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("relay kept the early subscription after its last subscriber left")
	}

	pub, err := r.AcceptSubscribe(nil)
	if err != nil {
		t.Fatalf("late AcceptSubscribe: %v", err)
	}
	select {
	case <-pub.Stream.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("relay kept the late publisher's subscription with no subscriber left")
	}
}

// TestRelay_WithdrawnPublishNamespaceGetsNoMoreSubscribes: §9.5 "When a
// publisher wants to stop new subscriptions for a published namespace, it
// cancels the request". SUBSCRIBEs for existing tracks stop with it.
func TestRelay_WithdrawnPublishNamespaceGetsNoMoreSubscribes(t *testing.T) {
	t.Parallel()
	video := ns("video")
	early, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := early.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("early PublishNamespace: %v", err)
	}
	earlySubs := acceptSubscribes(t, early)
	subSess := dialAnotherClient(t, early)
	for _, name := range []string{"cam1", "cam2"} {
		sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte(name)})
		if err != nil {
			t.Fatalf("Subscribe %s: %v", name, err)
		}
		t.Cleanup(func() { _ = sub.Close() })
		<-earlySubs
	}

	late := dialAnotherClient(t, early)
	lateReqs := acceptOneSubscribeRequest(t, late)
	nsPub, err := late.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video})
	if err != nil {
		t.Fatalf("late PublishNamespace: %v", err)
	}
	r := awaitRequest(t, lateReqs)
	_ = nsPub.Close() // withdraw while the first SUBSCRIBE is outstanding
	time.Sleep(100 * time.Millisecond)
	_, _ = r.AcceptSubscribe(nil)

	rest := acceptSubscribes(t, late)
	requireNoSubscribe(t, rest)
}

// TestRelay_PublishNamespaceDuringPendingSubscribe: a publisher registering
// while the track's first upstream SUBSCRIBE is pending is subscribed once the
// downstream is registered.
func TestRelay_PublishNamespaceDuringPendingSubscribe(t *testing.T) {
	video := ns("video")
	name := "cam-late-pending"
	early, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := early.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("early PublishNamespace: %v", err)
	}
	acceptSubscribes(t, early)
	late := dialAnotherClient(t, early)
	lateSubs := acceptSubscribes(t, late)

	var once sync.Once
	restore := relay.SetTestHookAfterAliasRegistered(func(n track.FullTrackName) {
		if string(n.Name) != name {
			return
		}
		once.Do(func() {
			if _, err := late.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
				t.Errorf("late PublishNamespace: %v", err)
			}
			// Let the relay handle it while the track still has no
			// established upstream and no downstream.
			time.Sleep(100 * time.Millisecond)
		})
	})
	t.Cleanup(restore)

	subSess := dialAnotherClient(t, early)
	sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: []byte(name)})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	awaitAcceptedSubscribe(t, lateSubs, "video/"+name)
}

// TestRelay_LatePublishNamespaceSkipsItsOwnDownstream: a session receiving the
// track is not asked to publish it back.
func TestRelay_LatePublishNamespaceSkipsItsOwnDownstream(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	subSess := newCam1Subscriber(t, pubSess)
	own := acceptSubscribes(t, subSess)
	if _, err := subSess.PublishNamespace(
		t.Context(),
		&message.PublishNamespace{Namespace: ns("video")},
	); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	requireNoSubscribe(t, own)
}

// TestRelay_OverlappingPublishNamespacesSubscribeOnce: one session sends two
// PUBLISH_NAMESPACEs that both cover an existing track. The relay sends that
// session one SUBSCRIBE for it, not one per namespace.
func TestRelay_OverlappingPublishNamespacesSubscribeOnce(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	newCam1Subscriber(t, pubSess)

	late := dialAnotherClient(t, pubSess)
	reqs := make(chan *session.Request, 4)
	go func() {
		for {
			r, err := late.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			reqs <- r
		}
	}()
	for _, ns := range []wire.TrackNamespace{{[]byte("video")}, {}} {
		if _, err := late.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
			t.Fatalf("PublishNamespace %v: %v", ns, err)
		}
	}
	first := awaitRequest(t, reqs)
	// Hold the first SUBSCRIBE_OK back while the second namespace's pass runs.
	select {
	case r := <-reqs:
		t.Fatalf("second upstream request %T for the same track while the first was in flight", r.First)
	case <-time.After(200 * time.Millisecond):
	}
	if _, err := first.AcceptSubscribe(nil); err != nil {
		t.Fatalf("AcceptSubscribe: %v", err)
	}
	requireNoSubscribeRequest(t, reqs)
}

// requireNoSubscribeRequest fails if a request arrives on reqs within the
// quiet period.
func requireNoSubscribeRequest(t *testing.T, reqs <-chan *session.Request) {
	t.Helper()
	select {
	case r := <-reqs:
		t.Fatalf("unexpected upstream request %T for the same track", r.First)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestRelay_LatePublisherResumedWhenForwardChangesMidSubscribe: a subscriber
// switching to Forward=1 while the late publisher's Forward=0 SUBSCRIBE is
// pending gets that upstream resumed once registered (§9.2).
func TestRelay_LatePublisherResumedWhenForwardChangesMidSubscribe(t *testing.T) {
	t.Parallel()
	video := ns("video")
	early, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	if _, err := early.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("early PublishNamespace: %v", err)
	}
	earlySubs := acceptSubscribes(t, early)
	subSess := dialAnotherClient(t, early)
	sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{
		Namespace:  video,
		Name:       []byte("cam1"),
		Parameters: message.Parameters{message.ForwardParam(false)},
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	t.Cleanup(func() { _ = sub.Close() })
	earlyUp := awaitAcceptedSubscribe(t, earlySubs, "video/cam1")
	go func() { _ = earlyUp.pub.Broker().Serve(t.Context(), func(message.Message) bool { return true }) }()

	late := dialAnotherClient(t, early)
	lateReqs := acceptOneSubscribeRequest(t, late)
	if _, err := late.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("late PublishNamespace: %v", err)
	}
	r := awaitRequest(t, lateReqs)
	if _, err := sub.Update(t.Context(), message.Parameters{message.ForwardParam(true)}); err != nil {
		t.Fatalf("downstream Update(Forward=1): %v", err)
	}

	pub, err := r.AcceptSubscribe(nil)
	if err != nil {
		t.Fatalf("late AcceptSubscribe: %v", err)
	}
	go func() { _ = pub.Broker().Serve(t.Context(), func(message.Message) bool { return true }) }()
	waitFor(t, 2*time.Second, func() bool {
		sg, err := pub.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit, TrackAlias: pub.TrackAlias(), GroupID: 1,
		})
		if err != nil {
			return false
		}
		_ = sg.Close()
		return true
	}, "late publisher's subscription stayed paused (Forward=0) after the subscriber resumed")
}

// TestRelay_SkippedLatePublisherSubscribedWhenFirstSubscriberArrives: a
// publisher skipped for a track with no subscriber is subscribed once one
// arrives.
func TestRelay_SkippedLatePublisherSubscribedWhenFirstSubscriberArrives(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	_, lateSubs := publishNamespaceLate(t, pubSess, ns("video"))
	requireNoSubscribe(t, lateSubs) // skipped: no subscriber yet
	newCam1Subscriber(t, pubSess)
	awaitAcceptedSubscribe(t, lateSubs, "video/cam1")
}

// TestRelay_RefusingLatePublisherNotReaskedPerSubscriber: a deferred late
// publisher that refuses the track is asked once, not again for every
// subscriber that later joins the reused upstream set.
func TestRelay_RefusingLatePublisherNotReaskedPerSubscriber(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	late := dialAnotherClient(t, pubSess)
	reqs := make(chan *session.Request, 8)
	go func() {
		for {
			r, err := late.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			reqs <- r
			_ = r.RejectError(moqt.RequestDoesNotExist, "not mine")
		}
	}()
	if _, err := late.PublishNamespace(
		t.Context(),
		&message.PublishNamespace{Namespace: ns("video")},
	); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	newCam1Subscriber(t, pubSess)
	awaitRequest(t, reqs) // the deferred SUBSCRIBE, refused
	newCam1Subscriber(t, pubSess)
	newCam1Subscriber(t, pubSess)
	requireNoSubscribeRequest(t, reqs)
}

// TestRelay_LatePublisherResubscribedForNextSubscriber: the late publisher's
// on-demand upstream is released when the track's only subscriber leaves; the
// next subscriber gets it SUBSCRIBEd again.
func TestRelay_LatePublisherResubscribedForNextSubscriber(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	_, lateSubs := publishNamespaceLate(t, pubSess, ns("video"))
	first := subscribeCam1(t, dialAnotherClient(t, pubSess))
	up := awaitAcceptedSubscribe(t, lateSubs, "video/cam1")
	_ = first.Close()
	select {
	case <-up.pub.Stream.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("late upstream not released after the last subscriber left")
	}
	newCam1Subscriber(t, pubSess)
	awaitAcceptedSubscribe(t, lateSubs, "video/cam1")
}

// TestRelay_WithdrawnSkippedPublisherNotAsked: a skipped publisher that
// withdrew its PUBLISH_NAMESPACE is not subscribed later (§9.5).
func TestRelay_WithdrawnSkippedPublisherNotAsked(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	late := dialAnotherClient(t, pubSess)
	lateSubs := acceptSubscribes(t, late)
	nsPub, err := late.PublishNamespace(
		t.Context(),
		&message.PublishNamespace{Namespace: ns("video")},
	)
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	requireNoSubscribe(t, lateSubs) // skipped: no subscriber yet
	_ = nsPub.Close()
	time.Sleep(100 * time.Millisecond)
	newCam1Subscriber(t, pubSess)
	requireNoSubscribe(t, lateSubs)
}

// latePublisherAnswering is a late publisher of (video) whose answer to each
// upstream request is decided by answer; the requests are reported.
func latePublisherAnswering(
	t *testing.T,
	anyOnRelay *session.Session,
	answer func(r *session.Request),
) <-chan *session.Request {
	t.Helper()
	late := dialAnotherClient(t, anyOnRelay)
	reqs := make(chan *session.Request, 8)
	go func() {
		for {
			r, err := late.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			reqs <- r
			answer(r)
		}
	}()
	if _, err := late.PublishNamespace(
		t.Context(),
		&message.PublishNamespace{Namespace: ns("video")},
	); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	return reqs
}

// TestRelay_RetryableLatePublisherRefusalIsRetried: a refusal with a non-zero
// Retry Interval (§10.6.2) is not remembered past it; the next subscriber
// after the interval asks again.
func TestRelay_RetryableLatePublisherRefusalIsRetried(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	reqs := latePublisherAnswering(t, pubSess, func(r *session.Request) {
		_ = message.Marshal(r.Stream, &message.RequestError{
			ErrorCode:     moqt.RequestExcessiveLoad,
			RetryInterval: 1, // may be retried immediately
			ErrorReason:   "busy",
		})
		_ = r.Stream.Close()
	})
	newCam1Subscriber(t, pubSess)
	awaitRequest(t, reqs)
	time.Sleep(50 * time.Millisecond) // let the refusal land
	newCam1Subscriber(t, pubSess)
	awaitRequest(t, reqs)
}

// TestRelay_MandatoryPropertyLatePublisherNotReasked: a late publisher whose
// SUBSCRIBE_OK carries an unknown Mandatory Track Property is cancelled
// (§2.5.1) and, like a refusal, not asked again per subscriber.
func TestRelay_MandatoryPropertyLatePublisherNotReasked(t *testing.T) {
	t.Parallel()
	pubSess, _ := newCam1Publisher(t, nil)
	reqs := latePublisherAnswering(t, pubSess, func(r *session.Request) {
		_, _ = r.AcceptSubscribe(&message.SubscribeOK{TrackProperties: mandatoryProps()})
	})
	newCam1Subscriber(t, pubSess)
	awaitRequest(t, reqs)
	newCam1Subscriber(t, pubSess)
	newCam1Subscriber(t, pubSess)
	requireNoSubscribeRequest(t, reqs)
}
