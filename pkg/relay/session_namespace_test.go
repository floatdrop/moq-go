package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestPublishNamespace_AcceptedAndRegistered exercises the happy path: a
// PUBLISH_NAMESPACE arrives, the relay authorizes it, replies REQUEST_OK,
// and keeps the request stream alive until the publisher cancels.
func TestPublishNamespace_AcceptedAndRegistered(t *testing.T) {
	t.Parallel()
	clientSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	stream, err := clientSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video", "cam1"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	// Close cancels the request (§6.2 withdrawal); the relay's handler should
	// unregister cleanly. The session itself remains alive.
	if err := stream.Close(); err != nil {
		t.Fatalf("stream.Close: %v", err)
	}
}

// TestSubscribeNamespace_AcceptedAndDeliversInitialNamespaces verifies the
// §6.1 catch-up rule: a subscriber that joins after publishers have already
// announced MUST receive a NAMESPACE for every matching publisher.
func TestSubscribeNamespace_AcceptedAndDeliversInitialNamespaces(t *testing.T) {
	t.Parallel()
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// First publisher: video/cam1.
	pubStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video", "cam1"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer pubStream.Close()

	// Open a separate session on the same in-process listener for the
	// subscriber. The first connectRelay registered its own pipeListener,
	// so we hand-roll a second session via session.Client over a fresh
	// conn pair.
	subSess := dialAnotherClient(t, pubSess)

	subStream, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer subStream.Close()

	// Expect exactly one NAMESPACE with suffix ("cam1",).
	deadline := time.After(2 * time.Second)
	got := relaytest.ReadNextMessage(t, subStream, deadline)
	ns, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("first message = %T, want *message.Namespace", got)
	}
	if len(ns.TrackNamespaceSuffix) != 1 || string(ns.TrackNamespaceSuffix[0]) != "cam1" {
		t.Fatalf("suffix = %v, want [cam1]", relaytest.FormatNamespace(ns.TrackNamespaceSuffix))
	}
}

// TestPublishNamespace_FanoutsToMatchingSubscriber drives the live forwarding
// path: a SUBSCRIBE_NAMESPACE is open when a PUBLISH_NAMESPACE arrives, the
// subscriber must receive a NAMESPACE message announcing the new publisher.
func TestPublishNamespace_FanoutsToMatchingSubscriber(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, subSess)

	pubStream, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video", "cam2"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}

	deadline := time.After(2 * time.Second)
	got := relaytest.ReadNextMessage(t, subStream, deadline)
	ns, ok := got.(*message.Namespace)
	if !ok {
		t.Fatalf("got %T, want *message.Namespace", got)
	}
	if len(ns.TrackNamespaceSuffix) != 1 || string(ns.TrackNamespaceSuffix[0]) != "cam2" {
		t.Fatalf("suffix = %v, want [cam2]", relaytest.FormatNamespace(ns.TrackNamespaceSuffix))
	}

	// Closing the publisher's request stream must produce a NAMESPACE_DONE
	// on the subscriber's stream.
	if err := pubStream.Close(); err != nil {
		t.Fatalf("pubStream.Close: %v", err)
	}
	deadline = time.After(2 * time.Second)
	got = relaytest.ReadNextMessage(t, subStream, deadline)
	done, ok := got.(*message.NamespaceDone)
	if !ok {
		t.Fatalf("after pub close: got %T, want *message.NamespaceDone", got)
	}
	if len(done.TrackNamespaceSuffix) != 1 || string(done.TrackNamespaceSuffix[0]) != "cam2" {
		t.Fatalf("done suffix = %v, want [cam2]", relaytest.FormatNamespace(done.TrackNamespaceSuffix))
	}
}

// TestSubscribeTracks_AcceptedWithoutForwarding: SUBSCRIBE_TRACKS is
// registered and replied OK, but no PUBLISH messages flow yet (those
// arrive via handlePublish). The subscriber's stream stays open and
// silent until the subscriber cancels.
func TestSubscribeTracks_AcceptedWithoutForwarding(t *testing.T) {
	t.Parallel()
	subSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}

	// Close cancels the subscription (§6.1); the handler must exit cleanly.
	if err := subStream.Close(); err != nil {
		t.Fatalf("subStream.Close: %v", err)
	}
}

// TestPublishNamespace_AuthDenialUsesPolicyCode pins the auth-precedence
// behaviour for the namespace dispatch arm: a custom policy that denies
// PUBLISH_NAMESPACE surfaces its own REQUEST_ERROR code, not REQUEST_OK.
func TestPublishNamespace_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: relay.Deny(moqt.RequestUnauthorized, "nope")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.publishNamespaceCalls.Load(); got != 1 {
		t.Errorf("publishNamespaceCalls = %d, want 1", got)
	}
}

// TestSubscribeNamespace_AuthDenialUsesPolicyCode is
// [TestPublishNamespace_AuthDenialUsesPolicyCode] for the SUBSCRIBE_NAMESPACE
// arm. Each namespace handler calls its own Authorize method and each has its
// own reject-before-register early return, so one arm passing says nothing
// about the others — a handler that skipped the check, or checked after
// registering, would leave this suite green.
func TestSubscribeNamespace_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: relay.Deny(moqt.RequestUnauthorized, "no subscribing")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: ns("video"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.subscribeNamespaceCalls.Load(); got != 1 {
		t.Errorf("subscribeNamespaceCalls = %d, want 1", got)
	}
}

// TestSubscribeTracks_AuthDenialUsesPolicyCode is the same for the
// SUBSCRIBE_TRACKS arm.
func TestSubscribeTracks_AuthDenialUsesPolicyCode(t *testing.T) {
	t.Parallel()
	auth := &denyAuthorizer{err: relay.Deny(moqt.RequestUnauthorized, "no tracks")}
	clientSess, teardown := connectRelay(t, relay.Config{Authorizer: auth})
	defer teardown()

	_, err := clientSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	requireRejectedWithCode(t, err, moqt.RequestUnauthorized)
	if got := auth.subscribeTracksCalls.Load(); got != 1 {
		t.Errorf("subscribeTracksCalls = %d, want 1", got)
	}
}

// ----- shared helpers --------------------------------------------------

// TestNamespaceStreams_AnswerRequestUpdate pins §10.9 on the namespace
// request streams: the relay previously held them open with a drain that
// discarded follow-ups unparsed, so a peer's REQUEST_UPDATE was never
// answered (the peer blocked until its ctx expired) and its §10.1 Request ID
// was never accounted for. Both the PUBLISH_NAMESPACE and the
// SUBSCRIBE_NAMESPACE streams must now reply REQUEST_OK.
func TestNamespaceStreams_AnswerRequestUpdate(t *testing.T) {
	t.Parallel()

	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	nsPub, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{
		Namespace: ns("video"),
	})
	if err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	defer nsPub.Close()

	subSess := dialAnotherClient(t, pubSess)
	nsSub, err := subSess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeNamespace: %v", err)
	}
	defer nsSub.Close()
	// Drain the initial NAMESPACE backlog announcement so UpdateRequest's
	// direct response read below cannot mistake it for the reply.
	if m, err := message.Parse(nsSub); err != nil {
		t.Fatalf("read initial NAMESPACE: %v", err)
	} else if _, ok := m.(*message.Namespace); !ok {
		t.Fatalf("initial message is %T, want *message.Namespace", m)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if _, err := pubSess.UpdateRequest(ctx, nsPub.Stream, nil); err != nil {
		t.Fatalf("PUBLISH_NAMESPACE REQUEST_UPDATE unanswered (§10.9): %v", err)
	}
	if _, err := subSess.UpdateRequest(ctx, nsSub.Stream, nil); err != nil {
		t.Fatalf("SUBSCRIBE_NAMESPACE REQUEST_UPDATE unanswered (§10.9): %v", err)
	}
}

// TestRelay_PrefixOverlap: within a session, a SUBSCRIBE_NAMESPACE or
// SUBSCRIBE_TRACKS whose prefix overlaps an established one of the same type is
// PREFIX_OVERLAP; the two types have independent spaces (§10.19, §10.20).
func TestRelay_PrefixOverlap(t *testing.T) {
	t.Parallel()

	t.Run("nested SUBSCRIBE_NAMESPACE is rejected", func(t *testing.T) {
		t.Parallel()
		sess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		first, err := sess.SubscribeNamespace(
			t.Context(),
			&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")},
		)
		if err != nil {
			t.Fatalf("first SubscribeNamespace: %v", err)
		}
		t.Cleanup(func() { _ = first.Close() })
		_, err = sess.SubscribeNamespace(
			t.Context(),
			&message.SubscribeNamespace{TrackNamespacePrefix: ns("video", "cam1")},
		)
		requireRejectedWithCode(t, err, moqt.RequestPrefixOverlap)
	})

	t.Run("nested SUBSCRIBE_TRACKS is rejected", func(t *testing.T) {
		t.Parallel()
		sess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		first, err := sess.SubscribeTracks(
			t.Context(),
			&message.SubscribeTracks{TrackNamespacePrefix: ns("video", "cam1")},
		)
		if err != nil {
			t.Fatalf("first SubscribeTracks: %v", err)
		}
		t.Cleanup(func() { _ = first.Close() })
		_, err = sess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns("video")})
		requireRejectedWithCode(t, err, moqt.RequestPrefixOverlap)
	})

	t.Run("the empty prefix overlaps everything", func(t *testing.T) {
		t.Parallel()
		sess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		first, err := sess.SubscribeNamespace(
			t.Context(),
			&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")},
		)
		if err != nil {
			t.Fatalf("first SubscribeNamespace: %v", err)
		}
		t.Cleanup(func() { _ = first.Close() })
		_, err = sess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{})
		requireRejectedWithCode(t, err, moqt.RequestPrefixOverlap)
	})

	t.Run("the two types have independent spaces", func(t *testing.T) {
		t.Parallel()
		sess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		a, err := sess.SubscribeNamespace(t.Context(), &message.SubscribeNamespace{TrackNamespacePrefix: ns("video")})
		if err != nil {
			t.Fatalf("SubscribeNamespace: %v", err)
		}
		t.Cleanup(func() { _ = a.Close() })
		b, err := sess.SubscribeTracks(t.Context(), &message.SubscribeTracks{TrackNamespacePrefix: ns("video")})
		if err != nil {
			t.Fatalf("SubscribeTracks with the same prefix: %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
	})

	t.Run("disjoint prefixes are accepted", func(t *testing.T) {
		t.Parallel()
		sess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		a, err := sess.SubscribeNamespace(
			t.Context(),
			&message.SubscribeNamespace{TrackNamespacePrefix: ns("video", "a")},
		)
		if err != nil {
			t.Fatalf("SubscribeNamespace a: %v", err)
		}
		t.Cleanup(func() { _ = a.Close() })
		b, err := sess.SubscribeNamespace(
			t.Context(),
			&message.SubscribeNamespace{TrackNamespacePrefix: ns("video", "b")},
		)
		if err != nil {
			t.Fatalf("SubscribeNamespace b (disjoint): %v", err)
		}
		t.Cleanup(func() { _ = b.Close() })
	})

	t.Run("a cancelled prefix can be reused", func(t *testing.T) {
		t.Parallel()
		sess, teardown := connectRelay(t, relay.Config{})
		defer teardown()
		first, err := sess.SubscribeNamespace(
			t.Context(),
			&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")},
		)
		if err != nil {
			t.Fatalf("first SubscribeNamespace: %v", err)
		}
		_ = first.Close() // cancels
		deadline := time.Now().Add(2 * time.Second)
		for {
			again, err := sess.SubscribeNamespace(
				t.Context(),
				&message.SubscribeNamespace{TrackNamespacePrefix: ns("video")},
			)
			if err == nil {
				t.Cleanup(func() { _ = again.Close() })
				return
			}
			if time.Now().After(deadline) {
				t.Fatalf("prefix never released after cancel: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	})
}
