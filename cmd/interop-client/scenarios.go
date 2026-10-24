package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// ns builds a single-element Track Namespace tuple from s. The runner's test
// namespace ("moq-test/interop") is treated as one opaque tuple element; the
// publisher and subscriber here are both this client, so the relay only has to
// route it consistently, which it does regardless of how the string is split.
func ns(s string) wire.TrackNamespace { return wire.Namespace(s) }

// testSetupOnly connects, completes SETUP, and closes gracefully.
func testSetupOnly(ctx context.Context, h *harness) error {
	sess, err := h.connect(ctx)
	if err != nil {
		return err
	}
	_ = sess.Close(moqt.SessionNoError, "done")
	return nil
}

// testAnnounceOnly announces the test namespace and verifies REQUEST_OK.
func testAnnounceOnly(ctx context.Context, h *harness) error {
	sess, err := h.connect(ctx)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "done")

	stream, err := sess.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns(testNamespace)})
	if err != nil {
		return fmt.Errorf("PUBLISH_NAMESPACE: %w", err)
	}
	_ = stream.Close()
	return nil
}

// testPublishNamespaceDone announces, gets REQUEST_OK, then withdraws the
// namespace by cancelling the request (§4.1: a PUBLISH_NAMESPACE "is withdrawn
// by cancelling the request"), which is what NamespacePublication.Close does.
func testPublishNamespaceDone(ctx context.Context, h *harness) error {
	sess, err := h.connect(ctx)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "done")

	stream, err := sess.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns(testNamespace)})
	if err != nil {
		return fmt.Errorf("PUBLISH_NAMESPACE: %w", err)
	}
	_ = stream.Close()
	// Give the withdrawal a moment to propagate before the session closes.
	time.Sleep(200 * time.Millisecond)
	return nil
}

// testSubscribeError subscribes to a non-existent track and expects the relay
// to reject it (REQUEST_ERROR / any subscribe-level error), not return OK.
func testSubscribeError(ctx context.Context, h *harness) error {
	sess, err := h.connect(ctx)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "done")

	stream, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  ns("nonexistent-namespace"),
		Name:       []byte("nonexistent-track"),
		Parameters: message.Parameters{message.NextObjectFilter()},
	})
	if err == nil {
		_ = stream.Close()
		return errors.New("expected SUBSCRIBE error for non-existent track, got SUBSCRIBE_OK")
	}
	// Any active rejection counts — a REQUEST_ERROR (the spec-preferred
	// REQUEST_ERROR / DOES_NOT_EXIST, surfaced as *RequestRejectedError) or a
	// stream reset. Only a timeout means the relay never actually answered,
	// which is not a rejection.
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("no SUBSCRIBE response for non-existent track (timeout): %w", err)
	}
	return nil
}

// testAnnounceSubscribe follows the runner's procedure: a publisher announces
// the test namespace with PUBLISH_NAMESPACE and answers the SUBSCRIBE the relay
// routes to it; a subscriber on its own session SUBSCRIBEs the test track and
// must get SUBSCRIBE_OK.
func testAnnounceSubscribe(ctx context.Context, h *harness) error {
	pub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("publisher connect: %w", err)
	}
	defer pub.Close(moqt.SessionNoError, "done")
	go servePublisher(ctx, pub)

	nsStream, err := pub.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns(testNamespace)})
	if err != nil {
		return fmt.Errorf("publisher PUBLISH_NAMESPACE: %w", err)
	}
	defer nsStream.Close()

	sub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("subscriber connect: %w", err)
	}
	defer sub.Close(moqt.SessionNoError, "done")

	if err := subscribeTestTrack(ctx, sub); err != nil {
		return fmt.Errorf("subscriber SUBSCRIBE: %w", err)
	}
	return nil
}

// testSubscribeBeforeAnnounce follows the runner's procedure: a subscriber
// SUBSCRIBEs the test track before anyone announces it, and a publisher
// announces the namespace 500ms later. The SUBSCRIBE carries no
// RENDEZVOUS_TIMEOUT, so draft-22 has the relay refuse it at once (§9.20.6:
// "The relay MUST immediately return SUBSCRIBE_ERROR with error code
// DOES_NOT_EXIST if no publisher is available"). The runner also passes a
// relay that holds it until the announcement and answers SUBSCRIBE_OK, so
// this does too, and the publisher answers the SUBSCRIBE such a relay routes.
func testSubscribeBeforeAnnounce(ctx context.Context, h *harness) error {
	sub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("subscriber connect: %w", err)
	}
	defer sub.Close(moqt.SessionNoError, "done")
	subscribed := make(chan error, 1)
	go func() { subscribed <- subscribeTestTrack(ctx, sub) }()

	select {
	case <-time.After(500 * time.Millisecond):
	case <-ctx.Done():
		return fmt.Errorf("before the publisher started: %w", ctx.Err())
	}
	pub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("publisher connect: %w", err)
	}
	defer pub.Close(moqt.SessionNoError, "done")
	go servePublisher(ctx, pub)

	nsStream, err := pub.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns(testNamespace)})
	if err != nil {
		return fmt.Errorf("publisher PUBLISH_NAMESPACE: %w", err)
	}
	defer nsStream.Close()

	select {
	case err := <-subscribed:
		if _, rejected := errors.AsType[*session.RequestRejectedError](err); err == nil || rejected {
			return nil
		}
		return fmt.Errorf("subscriber SUBSCRIBE: %w", err)
	case <-ctx.Done():
		return fmt.Errorf("no SUBSCRIBE response: %w", ctx.Err())
	}
}

// subscribeTestTrack SUBSCRIBEs the test track and returns once the relay
// answers: nil for SUBSCRIBE_OK, a *session.RequestRejectedError for
// REQUEST_ERROR.
func subscribeTestTrack(ctx context.Context, sess *session.Session) error {
	sub, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  ns(testNamespace),
		Name:       []byte(testTrack),
		Parameters: message.Parameters{message.NextObjectFilter()},
	})
	if err != nil {
		return err
	}
	_ = sub.Close()
	return nil
}

// servePublisher answers the requests the relay sends the publisher until the
// session ends: each SUBSCRIBE with SUBSCRIBE_OK, anything else with
// NOT_SUPPORTED. The relay SUBSCRIBEs each publisher of the namespace (§7.6)
// and needs that subscription Established before its own SUBSCRIBE_OK (§7.4).
// The SUBSCRIBE_OK carries no LARGEST_OBJECT, as nothing was published
// (§9.20.17), and no objects follow: the scenarios check routing only.
func servePublisher(ctx context.Context, pub *session.Session) {
	for {
		r, err := pub.AcceptRequest(ctx)
		if err != nil {
			return
		}
		if _, ok := r.First.(*message.Subscribe); !ok {
			_ = r.RejectError(moqt.RequestNotSupported, "interop-client: the publisher answers SUBSCRIBE only")
			continue
		}
		_, _ = r.AcceptSubscribe(nil)
	}
}
