package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
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

// runNamespace is a Track Namespace unique to this run of the test, laid out
// as the runner's data-plane routing prefix ("moq-test", "<test-id>",
// "<run-id>"), with a run ID of 128 random bits in lowercase hex. Concurrent
// runs at one relay then never see each other's namespaces.
func runNamespace(test string) wire.TrackNamespace {
	id := make([]byte, 16)
	_, _ = rand.Read(id) // never fails
	return wire.Namespace("moq-test", test, hex.EncodeToString(id))
}

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

// testSubscribeNamespaceLifecycle follows the runner's procedure: a
// subscriber SUBSCRIBE_NAMESPACEs a namespace unique to the run, a publisher
// on its own session publishes that namespace and then withdraws it, and the
// subscriber must be told NAMESPACE and then NAMESPACE_DONE for it (§4.2).
func testSubscribeNamespaceLifecycle(ctx context.Context, h *harness) error {
	namespace := runNamespace("subscribe-namespace-lifecycle")

	sub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("subscriber connect: %w", err)
	}
	defer sub.Close(moqt.SessionNoError, "done")
	nsSub, err := sub.SubscribeNamespace(ctx, &message.SubscribeNamespace{TrackNamespacePrefix: namespace})
	if err != nil {
		return fmt.Errorf("subscriber SUBSCRIBE_NAMESPACE: %w", err)
	}
	defer nsSub.Close()
	notes := watchNamespaces(ctx, nsSub)

	pub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("publisher connect: %w", err)
	}
	defer pub.Close(moqt.SessionNoError, "done")
	publication, err := pub.PublishNamespace(ctx, &message.PublishNamespace{Namespace: namespace})
	if err != nil {
		return fmt.Errorf("publisher PUBLISH_NAMESPACE: %w", err)
	}
	err = notes.expect(ctx, message.TypeNamespace)
	_ = publication.Close() // withdraws the namespace (§4.1)
	if err != nil {
		return err
	}
	return notes.expect(ctx, message.TypeNamespaceDone)
}

// namespaceNotes delivers, in order, the notifications a SUBSCRIBE_NAMESPACE
// receives, and once its stream ends, why.
type namespaceNotes struct {
	msgs <-chan message.Message
	end  error // set before msgs is closed
}

// watchNamespaces reads sub's notifications, through its broker so the
// session enforces a NAMESPACE_DONE only after its NAMESPACE (§4.2).
func watchNamespaces(ctx context.Context, sub *session.NamespaceSubscription) *namespaceNotes {
	msgs := make(chan message.Message)
	n := &namespaceNotes{msgs: msgs}
	go func() {
		defer close(msgs)
		n.end = sub.Broker().Serve(ctx, func(m message.Message) bool {
			select {
			case msgs <- m:
				return true
			case <-ctx.Done():
				return false
			}
		})
	}()
	return n
}

// expect returns nil if the next notification is a want for the subscribed
// namespace itself. The subscription's prefix is that whole namespace, so
// the notification's suffix is empty (§9.16, §9.17). A FIN does not stand in
// for NAMESPACE_DONE, though §4.2.2 has a subscriber treat it as one: the
// runner's criteria name the message.
func (n *namespaceNotes) expect(ctx context.Context, want message.Type) error {
	select {
	case m, ok := <-n.msgs:
		if !ok && n.end == nil {
			return fmt.Errorf("SUBSCRIBE_NAMESPACE stream FIN before %s", want)
		}
		if !ok {
			return fmt.Errorf("SUBSCRIBE_NAMESPACE stream ended before %s: %w", want, n.end)
		}
		var suffix wire.TrackNamespace
		switch m := m.(type) {
		case *message.Namespace:
			suffix = m.TrackNamespaceSuffix
		case *message.NamespaceDone:
			suffix = m.TrackNamespaceSuffix
		}
		if m.Type() != want || len(suffix) != 0 {
			return fmt.Errorf("got %s with suffix %q, want %s for the published namespace", m.Type(), suffix, want)
		}
		return nil
	case <-ctx.Done():
		return fmt.Errorf("no %s: %w", want, ctx.Err())
	}
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

// testRendezvousTimeout SUBSCRIBEs a track nobody publishes with a 500ms
// RENDEZVOUS_TIMEOUT: the relay should hold the SUBSCRIBE for a publisher,
// and once none has come answer REQUEST_ERROR with TIMEOUT (§9.20.6), which
// the runner's criteria require. It may hold it for less than asked, so how
// soon it answers is not checked; a DOES_NOT_EXIST says it held it not at all.
func testRendezvousTimeout(ctx context.Context, h *harness) error {
	sess, err := h.connect(ctx)
	if err != nil {
		return err
	}
	defer sess.Close(moqt.SessionNoError, "done")

	sub, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace: ns("nonexistent/rendezvous"),
		Name:      []byte(testTrack),
		Parameters: message.Parameters{
			message.NextObjectFilter(),
			message.RendezvousTimeoutParam(500 * time.Millisecond),
		},
	})
	if err == nil {
		_ = sub.Close()
		return errors.New("expected REQUEST_ERROR TIMEOUT for a track nobody publishes, got SUBSCRIBE_OK")
	}
	if rej, ok := errors.AsType[*session.RequestRejectedError](err); !ok || rej.Code != moqt.RequestTimeout {
		return fmt.Errorf("SUBSCRIBE: want REQUEST_ERROR TIMEOUT (%#x): %w", uint64(moqt.RequestTimeout), err)
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
