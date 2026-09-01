package main

import (
	"bytes"
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

func namespaceEqual(a, b wire.TrackNamespace) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
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

// testPublishNamespaceDone announces, gets REQUEST_OK, then unpublishes by
// finishing the request stream (§6.2: the bidi stream is the advertisement's
// keepalive; closing it withdraws the namespace).
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
	if err := stream.Close(); err != nil {
		return fmt.Errorf("close namespace stream: %w", err)
	}
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

// testAnnounceSubscribe runs a publisher and a subscriber on separate sessions:
// the publisher announces and publishes a track; the subscriber discovers the
// announcement via SUBSCRIBE_NAMESPACE and subscribes to the track.
func testAnnounceSubscribe(ctx context.Context, h *harness) error {
	pub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("publisher connect: %w", err)
	}
	defer pub.Close(moqt.SessionNoError, "done")

	nsStream, err := pub.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns(testNamespace)})
	if err != nil {
		return fmt.Errorf("publisher PUBLISH_NAMESPACE: %w", err)
	}
	defer nsStream.Close()

	if err := publishTrackOnce(ctx, pub); err != nil {
		return fmt.Errorf("publisher PUBLISH: %w", err)
	}

	// Let the announcement register before the subscriber joins.
	time.Sleep(300 * time.Millisecond)

	sub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("subscriber connect: %w", err)
	}
	defer sub.Close(moqt.SessionNoError, "done")

	if err := discoverNamespace(ctx, sub, ns(testNamespace)); err != nil {
		return fmt.Errorf("subscriber namespace discovery: %w", err)
	}

	subStream, err := sub.Subscribe(ctx, &message.Subscribe{
		Namespace:  ns(testNamespace),
		Name:       []byte(testTrack),
		Parameters: message.Parameters{message.NextObjectFilter()},
	})
	if err != nil {
		return fmt.Errorf("subscriber SUBSCRIBE: %w", err)
	}
	_ = subStream.Close()
	return nil
}

// testSubscribeBeforeAnnounce has the subscriber express namespace interest
// first; the publisher then announces, and the subscriber must receive the
// forwarded NAMESPACE.
func testSubscribeBeforeAnnounce(ctx context.Context, h *harness) error {
	sub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("subscriber connect: %w", err)
	}
	defer sub.Close(moqt.SessionNoError, "done")

	nsStream, err := sub.SubscribeNamespace(ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{},
	})
	if err != nil {
		return fmt.Errorf("subscriber SUBSCRIBE_NAMESPACE: %w", err)
	}
	defer nsStream.Close()
	found := watchForNamespace(nsStream, ns(testNamespace))

	// Subscriber interest is registered; now the publisher announces.
	time.Sleep(300 * time.Millisecond)

	pub, err := h.connect(ctx)
	if err != nil {
		return fmt.Errorf("publisher connect: %w", err)
	}
	defer pub.Close(moqt.SessionNoError, "done")

	pubStream, err := pub.PublishNamespace(ctx, &message.PublishNamespace{Namespace: ns(testNamespace)})
	if err != nil {
		return fmt.Errorf("publisher PUBLISH_NAMESPACE: %w", err)
	}
	defer pubStream.Close()

	select {
	case err := <-found:
		return err
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for announcement: %w", ctx.Err())
	}
}

// publishTrackOnce publishes the shared test track (push model) and writes a
// single object so the relay has something to register and serve.
func publishTrackOnce(ctx context.Context, sess *session.Session) error {
	alias := sess.AllocOutboundTrackAlias()
	if _, err := sess.Publish(ctx, &message.Publish{
		Namespace:  ns(testNamespace),
		Name:       []byte(testTrack),
		TrackAlias: alias,
	}); err != nil {
		return err
	}
	sg, err := sess.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		TrackAlias:     alias,
		GroupID:        0,
	})
	if err != nil {
		return fmt.Errorf("open subgroup: %w", err)
	}
	if err := sg.WriteObject(&message.SubgroupObject{Payload: []byte("interop")}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return fmt.Errorf("write object: %w", err)
	}
	return sg.Close()
}

// discoverNamespace opens a SUBSCRIBE_NAMESPACE (empty prefix = all) and waits
// for a NAMESPACE matching want, honoring ctx.
func discoverNamespace(ctx context.Context, sess *session.Session, want wire.TrackNamespace) error {
	stream, err := sess.SubscribeNamespace(ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.TrackNamespace{},
	})
	if err != nil {
		return fmt.Errorf("SUBSCRIBE_NAMESPACE: %w", err)
	}
	defer stream.Close()
	select {
	case err := <-watchForNamespace(stream, want):
		return err
	case <-ctx.Done():
		return fmt.Errorf("timeout waiting for announcement: %w", ctx.Err())
	}
}

// watchForNamespace reads NAMESPACE messages off an open SUBSCRIBE_NAMESPACE
// stream in the background and signals when one matching want arrives (or the
// stream errors). With an empty subscribed prefix the NAMESPACE suffix is the
// full advertised namespace. The goroutine unblocks when the caller closes the
// stream (on scenario return).
func watchForNamespace(stream session.Stream, want wire.TrackNamespace) <-chan error {
	ch := make(chan error, 1)
	go func() {
		for {
			m, err := message.Parse(stream)
			if err != nil {
				ch <- fmt.Errorf("read NAMESPACE: %w", err)
				return
			}
			if n, ok := m.(*message.Namespace); ok && namespaceEqual(n.TrackNamespaceSuffix, want) {
				ch <- nil
				return
			}
		}
	}()
	return ch
}
