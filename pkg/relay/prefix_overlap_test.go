package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// §10.19 / §10.20: "Within a session, if a publisher receives a
// SUBSCRIBE_NAMESPACE with a Track Namespace Prefix that shares a common
// prefix with an established SUBSCRIBE_NAMESPACE, it MUST respond with
// REQUEST_ERROR with error code PREFIX_OVERLAP", and likewise for
// SUBSCRIBE_TRACKS; the two "have independent overlap spaces". Two tuple
// prefixes overlap when one is a prefix of the other.

func ns(fields ...string) wire.TrackNamespace {
	out := make(wire.TrackNamespace, len(fields))
	for i, f := range fields {
		out[i] = []byte(f)
	}
	return out
}

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
