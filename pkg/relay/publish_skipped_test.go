package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestPublishSkipped_EmittedWhenSubscriberOutOfStreamCredit pins §6.1 / §10.20:
// when a SUBSCRIBE_TRACKS subscriber has no bidirectional-stream credit left,
// the relay cannot open a PUBLISH stream for a newly-published matching track,
// so it sends PUBLISH_SKIPPED on the SUBSCRIBE_TRACKS response stream instead.
// The message carries only the namespace suffix beyond the subscriber's prefix
// (here prefix "video", published "video"/"cam7" → suffix "cam7") plus the
// track name.
func TestPublishSkipped_EmittedWhenSubscriberOutOfStreamCredit(t *testing.T) {
	t.Parallel()
	primary, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// Subscriber whose relay-side (server) bidi credit is 0: the relay can
	// reply REQUEST_OK on the existing SUBSCRIBE_TRACKS stream, but cannot
	// open any new bidi stream toward it — so a PUBLISH forward is blocked.
	subSess := dialAnotherClientWithLimits(t, primary, -1 /*client*/, 0 /*server*/)

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, primary)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  wire.TrackNamespace{[]byte("video"), []byte("cam7")},
		Name:       []byte("rtp"),
		TrackAlias: 99,
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}
	defer pubStream.Close()

	// The subscriber reads the PUBLISH_SKIPPED via the session helper that
	// implements the §6.1 subscriber role.
	type result struct {
		pb  *message.PublishSkipped
		err error
	}
	res := make(chan result, 1)
	go func() {
		pb, err := subStream.ReadPublishSkipped()
		res <- result{pb, err}
	}()

	select {
	case r := <-res:
		if r.err != nil {
			t.Fatalf("ReadPublishSkipped: %v", r.err)
		}
		if len(r.pb.TrackNamespaceSuffix) != 1 || string(r.pb.TrackNamespaceSuffix[0]) != "cam7" {
			t.Fatalf("PublishSkipped suffix = %v, want [cam7]", relaytest.FormatNamespace(r.pb.TrackNamespaceSuffix))
		}
		if string(r.pb.TrackName) != "rtp" {
			t.Fatalf("PublishSkipped TrackName = %q, want %q", r.pb.TrackName, "rtp")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for PUBLISH_SKIPPED")
	}
}

// TestPublishSkipped_StickyAcrossRePublish pins the §6.1 MUST-NOT: once the
// relay has sent PUBLISH_SKIPPED for a track, a later origin re-PUBLISH of the
// SAME track must NOT be auto-forwarded to that subscriber — not even when
// stream credit might be available again — until the subscriber issues a
// SUBSCRIBE. Here the first PUBLISH (credit 0) blocks; the publisher FINs; a
// second PUBLISH of the same track must produce neither a PUBLISH nor a second
// PUBLISH_SKIPPED on the subscriber's stream.
func TestPublishSkipped_StickyAcrossRePublish(t *testing.T) {
	t.Parallel()
	primary, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subSess := dialAnotherClientWithLimits(t, primary, -1 /*client*/, 0 /*server*/)
	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: wire.TrackNamespace{[]byte("video")},
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pub := func() session.Stream {
		s, err := dialAnotherClient(t, primary).Publish(t.Context(), &message.Publish{
			Namespace:  wire.TrackNamespace{[]byte("video"), []byte("cam7")},
			Name:       []byte("rtp"),
			TrackAlias: 99,
		})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		return s
	}

	// First publication: blocked. Expect exactly one PUBLISH_SKIPPED.
	pubStream1 := pub()
	got := relaytest.ReadNextMessage(t, subStream, time.After(2*time.Second))
	if _, ok := got.(*message.PublishSkipped); !ok {
		t.Fatalf("first publication: got %T, want *message.PublishSkipped", got)
	}
	// FIN the first publication so the relay's handlePublish returns and the
	// upstream entry is torn down before we re-publish.
	_ = pubStream1.Close()
	time.Sleep(100 * time.Millisecond)

	// Second publication of the SAME track. Per §6.1 nothing must reach the
	// subscriber's SUBSCRIBE_TRACKS stream (no PUBLISH, no second
	// PUBLISH_SKIPPED).
	pubStream2 := pub()
	defer pubStream2.Close()

	done := make(chan message.Message, 1)
	go func() {
		m, _ := message.Parse(subStream)
		done <- m
	}()
	select {
	case m := <-done:
		t.Fatalf("re-PUBLISH of blocked track delivered %T to subscriber; want nothing (§6.1 MUST-NOT)", m)
	case <-time.After(400 * time.Millisecond):
		// Expected: the blocked track is suppressed on re-PUBLISH.
	}
}
