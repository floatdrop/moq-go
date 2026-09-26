package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
	"github.com/floatdrop/moq-go/pkg/relay/internal/relaytest"
)

// TestPublishSkipped_EmittedWhenSubscriberOutOfStreamCredit: with no bidi
// stream credit for a forwarded PUBLISH, the relay sends PUBLISH_SKIPPED with
// the namespace suffix and track name on the SUBSCRIBE_TRACKS stream (§6.1,
// §10.21).
func TestPublishSkipped_EmittedWhenSubscriberOutOfStreamCredit(t *testing.T) {
	t.Parallel()
	primary, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	// Subscriber whose relay-side (server) bidi credit is 0: the relay can
	// reply REQUEST_OK on the existing SUBSCRIBE_TRACKS stream, but cannot
	// open any new bidi stream toward it — so a PUBLISH forward is blocked.
	subSess := dialAnotherClientWithLimits(t, primary, -1 /*client*/, 0 /*server*/)

	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pubSess := dialAnotherClient(t, primary)
	pubStream, err := pubSess.Publish(t.Context(), &message.Publish{
		Namespace:  ns("video", "cam7"),
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

// TestPublishSkipped_NotStickyAcrossRePublish: PUBLISH_SKIPPED covers one
// PUBLISH only (§6.1); a re-PUBLISH of the track is attempted again, and
// skipped again while credit is still exhausted.
func TestPublishSkipped_NotStickyAcrossRePublish(t *testing.T) {
	t.Parallel()
	primary, teardown := connectRelay(t, relay.Config{})
	defer teardown()

	subSess := dialAnotherClientWithLimits(t, primary, -1 /*client*/, 0 /*server*/)
	subStream, err := subSess.SubscribeTracks(t.Context(), &message.SubscribeTracks{
		TrackNamespacePrefix: ns("video"),
	})
	if err != nil {
		t.Fatalf("SubscribeTracks: %v", err)
	}
	defer subStream.Close()

	pub := func() session.Stream {
		s, err := dialAnotherClient(t, primary).Publish(t.Context(), &message.Publish{
			Namespace:  ns("video", "cam7"),
			Name:       []byte("rtp"),
			TrackAlias: 99,
		})
		if err != nil {
			t.Fatalf("Publish: %v", err)
		}
		return s
	}

	// First publication: blocked (credit 0). Expect one PUBLISH_SKIPPED.
	pubStream1 := pub()
	got := relaytest.ReadNextMessage(t, subStream, time.After(2*time.Second))
	if _, ok := got.(*message.PublishSkipped); !ok {
		t.Fatalf("first publication: got %T, want *message.PublishSkipped", got)
	}
	// FIN the first publication so the relay's handlePublish returns and the
	// upstream entry is torn down before we re-publish.
	_ = pubStream1.Close()
	time.Sleep(100 * time.Millisecond)

	// Second publication of the SAME track. Per §6.1 the earlier skip
	// does not persist, so the relay re-attempts the forward — still no credit,
	// so a SECOND PUBLISH_SKIPPED reaches the subscriber's stream.
	pubStream2 := pub()
	defer pubStream2.Close()

	got2 := relaytest.ReadNextMessage(t, subStream, time.After(2*time.Second))
	if _, ok := got2.(*message.PublishSkipped); !ok {
		t.Fatalf("re-PUBLISH: got %T, want a second *message.PublishSkipped (skip is not sticky)", got2)
	}
}
