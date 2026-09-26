package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestSubscribeUpstream_TrackEntryPrecedesAliasRouting: the SUBSCRIBE
// counterpart of TestPublish_TrackEntryPrecedesAliasRouting. The SUBSCRIBE_OK's
// alias (§11.1) routes as soon as session.Subscribe returns, so the track entry
// must exist by then or the first Group is lost.
func TestSubscribeUpstream_TrackEntryPrecedesAliasRouting(t *testing.T) {
	video := ns("video")
	name := []byte("cam-subscribe-alias-window")
	const liveLo, liveHi = uint64(5), uint64(9)

	restore := relay.SetTestHookAfterAliasRegistered(func(n track.FullTrackName) {
		if string(n.Name) != string(name) {
			return // another test's track; leave its timing alone.
		}
		// Hold AddUpstream off while the publisher's Groups arrive. The
		// in-process pipe delivers them in microseconds, so this is margin
		// rather than a tuned value.
		time.Sleep(50 * time.Millisecond)
	})
	t.Cleanup(restore)

	// unknownGapTopology's own barrier waits for LARGEST_OBJECT {liveHi,0},
	// which cannot arrive if the Groups carrying it were dropped, so a
	// regression fails inside the helper before reaching the assert below.
	fc := unknownGapTopology(t, video, name, liveLo, liveHi,
		func(_ *session.Session, req *session.Request, _ *message.Fetch) {
			_ = req.RejectError(moqt.RequestDoesNotExist, "no FETCH here")
		})

	// The watermark only proves the newest Group landed; assert the whole
	// tail is served, which is the property a dropped floor violates.
	waitFor(t, 5*time.Second, func() bool {
		return groupsEqual(realGroups(tryFetchElems(t, fc, video, name, liveHi, nil)), liveLo, liveHi)
	}, "relay never served the full tail; the oldest Group was dropped in the alias window")
}

// TestFetch_UnconfirmedTrackIsNotKnown: while the upstream has not answered the
// relay's SUBSCRIBE, a FETCH of the track is DOES_NOT_EXIST (§10.6), not
// INVALID_RANGE (§10.13).
func TestFetch_UnconfirmedTrackIsNotKnown(t *testing.T) {
	video := ns("video")
	name := []byte("cam-never-answered")

	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: video}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	// Accept the relay's SUBSCRIBE and never reply to it.
	go func() {
		for {
			if _, err := upSess.AcceptRequest(t.Context()); err != nil {
				return
			}
		}
	}()

	// Trigger the on-demand upstream SUBSCRIBE, which will hang.
	live := dialAnotherClient(t, upSess)
	go func() {
		_, _ = live.Subscribe(t.Context(), &message.Subscribe{Namespace: video, Name: name})
	}()

	fetcher := dialAnotherClient(t, upSess)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := fetcher.Fetch(t.Context(), &message.Fetch{
			Namespace: video,
			Name:      name,
			Parameters: message.Parameters{
				fetchRangeFilter(message.Location{}, message.Location{Group: 1, Object: 0}),
			},
		})
		requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
		time.Sleep(20 * time.Millisecond)
	}
}
