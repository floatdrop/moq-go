package relay_test

import (
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestSubscribeUpstream_TrackEntryPrecedesAliasRouting is the SUBSCRIBE
// counterpart of TestPublish_TrackEntryPrecedesAliasRouting.
//
// session.Subscribe registers the SUBSCRIBE_OK's §11.1 Track Alias inside its
// own response handler, so the alias resolves on inbound data streams the
// instant it returns — while AddUpstream, which would otherwise be the first
// thing to create the track entry, is still several statements away. A
// subgroup stream arriving in between reaches runFanout, resolves its alias,
// finds no entry, and is reset: those Objects are lost from the cache and from
// live fanout alike, permanently, with nothing above DEBUG to say so.
//
// The publisher writes its first Group immediately after SUBSCRIBE_OK, so the
// Group that loses is the oldest — which is what made
// TestFetch_UnknownRangeMarkerDescending fail on CI while passing everywhere
// else: it asserts on a complete tail and the floor kept going missing.
func TestSubscribeUpstream_TrackEntryPrecedesAliasRouting(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
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
	fc := unknownGapTopology(t, ns, name, liveLo, liveHi,
		func(_ *session.Session, req *session.Request, _ *message.Fetch) {
			_ = req.RejectError(moqt.RequestDoesNotExist, "no FETCH here")
		})

	// The watermark only proves the newest Group landed; assert the whole
	// tail is served, which is the property a dropped floor violates.
	waitFor(t, 5*time.Second, func() bool {
		return groupsEqual(realGroups(tryFetchElems(t, fc, ns, name, liveHi, nil)), liveLo, liveHi)
	}, "relay never served the full tail; the oldest Group was dropped in the alias window")
}

// TestFetch_UnconfirmedTrackIsNotKnown pins the cost of creating that entry
// before the upstream round trip that confirms the track exists.
//
// For the duration of the round trip an entry stands for a track nobody has
// vouched for. handleFetch used to read bare existence as "track known", fall
// through to the §10.12.3 "no Objects have been published" rule, and answer
// INVALID_RANGE — "the range you asked for cannot be satisfied" — where §10.6
// DOES_NOT_EXIST, "the track or namespace is not available at the publisher",
// is the truthful answer. A client deciding whether to retry needs them apart.
//
// The upstream here advertises the namespace and then never answers the
// relay's SUBSCRIBE, so the entry sits unvouched for as long as the test cares
// to look. Every answer over that window must be DOES_NOT_EXIST — polling
// rather than a single probe because the first FETCH may land before the entry
// exists, which is DOES_NOT_EXIST for the uninteresting reason.
func TestFetch_UnconfirmedTrackIsNotKnown(t *testing.T) {
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam-never-answered")

	upSess, teardown := connectRelay(t, relay.Config{})
	t.Cleanup(teardown)

	if _, err := upSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
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
		_, _ = live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name})
	}()

	fetcher := dialAnotherClient(t, upSess)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		_, err := fetcher.Fetch(t.Context(), &message.Fetch{
			FetchType: message.FetchTypeStandalone,
			Standalone: &message.StandaloneFetch{
				Namespace:     ns,
				Name:          name,
				StartLocation: message.Location{Group: 0, Object: 0},
				EndLocation:   message.Location{Group: 1, Object: 1},
			},
		})
		requireRejectedWithCode(t, err, moqt.RequestDoesNotExist)
		time.Sleep(20 * time.Millisecond)
	}
}
