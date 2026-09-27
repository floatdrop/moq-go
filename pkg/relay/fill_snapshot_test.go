package relay_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// publishDuringFill has the fill of track name, as it is about to be
// evaluated, wait for pubSess to publish Group group Object 0 on alias and
// the relay to see it (probe polls TRACK_STATUS for it).
func publishDuringFill(t *testing.T, pubSess, probe *session.Session, name string, alias, group uint64) {
	t.Helper()
	restore := relay.SetTestHookBeforeFill(func(n track.FullTrackName) {
		if string(n.Name) != name {
			return
		}
		sendObjects(pubSess, alias, group, 1)
		for range 200 {
			ts, err := probe.TrackStatus(
				context.Background(),
				&message.TrackStatus{Namespace: ns("video"), Name: []byte(name)},
			)
			if err == nil {
				p, ok := ts.OK.Parameters.Find(message.ParamLargestObject)
				_ = ts.Close()
				if ok && p.Group == group {
					return
				}
			}
			time.Sleep(10 * time.Millisecond)
		}
		t.Errorf("the relay never saw Group %d of %s", group, name)
	})
	t.Cleanup(restore)
}

// fillStream returns the fill fetch stream sess accepts within d, skipping the
// live subgroups, or nil.
func fillStream(t *testing.T, sess *session.Session, d time.Duration) *session.IncomingFetchStream {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Until(deadline) > 0 {
		ds, ok := tryAcceptDataStream(t, sess, time.Until(deadline))
		if !ok {
			return nil
		}
		if fs, ok := ds.(*session.IncomingFetchStream); ok {
			return fs
		}
	}
	return nil
}

// TestFill_EndsAtTheLargestObjectReported: the fill paired with a Next Object
// filter "the publisher will end at Largest Object" (§5.1.3), the one its
// SUBSCRIBE_OK reported. An Object arriving before the fill is evaluated goes
// out live only, and with no Largest reported there is nothing to fill.
func TestFill_EndsAtTheLargestObjectReported(t *testing.T) {
	t.Run("SUBSCRIBE_OK reported none", func(t *testing.T) {
		pubSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		publishVideoTrack(t, pubSess, "fillsnap-none", 7)
		publishDuringFill(t, pubSess, dialAnotherClient(t, pubSess), "fillsnap-none", 7, 0)

		subSess := dialAnotherClient(t, pubSess)
		sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: ns("video"), Name: []byte("fillsnap-none"), Parameters: fillWholeTrack,
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		t.Cleanup(func() { _ = sub.Close() })
		if _, ok := sub.OK.Parameters.Find(message.ParamLargestObject); ok {
			t.Fatal("SUBSCRIBE_OK reported a LARGEST_OBJECT before any Object")
		}
		if fs := fillStream(t, subSess, 500*time.Millisecond); fs != nil {
			t.Fatalf("a fill opened for Objects published after SUBSCRIBE_OK: %v",
				decodeFetchStream(t, fs, message.GroupOrderAscending))
		}
	})
	t.Run("SUBSCRIBE_OK reported {0, 0}", func(t *testing.T) {
		pubSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		publishVideoTrack(t, pubSess, "fillsnap-some", 7)
		probe := dialAnotherClient(t, pubSess)
		sendObjects(pubSess, 7, 0, 1)
		waitRelayLargest(t, probe, ns("video"), []byte("fillsnap-some"), 0, 0)
		publishDuringFill(t, pubSess, probe, "fillsnap-some", 7, 1)

		subSess := dialAnotherClient(t, pubSess)
		sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: ns("video"), Name: []byte("fillsnap-some"), Parameters: fillWholeTrack,
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		t.Cleanup(func() { _ = sub.Close() })
		fs := fillStream(t, subSess, 2*time.Second)
		if fs == nil {
			t.Fatal("no fill fetch stream")
		}
		objs := decodeFetchStream(t, fs, message.GroupOrderAscending)
		if len(objs) == 0 {
			t.Fatal("empty fill")
		}
		if last := objs[len(objs)-1]; last.group != 0 || last.object != 0 {
			t.Fatalf("fill ended at {%d, %d}, want the reported {0, 0}", last.group, last.object)
		}
	})
	t.Run("REQUEST_UPDATE_OK reported {0, 0}", func(t *testing.T) {
		pubSess, teardown := connectRelay(t, relay.Config{})
		t.Cleanup(teardown)
		publishVideoTrack(t, pubSess, "fillsnap-update", 7)
		probe := dialAnotherClient(t, pubSess)
		sendObjects(pubSess, 7, 0, 1)
		waitRelayLargest(t, probe, ns("video"), []byte("fillsnap-update"), 0, 0)

		subSess := dialAnotherClient(t, pubSess)
		sub, err := subSess.Subscribe(t.Context(), &message.Subscribe{
			Namespace: ns("video"), Name: []byte("fillsnap-update"),
			Parameters: message.Parameters{message.NextObjectFilter()},
		})
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		t.Cleanup(func() { _ = sub.Close() })
		publishDuringFill(t, pubSess, probe, "fillsnap-update", 7, 1)
		ok, err := sub.Update(t.Context(), message.Parameters{
			message.FillParametersParam(message.Parameters{message.UnfilteredFilter()}),
		})
		if err != nil {
			t.Fatalf("Update: %v", err)
		}
		if p, found := ok.Parameters.Find(message.ParamLargestObject); !found || p.Group != 0 || p.Object != 0 {
			t.Fatalf("REQUEST_UPDATE_OK LARGEST_OBJECT = %+v (found %v), want {0, 0}", p, found)
		}
		fs := fillStream(t, subSess, 2*time.Second)
		if fs == nil {
			t.Fatal("no fill fetch stream")
		}
		objs := decodeFetchStream(t, fs, message.GroupOrderAscending)
		if len(objs) == 0 {
			t.Fatal("empty fill")
		}
		if last := objs[len(objs)-1]; last.group != 0 || last.object != 0 {
			t.Fatalf("fill ended at {%d, %d}, want the reported {0, 0}", last.group, last.object)
		}
	})
}
