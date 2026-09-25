package relay_test

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// TestRelay_UpstreamFetchOKUnknownMandatoryPropertyResetsStream pins §2.5.1
// for FETCH_OK: a relay receiving a Mandatory Track Property it does not
// understand "MUST cancel the fetch", and "If the relay has already forwarded
// data on a fetch stream, it MUST reset the stream." The relay answers FETCH_OK
// downstream before it stitches from upstream, so the downstream fetch stream
// is reset, not completed from the cache with an unknown range.
//
// Track Properties that do not parse are refused the same way. No Object of the
// track reaches the subscriber, not even the cached ones.
func TestRelay_UpstreamFetchOKUnknownMandatoryPropertyResetsStream(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		props []byte
	}{
		{"unknown Mandatory Track Property", message.AppendTrackProperties([]wire.KVPair{{Type: 0x4000, IntVal: 1}})},
		{"Track Properties that do not parse", []byte{0x01}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refusedFetchResetsStream(t, tc.props)
		})
	}
}

func refusedFetchResetsStream(t *testing.T, upstreamProps []byte) {
	pubSess, teardown := connectRelay(t, relay.Config{})
	defer teardown()
	ns := wire.TrackNamespace{[]byte("video")}
	name := []byte("cam1")
	if _, err := pubSess.PublishNamespace(t.Context(), &message.PublishNamespace{Namespace: ns}); err != nil {
		t.Fatalf("PublishNamespace: %v", err)
	}
	go func() {
		for {
			req, err := pubSess.AcceptRequest(t.Context())
			if err != nil {
				return
			}
			switch req.First.(type) {
			case *message.Subscribe:
				if req.Reply(&message.SubscribeOK{TrackAlias: 42}) != nil {
					return
				}
				for g := stitchLiveLo; g <= stitchLiveHi; g++ {
					sg, err := openSubgroupWaiting(t, pubSess, message.SubgroupHeader{
						SubgroupIDMode: message.SubgroupIDImplicitZero, TrackAlias: 42, GroupID: g,
					})
					if err != nil {
						return
					}
					_ = sg.WriteObject(&message.SubgroupObject{Payload: []byte{byte('a' + g)}})
					_ = sg.Close()
				}
			case *message.Fetch:
				_ = req.Reply(&message.FetchOK{
					EndLocation:     message.Location{Group: stitchLiveLo - 1},
					TrackProperties: upstreamProps,
				})
			}
		}
	}()

	live := dialAnotherClient(t, pubSess)
	if _, err := live.Subscribe(t.Context(), &message.Subscribe{Namespace: ns, Name: name}); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	go drainAll(t.Context(), live)
	fc := dialAnotherClient(t, pubSess)
	deadline := time.Now().Add(5 * time.Second)
	for {
		if objs, served := fetchRange(t, fc, ns, name,
			message.Location{Group: stitchLiveLo}, message.Location{Group: stitchLiveHi}); served && len(objs) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the relay never cached the live tail")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Reaches below the cache, so the relay stitches from the upstream.
	fr, err := fc.Fetch(t.Context(), &message.Fetch{
		Namespace: ns, Name: name,
		Parameters: message.Parameters{fetchRangeFilter(message.Location{}, message.Location{Group: stitchLiveHi})},
	})
	if err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	defer fr.Close()
	ds, err := fc.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fs, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a FETCH stream", ds)
	}
	obj, err := fs.ReadDecoded()
	switch {
	case err == nil:
		t.Fatalf("the relay forwarded Object {%d,%d} of a track whose Track Properties it refused",
			obj.GroupID, obj.ObjectID)
	case errors.Is(err, io.EOF):
		t.Fatal("the FETCH stream completed; want it reset over the upstream's Track Properties")
	}
}
