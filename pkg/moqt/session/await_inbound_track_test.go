package session_test

import (
	"context"
	"testing"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// earlySubgroup returns the server's side of a subgroup stream the client
// opened for alias, which the server has not registered.
func earlySubgroup(t *testing.T, alias uint64) (client, server *session.Session, sg *session.IncomingSubgroupStream) {
	t.Helper()
	client, server = openPair(t)
	go func() {
		_, _ = client.OpenSubgroup(message.SubgroupHeader{
			SubgroupIDMode: message.SubgroupIDExplicit,
			TrackAlias:     alias,
		})
	}()
	ds, err := server.AcceptDataStream(t.Context())
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	sg, ok := ds.(*session.IncomingSubgroupStream)
	if !ok {
		t.Fatalf("AcceptDataStream = %T, want a subgroup stream", ds)
	}
	return client, server, sg
}

// TestAwaitInboundTrack pins the ways a wait for a stream's Track Alias ends:
// the alias is registered (a registration of another alias must not end it),
// ctx ends, or the session closes.
func TestAwaitInboundTrack(t *testing.T) {
	t.Parallel()
	key := track.NewKey(wire.TrackNamespace{[]byte("ns")}, []byte("t"))
	other := track.NewKey(wire.TrackNamespace{[]byte("ns")}, []byte("u"))

	t.Run("registered later", func(t *testing.T) {
		t.Parallel()
		_, server, sg := earlySubgroup(t, 7)
		got := make(chan bool, 1)
		go func() {
			in, ok := sg.AwaitInboundTrack(t.Context())
			got <- ok && in.Key == key
		}()
		// Give the waiter time to block before anything is registered, so
		// the registration of alias 8 wakes it rather than preceding it.
		time.Sleep(50 * time.Millisecond)
		if err := server.RegisterInboundTrackAlias(8, other); err != nil {
			t.Fatalf("register 8: %v", err)
		}
		select {
		case <-got:
			t.Fatal("the wait for alias 7 ended when alias 8 was registered")
		case <-time.After(50 * time.Millisecond):
		}
		if err := server.RegisterInboundTrackAlias(7, key); err != nil {
			t.Fatalf("register 7: %v", err)
		}
		select {
		case ok := <-got:
			if !ok {
				t.Fatal("AwaitInboundTrack did not return the registered track")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("AwaitInboundTrack still waiting after the alias was registered")
		}
	})

	t.Run("already registered", func(t *testing.T) {
		t.Parallel()
		_, server, sg := earlySubgroup(t, 7)
		if err := server.RegisterInboundTrackAlias(7, key); err != nil {
			t.Fatalf("register: %v", err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel() // a registered alias needs no wait at all
		if in, ok := sg.AwaitInboundTrack(ctx); !ok || in.Key != key {
			t.Fatalf("AwaitInboundTrack = (%v, %v), want the registered track", in, ok)
		}
	})

	t.Run("ctx ends", func(t *testing.T) {
		t.Parallel()
		_, _, sg := earlySubgroup(t, 7)
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer cancel()
		if _, ok := sg.AwaitInboundTrack(ctx); ok {
			t.Fatal("AwaitInboundTrack reported an alias nobody registered")
		}
	})

	t.Run("session closes", func(t *testing.T) {
		t.Parallel()
		_, server, sg := earlySubgroup(t, 7)
		got := make(chan bool, 1)
		go func() {
			_, ok := sg.AwaitInboundTrack(context.Background())
			got <- ok
		}()
		_ = server.Close(moqt.SessionNoError, "bye")
		select {
		case ok := <-got:
			if ok {
				t.Fatal("AwaitInboundTrack reported an alias nobody registered")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("AwaitInboundTrack still waiting after the session closed")
		}
	})
}
