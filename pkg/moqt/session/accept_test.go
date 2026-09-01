package session_test

import (
	"testing"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// TestAcceptFetchRepliesFetchOK checks AcceptFetch writes FETCH_OK with the
// supplied fields and that the responder's OpenFetchStream binds this fetch's
// Request ID onto the FETCH_HEADER uni-stream automatically.
//
// The server side runs in a goroutine and the client reads on the main
// goroutine in send order (FETCH_OK, then the FETCH_HEADER stream): the
// sessiontest streams are synchronous pipes, so a writer needs a concurrent
// reader.
func TestAcceptFetchRepliesFetchOK(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	ctx := t.Context()

	fetchMsg := &message.Fetch{
		RequestID: client.AllocRequestID(),
		Namespace: wire.Namespace("ns"),
		Name:      []byte("clip"),
	}

	srvErr := make(chan error, 1)
	go func() {
		srvErr <- func() error {
			req, err := server.AcceptRequest(ctx)
			if err != nil {
				return err
			}
			resp, err := req.AcceptFetch(&message.FetchOK{
				EndOfTrack:  true,
				EndLocation: message.Location{Group: 5, Object: 9},
			})
			if err != nil {
				return err
			}
			fs, err := resp.OpenFetchStream() // Request ID bound automatically
			if err != nil {
				return err
			}
			return fs.Close()
		}()
	}()

	fr, err := client.Fetch(ctx, fetchMsg)
	if err != nil {
		t.Fatalf("client Fetch: %v", err)
	}
	if !fr.OK.EndOfTrack || fr.OK.EndLocation != (message.Location{Group: 5, Object: 9}) {
		t.Errorf("FETCH_OK = %+v, want EndOfTrack and EndLocation {5,9}", fr.OK)
	}

	ds, err := client.AcceptDataStream(ctx)
	if err != nil {
		t.Fatalf("AcceptDataStream: %v", err)
	}
	fin, ok := ds.(*session.IncomingFetchStream)
	if !ok {
		t.Fatalf("got %T, want *IncomingFetchStream", ds)
	}
	if fin.Header.RequestID != fetchMsg.RequestID {
		t.Errorf("FETCH_HEADER RequestID = %d, want %d (bound by the responder)",
			fin.Header.RequestID, fetchMsg.RequestID)
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server: %v", err)
	}
}

// TestAcceptTrackStatusRepliesOK checks AcceptTrackStatus replies TRACK_STATUS_OK
// (a REQUEST_OK) carrying the supplied Track Properties.
func TestAcceptTrackStatusRepliesOK(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)

	ts := &message.TrackStatus{
		RequestID: client.AllocRequestID(),
		Namespace: wire.Namespace("ns"),
		Name:      []byte("t"),
	}
	_, resp := runRequestRoundTrip(t, client, server, ts, func(r *session.Request) error {
		return r.AcceptTrackStatus(&message.TrackStatusOK{})
	})
	if _, ok := resp.(*message.RequestOK); !ok {
		t.Fatalf("client got %s, want REQUEST_OK (TRACK_STATUS_OK)", resp.Type())
	}
}

// TestAcceptNamespaceRequestsReplyOK checks the three namespace-request accept
// helpers each reply REQUEST_OK and return a handle.
func TestAcceptNamespaceRequestsReplyOK(t *testing.T) {
	t.Parallel()

	t.Run("PublishNamespace", func(t *testing.T) {
		t.Parallel()
		client, server := openPair(t)
		m := &message.PublishNamespace{RequestID: client.AllocRequestID(), Namespace: wire.Namespace("ns")}
		_, resp := runRequestRoundTrip(t, client, server, m, func(r *session.Request) error {
			_, err := r.AcceptPublishNamespace()
			return err
		})
		if _, ok := resp.(*message.RequestOK); !ok {
			t.Fatalf("got %s, want REQUEST_OK", resp.Type())
		}
	})

	t.Run("SubscribeNamespace", func(t *testing.T) {
		t.Parallel()
		client, server := openPair(t)
		m := &message.SubscribeNamespace{RequestID: client.AllocRequestID(), TrackNamespacePrefix: wire.Namespace("ns")}
		_, resp := runRequestRoundTrip(t, client, server, m, func(r *session.Request) error {
			_, err := r.AcceptSubscribeNamespace()
			return err
		})
		if _, ok := resp.(*message.RequestOK); !ok {
			t.Fatalf("got %s, want REQUEST_OK", resp.Type())
		}
	})

	t.Run("SubscribeTracks", func(t *testing.T) {
		t.Parallel()
		client, server := openPair(t)
		m := &message.SubscribeTracks{RequestID: client.AllocRequestID(), TrackNamespacePrefix: wire.Namespace("ns")}
		_, resp := runRequestRoundTrip(t, client, server, m, func(r *session.Request) error {
			_, err := r.AcceptSubscribeTracks()
			return err
		})
		if _, ok := resp.(*message.RequestOK); !ok {
			t.Fatalf("got %s, want REQUEST_OK", resp.Type())
		}
	})
}

// TestAcceptSubscribeTracksWritePublishSkipped checks the publisher-side
// WritePublishSkipped round-trips to the subscriber's ReadPublishSkipped. The
// server side runs in a goroutine so its REQUEST_OK and PUBLISH_SKIPPED writes
// have the client's concurrent reads (SubscribeTracks, ReadPublishSkipped) to
// drain the synchronous sessiontest pipes.
func TestAcceptSubscribeTracksWritePublishSkipped(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	ctx := t.Context()

	srvErr := make(chan error, 1)
	go func() {
		srvErr <- func() error {
			req, err := server.AcceptRequest(ctx)
			if err != nil {
				return err
			}
			pub, err := req.AcceptSubscribeTracks()
			if err != nil {
				return err
			}
			return pub.WritePublishSkipped(&message.PublishSkipped{
				TrackNamespaceSuffix: wire.Namespace("ns"),
				TrackName:            []byte("blocked-track"),
			})
		}()
	}()

	ts, err := client.SubscribeTracks(ctx, &message.SubscribeTracks{
		TrackNamespacePrefix: wire.Namespace("ns"),
	})
	if err != nil {
		t.Fatalf("client SubscribeTracks: %v", err)
	}
	defer ts.Close()

	pb, err := ts.ReadPublishSkipped()
	if err != nil {
		t.Fatalf("ReadPublishSkipped: %v", err)
	}
	if string(pb.TrackName) != "blocked-track" {
		t.Errorf("PUBLISH_SKIPPED TrackName = %q, want %q", pb.TrackName, "blocked-track")
	}
	if err := <-srvErr; err != nil {
		t.Fatalf("server: %v", err)
	}
}

// TestAcceptPublishReturnsHandle checks AcceptPublish replies REQUEST_OK and
// returns a handle reporting the publisher-assigned Track Alias.
func TestAcceptPublishReturnsHandle(t *testing.T) {
	t.Parallel()
	client, server := openPair(t)
	ctx := t.Context()

	type result struct {
		pub *session.Publication
		err error
	}
	done := make(chan result, 1)
	go func() {
		pub, err := client.Publish(ctx, &message.Publish{
			Namespace:  wire.Namespace("ns"),
			Name:       []byte("track"),
			TrackAlias: 11,
		})
		done <- result{pub, err}
	}()

	req, err := server.AcceptRequest(ctx)
	if err != nil {
		t.Fatalf("AcceptRequest: %v", err)
	}
	recv, err := req.AcceptPublish()
	if err != nil {
		t.Fatalf("AcceptPublish: %v", err)
	}
	if recv.TrackAlias() != 11 {
		t.Errorf("IncomingPublication.TrackAlias = %d, want 11", recv.TrackAlias())
	}

	got := <-done
	if got.err != nil {
		t.Fatalf("client Publish: %v", got.err)
	}
	_ = got.pub
}
