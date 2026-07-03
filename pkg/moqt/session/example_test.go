package session_test

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/quic-go/quic-go"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/moqt/session/quicconn"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// Opening a session. The session layer is transport-agnostic: it operates on
// a Conn. The quicconn adapter wraps a quic-go connection; wtconn wraps a
// WebTransport session.
func ExampleClient() {
	ctx := context.Background()

	tlsCfg := &tls.Config{
		InsecureSkipVerify: true, // dev only
		NextProtos:         []string{"moq-00"},
	}
	qconn, err := quic.DialAddr(ctx, "relay.example:4433", tlsCfg, &quic.Config{
		MaxIdleTimeout:  30 * time.Second,
		KeepAlivePeriod: 5 * time.Second,
		EnableDatagrams: true,
	})
	if err != nil {
		panic(err)
	}

	// Client drives the SETUP handshake; the returned Session owns its
	// control-stream goroutines until you Close it.
	sess, err := session.Client(ctx, quicconn.New(qconn),
		session.WithImplementation("my-app/0.1"),
	)
	if err != nil {
		panic(err)
	}
	// Close with one of the §3.5 session-error codes from pkg/moqt.
	defer sess.Close(moqt.SessionNoError, "bye")
}

// Publishing a track: send PUBLISH to declare it, then open one subgroup
// stream per group and write objects onto it.
func ExampleSession_Publish() {
	var sess *session.Session // from session.Client / session.Server
	ctx := context.Background()

	// Publish assigns the Track Alias for you; the returned Publication owns
	// it, so subgroups open via pub.OpenSubgroup without threading the alias.
	pub, err := sess.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("moq-example"),
		Name:      []byte("clock"),
	})
	if err != nil {
		return // includes *session.RequestRejectedError on REQUEST_ERROR
	}
	defer pub.Close() // stays open for follow-ups / PUBLISH_DONE

	sg, err := pub.OpenSubgroup(message.SubgroupHeader{
		SubgroupIDMode: message.SubgroupIDImplicitZero,
		GroupID:        0,
	})
	if err != nil {
		return
	}
	// WriteObjectAt takes absolute Object IDs and computes the §11.4.2 delta
	// encoding for you (the mirror of ReadDecoded). Use the lower-level
	// WriteObject if you want to set ObjectIDDelta yourself.
	if err := sg.WriteObjectAt(0, &message.SubgroupObject{
		Payload: []byte("2026-06-18T00:00:00Z"),
	}); err != nil {
		sg.Cancel(moqt.StreamResetInternalError)
		return
	}
	if err := sg.Close(); err != nil { // FIN the stream
		return
	}
}

// Subscribing to a track: send SUBSCRIBE, then read objects from the
// uni-streams pulled by AcceptDataStream. The subscription filter (§5.1.2)
// decides where delivery starts; FilterLargestObject means "everything
// strictly after the current live edge".
func ExampleSession_Subscribe() {
	var sess *session.Session
	ctx := context.Background()

	sub, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  wire.Namespace("moq-example"),
		Name:       []byte("clock"),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	})
	if err != nil {
		return
	}
	defer sub.Close()
	_ = sub.TrackAlias() // matches the alias on inbound subgroup streams

	for {
		ds, err := sess.AcceptDataStream(ctx)
		if err != nil {
			if errors.Is(err, session.ErrPaddingStream) {
				continue // §11.6 padding stream — ignore and keep looping
			}
			return
		}
		switch s := ds.(type) {
		case *session.IncomingSubgroupStream:
			for {
				obj, err := s.ReadDecoded() // resolves §11.4.2 deltas
				if err != nil {
					break // io.EOF on clean FIN
				}
				fmt.Printf("group=%d object=%d payload=%q\n",
					obj.GroupID, obj.ObjectID, obj.Payload)
			}
		case *session.IncomingFetchStream:
			// FETCH objects — see ExampleSession_Fetch / ExampleIncomingFetchStream.
		}
	}
}

// Routing inbound data streams with a Demux instead of the hand-rolled
// AcceptDataStream loop + type-switch above. Subgroup streams dispatch by their
// §11.1 Track Alias, FETCH streams by their §11.5 Request ID, so a subscriber
// to several tracks gets each track's objects on its own handler without
// matching aliases by hand.
func ExampleDemux() {
	var sess *session.Session
	ctx := context.Background()

	audio, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  wire.Namespace("moq-example"),
		Name:       []byte("audio"),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	})
	if err != nil {
		return
	}
	defer audio.Close()

	video, err := sess.Subscribe(ctx, &message.Subscribe{
		Namespace:  wire.Namespace("moq-example"),
		Name:       []byte("video"),
		Parameters: message.Parameters{message.LargestObjectFilter()},
	})
	if err != nil {
		return
	}
	defer video.Close()

	d := session.NewDemux()
	// The alias comes from each subscription's SUBSCRIBE_OK. Handlers may be
	// registered before or after Run starts — registration is concurrency-safe.
	d.HandleTrack(audio.TrackAlias(), func(s *session.IncomingSubgroupStream) {
		drain(s, "audio")
	})
	d.HandleTrack(video.TrackAlias(), func(s *session.IncomingSubgroupStream) {
		drain(s, "video")
	})
	// Optional: without OnUnknown, an unmatched stream is reset and dropped.
	d.OnUnknown(func(ds session.DataStream) {
		ds.Cancel(moqt.StreamResetInternalError)
	})

	// Run dispatches synchronously, so a handler that reads a long-lived stream
	// blocks the loop; the drain helper spawns a goroutine per stream.
	_ = d.Run(ctx, sess) // returns when ctx is cancelled or the session ends.
}

// drain reads one subgroup stream to completion off the Demux dispatch loop, so
// concurrent tracks don't head-of-line block each other.
func drain(s *session.IncomingSubgroupStream, label string) {
	go func() {
		for {
			obj, err := s.ReadDecoded() // resolves §11.4.2 deltas
			if err != nil {
				return // io.EOF on clean FIN
			}
			//nolint:forbidigo // doc-example helper prints like the Example* bodies it backs
			fmt.Printf("%s group=%d object=%d payload=%q\n",
				label, obj.GroupID, obj.ObjectID, obj.Payload)
		}
	}()
}

// Routing inbound requests on the server side. RequestMux is the request-stream
// counterpart of Demux: register a handler per message.Type instead of hand-
// rolling an AcceptRequest loop + type switch.
func ExampleRequestMux() {
	var server *session.Session // e.g. from session.Server
	ctx := context.Background()

	mux := session.NewRequestMux()

	// HandleType hands the handler the already-asserted typed message. A
	// SUBSCRIBE handler keeps the request stream open for the subscription's
	// lifetime, so it spawns a goroutine — Run dispatches synchronously, exactly
	// like Demux.
	session.HandleType(mux, func(r *session.Request, _ *message.Subscribe) {
		go func() {
			pub, err := r.AcceptSubscribe(nil) // writes SUBSCRIBE_OK, returns a Publication
			if err != nil {
				return
			}
			defer pub.Close()
			// … push objects via pub.OpenSubgroup … then pub.Done(...).
			_ = pub
		}()
	})
	mux.Handle(message.TypePublishNamespace, func(r *session.Request) {
		_ = r.Reply(&message.RequestOK{})
		_ = r.Stream.Close()
	})

	// Optional: without OnUnknown, an unhandled type is rejected NOT_SUPPORTED.
	mux.OnUnknown(func(r *session.Request) {
		_ = r.RejectError(moqt.RequestNotSupported, "unsupported request type")
	})

	// Run returns when ctx is cancelled or AcceptRequest fails. A session-fatal
	// error (e.g. *session.ErrDuplicateRequestID) should be escalated by closing
	// the session with the mapped code.
	_ = mux.Run(ctx, server)
}

// Backfilling with a Relative Joining FETCH (§10.12.2): a FilterLargestObject
// subscription only delivers objects strictly after the live edge, so the
// current group is invisible until the next one lands. A joining FETCH keyed
// to the subscription's Request ID backfills it.
func ExampleSession_Fetch() {
	var sess *session.Session
	ctx := context.Background()
	var subscribeRequestID uint64 // the RequestID of an earlier SUBSCRIBE

	fetch, err := sess.Fetch(ctx, &message.Fetch{
		FetchType: message.FetchTypeRelativeJoining,
		Joining: &message.JoiningFetch{
			JoiningRequestID: subscribeRequestID,
			JoiningStart:     0, // 0 = current group only
		},
	})
	if err != nil {
		return // a failed backfill is not fatal — the live subscription stands
	}
	defer fetch.Close()
	_ = fetch.OK.EndLocation.Group
	// Objects arrive on a *session.IncomingFetchStream via AcceptDataStream.
}

// Standalone FETCH of an explicit [Start, End] range, with no associated
// subscription — useful for retrieving a known-cached object (a catalog, a
// keyframe) by its coordinates.
func ExampleSession_Fetch_standalone() {
	var sess *session.Session
	ctx := context.Background()

	fetch, err := sess.Fetch(ctx, &message.Fetch{
		FetchType: message.FetchTypeStandalone,
		Standalone: &message.StandaloneFetch{
			Namespace:     wire.Namespace("moq-example"),
			Name:          []byte("clock"),
			StartLocation: message.Location{Group: 0, Object: 0},
			EndLocation:   message.Location{Group: 10, Object: 0},
		},
	})
	if err != nil {
		return
	}
	defer fetch.Close()
}

// Draining the objects a FETCH delivers. The *IncomingFetchStream arrives
// from Session.AcceptDataStream; ReadDecoded resolves §11.4.4 deltas to
// absolute IDs.
func ExampleIncomingFetchStream() {
	var s *session.IncomingFetchStream
	for {
		obj, err := s.ReadDecoded()
		if err != nil {
			return // io.EOF on clean FIN
		}
		if obj.EndOfNonExistentRange || obj.EndOfUnknownRange {
			continue // §11.4.4.2 absence markers carry no payload
		}
		fmt.Printf("backfill group=%d object=%d\n", obj.GroupID, obj.ObjectID)
	}
}

// Updating a live request (§10.9). REQUEST_UPDATE rides the original
// request's bidi stream and consumes no new Request ID. Only the parameters
// you include change; omitted ones keep their prior value on the peer.
// [Subscription.Update] (and the FetchRequest equivalent) fills in the stream
// and Request ID; [Session.UpdateRequest] is the lower-level form for when you
// hold those yourself.
func ExampleSession_UpdateRequest() {
	// raisePriority takes the live Subscription returned by sess.Subscribe.
	raisePriority := func(ctx context.Context, sub *session.Subscription) error {
		_, err := sub.Update(ctx, message.Parameters{
			message.SubscriberPriorityParam(10), // lower = higher priority
		})
		return err // *session.RequestRejectedError on REQUEST_ERROR
	}
	_ = raisePriority
}

// Ending a publication (§10.11): Publication.Done writes PUBLISH_DONE on the
// request stream and FINs it. (message.Marshal + Close is the lower-level form.)
func Example_endingAPublication() {
	var sess *session.Session
	ctx := context.Background()

	pub, err := sess.Publish(ctx, &message.Publish{
		Namespace: wire.Namespace("moq-example"),
		Name:      []byte("clock"),
	})
	if err != nil {
		return
	}
	// Done writes PUBLISH_DONE (with the §10.11 Stream Count of subgroups opened
	// via the handle) and FINs the stream in one call.
	_ = pub.Done(moqt.PublishDoneTrackEnded, "")
}

// Reacting to stream exhaustion. A relay forwarding many tracks can't block,
// so it uses the non-blocking OpenPublish. When the peer's stream limit is
// exhausted it returns ErrNoStreamCredit WITHOUT consuming a Request ID, so
// the caller can send PUBLISH_BLOCKED (§6.1, §10.20) and let the subscriber
// recover with an explicit SUBSCRIBE.
func ExampleSession_OpenPublish() {
	var sess *session.Session
	var subscribeTracksStream session.Stream
	ns := wire.Namespace("rooms", "room-42")
	name := []byte("camera")

	pubStream, err := sess.OpenPublish(&message.Publish{
		Namespace:  ns,
		Name:       name,
		TrackAlias: sess.AllocOutboundTrackAlias(),
	})
	if errors.Is(err, session.ErrNoStreamCredit) {
		_ = message.Marshal(subscribeTracksStream, &message.PublishBlocked{
			TrackNamespaceSuffix: ns,
			TrackName:            name,
		})
		return
	}
	if err != nil {
		return
	}
	defer pubStream.Close()
}

// The subscriber reads PUBLISH_BLOCKED on the SUBSCRIBE_TRACKS response
// stream; the sanctioned recovery is an explicit SUBSCRIBE for the track.
func ExampleTrackSubscription_ReadPublishBlocked() {
	var sub *session.TrackSubscription // returned by Session.SubscribeTracks

	pb, err := sub.ReadPublishBlocked()
	if err != nil {
		return
	}
	fmt.Printf("PUBLISH_BLOCKED for track %q\n", pb.TrackName)
	// Recover: sess.Subscribe for (pb.TrackNamespaceSuffix, pb.TrackName).
}

// Announcing a whole namespace once (§10.15) rather than a PUBLISH per track.
func ExampleSession_PublishNamespace() {
	var sess *session.Session
	ctx := context.Background()

	nsStream, err := sess.PublishNamespace(ctx, &message.PublishNamespace{
		Namespace: wire.Namespace("rooms", "room-42"),
	})
	if err != nil {
		return
	}
	defer nsStream.Close()
}

// Discovering tracks under a prefix (§10.18): NAMESPACE / NAMESPACE_DONE
// arrive on the returned stream as tracks come and go.
func ExampleSession_SubscribeNamespace() {
	var sess *session.Session
	ctx := context.Background()

	annStream, err := sess.SubscribeNamespace(ctx, &message.SubscribeNamespace{
		TrackNamespacePrefix: wire.Namespace("rooms"),
	})
	if err != nil {
		return
	}
	defer annStream.Close()

	for {
		msg, err := message.Parse(annStream)
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *message.Namespace:
			fmt.Printf("announced: %v\n", m.TrackNamespaceSuffix)
		case *message.NamespaceDone:
			fmt.Printf("done: %v\n", m.TrackNamespaceSuffix)
		}
	}
}

// Accepting requests on the server side. A server (or relay) pulls inbound
// request streams with AcceptRequest, type-switches on req.First, and answers
// with Reply or RejectError. AcceptRequest enforces the §10.1 Request-ID
// parity/monotonicity rules and resolves AUTHORIZATION_TOKEN params into
// req.Tokens before returning.
func ExampleSession_AcceptRequest() {
	var sess *session.Session
	ctx := context.Background()

	for {
		req, err := sess.AcceptRequest(ctx)
		if err != nil {
			return
		}
		switch req.First.(type) {
		case *message.Subscribe:
			// AcceptSubscribe assigns a Track Alias, replies SUBSCRIBE_OK, and
			// returns a Publication to push objects on (its OpenSubgroup is
			// pre-bound to the alias).
			pub, err := req.AcceptSubscribe(nil)
			if err != nil {
				return
			}
			_ = pub // ... push subgroups, then pub.Done(...) when finished.
		case *message.Publish:
			// AcceptPublish registers the Track Alias (§11.1) and replies
			// REQUEST_OK; objects arrive via Session.AcceptDataStream.
			recv, err := req.AcceptPublish()
			if err != nil {
				return
			}
			_ = recv // recv.TrackAlias() / recv.Update(...) for follow-ups.
		case *message.Fetch:
			// AcceptFetch replies FETCH_OK and returns a responder whose
			// OpenFetchStream is pre-bound to this fetch's Request ID.
			resp, err := req.AcceptFetch(nil)
			if err != nil {
				return
			}
			_ = resp // resp.OpenFetchStream() to stream the response objects.
		case *message.TrackStatus:
			// AcceptTrackStatus replies TRACK_STATUS_OK. It is a one-shot status
			// query, so (unlike the others) it returns no handle.
			_ = req.AcceptTrackStatus(nil)
		case *message.PublishNamespace:
			// AcceptPublishNamespace replies REQUEST_OK; NAMESPACE / NAMESPACE_DONE
			// follow-ups arrive by reading the returned handle (e.g. message.Parse).
			ann, err := req.AcceptPublishNamespace()
			if err != nil {
				return
			}
			_ = ann
		case *message.SubscribeNamespace:
			// AcceptSubscribeNamespace replies REQUEST_OK; announce matching
			// namespaces by writing NAMESPACE / NAMESPACE_DONE to the handle.
			sub, err := req.AcceptSubscribeNamespace()
			if err != nil {
				return
			}
			_ = sub
		case *message.SubscribeTracks:
			// AcceptSubscribeTracks replies REQUEST_OK; forward matching tracks as
			// PUBLISHes and signal stream exhaustion with WritePublishBlocked.
			tracks, err := req.AcceptSubscribeTracks()
			if err != nil {
				return
			}
			_ = tracks // tracks.WritePublishBlocked(...) on §6.1 stream exhaustion.
		}
	}
}

// Graceful shutdown (§3.5). The server sends GOAWAY with an optional
// new-session URI and a drain timeout.
func ExampleSession_SendGoaway() {
	var sess *session.Session
	_ = sess.SendGoaway(5*time.Second, "moqt://relay-2.example/")
}

// The client reacts to a server GOAWAY via a callback — typically to dial the
// new URI and re-issue its subscriptions before the old session closes.
func ExampleSession_OnGoaway() {
	var sess *session.Session
	sess.OnGoaway(func(g *message.Goaway) {
		fmt.Printf("server going away: uri=%q timeout=%d\n",
			g.NewSessionURI, g.Timeout)
	})
}
