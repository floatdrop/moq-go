package session

import (
	"context"
	"errors"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// NamespacePublication is an established PUBLISH_NAMESPACE request (§9.14). It
// embeds the still-open request stream (so Close / writes / message.Marshal work
// directly on it) and carries the peer's REQUEST_OK. The namespace stays
// published until the request is cancelled ([NamespacePublication.Close],
// §4.1, §6.4.2.3); a FIN does not withdraw it (§6.4.2.2). NAMESPACE and
// NAMESPACE_DONE answer a SUBSCRIBE_NAMESPACE instead (§9.16, §9.17).
type NamespacePublication struct {
	// Stream is the PUBLISH_NAMESPACE request stream, still open for
	// REQUEST_UPDATE follow-ups (§9.5). [NamespacePublication.Close]
	// withdraws the publication.
	Stream

	// OK is the REQUEST_OK the peer replied with.
	OK *message.RequestOK
}

// Close withdraws the namespace by cancelling the request (§4.1, §6.4.2.3).
func (p *NamespacePublication) Close() error {
	cancelRequest(p.Stream)
	return nil
}

// NamespaceSubscription is an established SUBSCRIBE_NAMESPACE request (§9.15).
// It embeds the still-open request stream and carries the peer's REQUEST_OK;
// NAMESPACE / NAMESPACE_DONE notifications arrive on the embedded stream. Read
// it with its [NamespaceSubscription.Broker]'s [RequestBroker.Serve], which
// enforces the session-level rules: §9 and §9.20.1, a NAMESPACE_DONE only
// after its NAMESPACE (§4.2), and no PUBLISH_STATE_NOTIFY or REQUEST_UPDATE
// from the publisher (§9.5, §9.10). The NAMESPACE_DONE check follows a
// TRACK_NAMESPACE_PREFIX update from the REQUEST_OK that accepts it (§9.5.2).
// A caller using message.Parse must apply these rules itself.
// [NamespaceSubscription.Update] modifies the subscription (§9.5); Close ends
// it (§4.2, §6.4.2.3).
type NamespaceSubscription struct {
	// requestHandle carries the SUBSCRIBE_NAMESPACE request stream, still
	// open to receive NAMESPACE / NAMESPACE_DONE notifications.
	requestHandle

	// OK is the REQUEST_OK the peer replied with.
	OK *message.RequestOK
}

// TrackSubscription is an established SUBSCRIBE_TRACKS request (§9.18). It
// embeds the still-open request stream and carries the peer's REQUEST_OK.
// Follow-up PUBLISH_SKIPPED notifications are read via
// [TrackSubscription.ReadPublishSkipped], or its [TrackSubscription.Broker]'s
// [RequestBroker.Serve], never both at once. Both close the session on a
// PUBLISH_STATE_NOTIFY or REQUEST_UPDATE from the publisher (§9.5, §9.10).
// [TrackSubscription.Update] modifies the subscription (§9.5); Close ends it
// (§3.6, §6.4.2.3).
type TrackSubscription struct {
	// requestHandle carries the SUBSCRIBE_TRACKS request stream, still open
	// to receive PUBLISH_SKIPPED follow-ups.
	requestHandle

	// OK is the REQUEST_OK the peer replied with.
	OK *message.RequestOK
}

// Update sends a REQUEST_UPDATE (§9.5), e.g. a new TRACK_NAMESPACE_PREFIX
// (§9.5.2), through the subscription's [NamespaceSubscription.Broker], whose
// Serve must be running to deliver the answer. Reading the answer directly
// would take a NAMESPACE for it.
func (n *NamespaceSubscription) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	return n.Broker().Update(ctx, params)
}

// Update sends a REQUEST_UPDATE (§9.5) through the subscription's
// [TrackSubscription.Broker], whose Serve must be running to deliver the
// answer, as for [NamespaceSubscription.Update].
func (t *TrackSubscription) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	return t.Broker().Update(ctx, params)
}

// PublishNamespace opens a PUBLISH_NAMESPACE request stream (§9.14) and
// awaits REQUEST_OK or REQUEST_ERROR. The session assigns m.RequestID; the
// caller supplies Namespace and optional Parameters.
//
// On success a [NamespacePublication] is returned whose embedded stream stays
// open (the caller may send REQUEST_UPDATE on it, §9.5). On REQUEST_ERROR the
// stream is closed and a *RequestRejectedError is returned.
func (s *Session) PublishNamespace(
	ctx context.Context,
	m *message.PublishNamespace,
) (*NamespacePublication, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.RequestOK) (*NamespacePublication, error) {
			return &NamespacePublication{Stream: stream, OK: ok}, nil
		})
}

// SubscribeNamespace opens a SUBSCRIBE_NAMESPACE request stream (§9.15) and
// awaits REQUEST_OK or REQUEST_ERROR. The session assigns m.RequestID; the
// caller supplies TrackNamespacePrefix and optional Parameters.
//
// On success a [NamespaceSubscription] is returned whose embedded stream stays
// open (the caller will receive NAMESPACE / NAMESPACE_DONE messages on it). On
// REQUEST_ERROR the stream is closed and a *RequestRejectedError is returned;
// any other first response closes the session with PROTOCOL_VIOLATION (§4.2).
func (s *Session) SubscribeNamespace(
	ctx context.Context,
	m *message.SubscribeNamespace,
) (*NamespaceSubscription, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.RequestOK) (*NamespaceSubscription, error) {
			return &NamespaceSubscription{
				Stream: stream, s: s, requestID: m.RequestID,
				namespaces: true, nsPrefix: m.TrackNamespacePrefix, OK: ok,
			}, nil
		})
}

// SubscribeTracks opens a SUBSCRIBE_TRACKS request stream (§9.18) and awaits
// REQUEST_OK or REQUEST_ERROR. The session assigns m.RequestID; the caller
// supplies TrackNamespacePrefix and optional Parameters.
//
// On success a [TrackSubscription] is returned whose embedded stream stays open
// for PUBLISH_SKIPPED follow-ups (read via [TrackSubscription.ReadPublishSkipped]).
// On REQUEST_ERROR the stream is closed and a *RequestRejectedError is returned;
// any other first response closes the session with PROTOCOL_VIOLATION (§3.6).
func (s *Session) SubscribeTracks(ctx context.Context, m *message.SubscribeTracks) (*TrackSubscription, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.RequestOK) (*TrackSubscription, error) {
			if err := s.checkTrackPropertyValues(ok.TrackProperties, "SUBSCRIBE_TRACKS_OK"); err != nil {
				cancelRequest(stream)
				return nil, err
			}
			return &TrackSubscription{Stream: stream, s: s, requestID: m.RequestID, OK: ok}, nil
		})
}

// IncomingNamespacePublication is an accepted inbound PUBLISH_NAMESPACE (§9.14)
// — the receiving side of [Session.PublishNamespace]'s [NamespacePublication],
// returned by [Request.AcceptPublishNamespace]. REQUEST_OK has been sent; the
// announcer's follow-ups (REQUEST_UPDATE, GOAWAY) arrive on the embedded
// stream. Read it with a [Session.NewRequestBroker]'s [RequestBroker.Serve],
// which closes the session on a malformed message (§9) or a GOAWAY violation
// (§9.2). Before Serve, call [RequestBroker.PeerMessages](true, false), since
// PUBLISH_STATE_NOTIFY applies only to subscriptions (§9.10), and
// [RequestBroker.UpdateScope](message.ScopeUpdatePublishNamespace), so an
// update's parameters are checked (§9.20.1) before it is answered; without
// [RequestBroker.HandleUpdates] each REQUEST_UPDATE is declined with
// NOT_SUPPORTED. Cancel the request (CancelRead and CancelWrite, §6.4.2.3) to
// revoke acceptance (§4.1); Close only FINs this side (§6.4.2.2).
type IncomingNamespacePublication struct {
	// Stream is the PUBLISH_NAMESPACE request stream, still open to receive
	// the announcer's follow-ups.
	Stream
}

// IncomingNamespaceSubscription is an accepted inbound SUBSCRIBE_NAMESPACE
// (§4.2) — the announcing side of [Session.SubscribeNamespace]'s
// [NamespaceSubscription], returned by [Request.AcceptSubscribeNamespace].
// REQUEST_OK has been sent; the caller announces matching namespaces by writing
// NAMESPACE / NAMESPACE_DONE to the embedded stream (e.g. via message.Marshal).
// Close it to end the subscription.
type IncomingNamespaceSubscription struct {
	// Stream is the SUBSCRIBE_NAMESPACE request stream, still open for
	// NAMESPACE / NAMESPACE_DONE follow-ups. Close it to end the subscription.
	Stream
}

// IncomingTrackSubscription is an accepted inbound SUBSCRIBE_TRACKS (§9.18) —
// the publishing side of [Session.SubscribeTracks]'s [TrackSubscription],
// returned by [Request.AcceptSubscribeTracks]. REQUEST_OK has been sent; the
// publisher forwards matching tracks as PUBLISH requests on new streams (see
// [Session.OpenPublish]) and signals stream exhaustion with
// [IncomingTrackSubscription.WritePublishSkipped] (§3.6.3 / §9.19). Close it to
// end the subscription.
type IncomingTrackSubscription struct {
	// Stream is the SUBSCRIBE_TRACKS request stream, still open for
	// PUBLISH_SKIPPED follow-ups. Close it to end the subscription.
	Stream
}

// WritePublishSkipped sends a PUBLISH_SKIPPED (§3.6.3 / §9.19) on the
// SUBSCRIBE_TRACKS stream, telling the subscriber the publisher could not open a
// PUBLISH stream for the named track because it has no available bidirectional
// streams. It is the publisher-side counterpart of
// [TrackSubscription.ReadPublishSkipped].
func (t *IncomingTrackSubscription) WritePublishSkipped(pb *message.PublishSkipped) error {
	return message.Marshal(t.Stream, pb)
}

// acceptNamespaceRequest is the shared accept path of the three namespace
// requests (§9.14 / §4.2 / §3.6): assert the request's first message is
// of type M, reply the all-default REQUEST_OK, and hand the still-open
// stream to wrap. op names the caller for error messages.
func acceptNamespaceRequest[M message.Message, T any](r *Request, op string, wrap func(Stream) T) (T, error) {
	var zero T
	if _, ok := r.First.(M); !ok {
		return zero, fmt.Errorf("moqt/session: %s on a %s request", op, r.First.Type())
	}
	if err := r.Reply(&message.RequestOK{}); err != nil {
		return zero, fmt.Errorf("moqt/session: %s: write REQUEST_OK: %w", op, err)
	}
	return wrap(r.Stream), nil
}

// AcceptPublishNamespace accepts an inbound PUBLISH_NAMESPACE (§9.14), replies
// REQUEST_OK, and returns an [IncomingNamespacePublication] for receiving the
// announcer's follow-ups — the accept-side counterpart of
// [Session.PublishNamespace]. r.First MUST be a
// *message.PublishNamespace.
func (r *Request) AcceptPublishNamespace() (*IncomingNamespacePublication, error) {
	return acceptNamespaceRequest[*message.PublishNamespace](r, "AcceptPublishNamespace",
		func(s Stream) *IncomingNamespacePublication { return &IncomingNamespacePublication{Stream: s} })
}

// AcceptSubscribeNamespace accepts an inbound SUBSCRIBE_NAMESPACE (§9.15),
// replies REQUEST_OK, and returns an [IncomingNamespaceSubscription] for
// announcing matching namespaces via NAMESPACE / NAMESPACE_DONE — the
// accept-side counterpart of [Session.SubscribeNamespace]. r.First MUST be a
// *message.SubscribeNamespace.
func (r *Request) AcceptSubscribeNamespace() (*IncomingNamespaceSubscription, error) {
	return acceptNamespaceRequest[*message.SubscribeNamespace](r, "AcceptSubscribeNamespace",
		func(s Stream) *IncomingNamespaceSubscription { return &IncomingNamespaceSubscription{Stream: s} })
}

// AcceptSubscribeTracks accepts an inbound SUBSCRIBE_TRACKS (§9.18), replies
// REQUEST_OK, and returns an [IncomingTrackSubscription] for forwarding matching
// PUBLISHes and sending PUBLISH_SKIPPED follow-ups — the accept-side counterpart
// of [Session.SubscribeTracks]. r.First MUST be a *message.SubscribeTracks.
func (r *Request) AcceptSubscribeTracks() (*IncomingTrackSubscription, error) {
	return acceptNamespaceRequest[*message.SubscribeTracks](r, "AcceptSubscribeTracks",
		func(s Stream) *IncomingTrackSubscription { return &IncomingTrackSubscription{Stream: s} })
}

// ReadPublishSkipped reads the next follow-up message on this SUBSCRIBE_TRACKS
// response stream and returns it as a PUBLISH_SKIPPED.
//
// This is the subscriber side of §3.6.3 / §9.19. After the initial REQUEST_OK,
// the publisher sends PUBLISH_SKIPPED on this stream when it cannot open a
// PUBLISH stream for a matching track because it has no available
// bidirectional streams. (Forwarded PUBLISHes themselves arrive on their own
// new bidi streams via [Session.AcceptRequest], not here.) The returned
// message names the track the publisher couldn't push; the caller's sanctioned
// recovery is to issue an explicit SUBSCRIBE for it.
//
// It cannot be combined with [TrackSubscription.Update], whose answer only the
// broker's Serve delivers.
//
// It blocks until a PUBLISH_SKIPPED arrives or the stream ends. A single
// GOAWAY is skipped (§9.2); a caller that would re-issue the request at its
// New Session URI reads with the broker instead, whose Serve hands it over. A
// second GOAWAY, and a PUBLISH_STATE_NOTIFY or REQUEST_UPDATE from the
// publisher (§9.5, §9.10), close the session with PROTOCOL_VIOLATION. Any
// other message is reported as an error, as is the underlying read error (e.g.
// io.EOF when the publisher FINs the SUBSCRIBE_TRACKS stream).
func (t *TrackSubscription) ReadPublishSkipped() (*message.PublishSkipped, error) {
	for {
		m, err := message.Parse(t)
		if errors.Is(err, message.ErrMalformedMessage) {
			return nil, t.s.closeProtocolViolation(fmt.Errorf("moqt/session: read SUBSCRIBE_TRACKS follow-up: %w", err))
		}
		if err != nil {
			return nil, fmt.Errorf("moqt/session: read SUBSCRIBE_TRACKS follow-up: %w", err)
		}
		switch m := m.(type) {
		case *message.PublishSkipped:
			return m, nil
		case *message.Goaway:
			// §9.2: legal once; the request is not migrated.
			if err := t.goaways.Received(t.s, m); err != nil {
				return nil, err
			}
		case *message.PublishStateNotify, *message.RequestUpdate:
			// §9.10: PUBLISH_STATE_NOTIFY "applies only to subscriptions";
			// §9.5: the publisher did not send this request.
			return nil, t.s.closeProtocolViolation(
				fmt.Errorf("moqt/session: %s on a SUBSCRIBE_TRACKS stream", m.Type()))
		default:
			return nil, fmt.Errorf(
				"moqt/session: unexpected %s on SUBSCRIBE_TRACKS stream, want PUBLISH_SKIPPED",
				m.Type(),
			)
		}
	}
}
