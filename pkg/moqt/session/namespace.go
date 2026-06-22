package session

import (
	"context"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// NamespacePublication is an established PUBLISH_NAMESPACE request (§10.15). It
// embeds the still-open request stream (so Close / writes / message.Marshal work
// directly on it) and carries the peer's REQUEST_OK. The caller announces tracks
// by writing NAMESPACE / NAMESPACE_DONE follow-ups to the embedded stream.
type NamespacePublication struct {
	// Stream is the PUBLISH_NAMESPACE request stream, still open for
	// NAMESPACE / NAMESPACE_DONE follow-ups. Close it to end the publication.
	Stream

	// OK is the REQUEST_OK the peer replied with.
	OK *message.RequestOK
}

// NamespaceSubscription is an established SUBSCRIBE_NAMESPACE request (§10.18).
// It embeds the still-open request stream and carries the peer's REQUEST_OK;
// NAMESPACE / NAMESPACE_DONE notifications arrive by reading the embedded stream
// (e.g. via message.Parse).
type NamespaceSubscription struct {
	// Stream is the SUBSCRIBE_NAMESPACE request stream, still open to receive
	// NAMESPACE / NAMESPACE_DONE notifications. Close it to end the subscription.
	Stream

	// OK is the REQUEST_OK the peer replied with.
	OK *message.RequestOK
}

// TrackSubscription is an established SUBSCRIBE_TRACKS request (§10.19). It
// embeds the still-open request stream and carries the peer's REQUEST_OK.
// Follow-up PUBLISH_BLOCKED notifications are read via
// [TrackSubscription.ReadPublishBlocked].
type TrackSubscription struct {
	// Stream is the SUBSCRIBE_TRACKS request stream, still open to receive
	// PUBLISH_BLOCKED follow-ups. Close it to end the subscription.
	Stream

	// OK is the REQUEST_OK the peer replied with.
	OK *message.RequestOK
}

// PublishNamespace opens a PUBLISH_NAMESPACE request stream (§10.15) and
// awaits REQUEST_OK or REQUEST_ERROR. The session assigns m.RequestID; the
// caller supplies Namespace and optional Parameters.
//
// On success a [NamespacePublication] is returned whose embedded stream stays
// open (the caller may send NAMESPACE / NAMESPACE_DONE messages on it). On
// REQUEST_ERROR the stream is closed and a *RequestRejectedError is returned.
func (s *Session) PublishNamespace(
	ctx context.Context,
	m *message.PublishNamespace,
) (*NamespacePublication, error) {
	stream, err := s.openAllocRequest(m)
	if err != nil {
		return nil, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: read PUBLISH_NAMESPACE response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		return &NamespacePublication{Stream: stream, OK: r}, nil
	case *message.RequestError:
		_ = stream.Close()
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: unexpected %s in PUBLISH_NAMESPACE response", resp.Type())
	}
}

// SubscribeNamespace opens a SUBSCRIBE_NAMESPACE request stream (§10.18) and
// awaits REQUEST_OK or REQUEST_ERROR. The session assigns m.RequestID; the
// caller supplies TrackNamespacePrefix and optional Parameters.
//
// On success a [NamespaceSubscription] is returned whose embedded stream stays
// open (the caller will receive NAMESPACE / NAMESPACE_DONE messages on it). On
// REQUEST_ERROR the stream is closed and a *RequestRejectedError is returned.
func (s *Session) SubscribeNamespace(
	ctx context.Context,
	m *message.SubscribeNamespace,
) (*NamespaceSubscription, error) {
	stream, err := s.openAllocRequest(m)
	if err != nil {
		return nil, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: read SUBSCRIBE_NAMESPACE response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		return &NamespaceSubscription{Stream: stream, OK: r}, nil
	case *message.RequestError:
		_ = stream.Close()
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: unexpected %s in SUBSCRIBE_NAMESPACE response", resp.Type())
	}
}

// SubscribeTracks opens a SUBSCRIBE_TRACKS request stream (§10.19) and awaits
// REQUEST_OK or REQUEST_ERROR. The session assigns m.RequestID; the caller
// supplies TrackNamespacePrefix and optional Parameters.
//
// On success a [TrackSubscription] is returned whose embedded stream stays open
// for PUBLISH_BLOCKED follow-ups (read via [TrackSubscription.ReadPublishBlocked]).
// On REQUEST_ERROR the stream is closed and a *RequestRejectedError is returned.
func (s *Session) SubscribeTracks(ctx context.Context, m *message.SubscribeTracks) (*TrackSubscription, error) {
	stream, err := s.openAllocRequest(m)
	if err != nil {
		return nil, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: read SUBSCRIBE_TRACKS response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		return &TrackSubscription{Stream: stream, OK: r}, nil
	case *message.RequestError:
		_ = stream.Close()
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: unexpected %s in SUBSCRIBE_TRACKS response", resp.Type())
	}
}

// ReadPublishBlocked reads the next follow-up message on this SUBSCRIBE_TRACKS
// response stream and returns it as a PUBLISH_BLOCKED.
//
// This is the subscriber side of §6.1 / §10.20. After the initial REQUEST_OK,
// the publisher sends PUBLISH_BLOCKED on this stream when it cannot open a
// PUBLISH stream for a matching track because it has no available
// bidirectional streams. (Forwarded PUBLISHes themselves arrive on their own
// new bidi streams via [Session.AcceptRequest], not here.) The returned
// message names the track the publisher couldn't push; the caller's sanctioned
// recovery is to issue an explicit SUBSCRIBE for it.
//
// It blocks until a message arrives or the stream ends. A non-PUBLISH_BLOCKED
// message is reported as an error, as is the underlying read error (e.g.
// io.EOF when the publisher FINs the SUBSCRIBE_TRACKS stream).
func (t *TrackSubscription) ReadPublishBlocked() (*message.PublishBlocked, error) {
	m, err := message.Parse(t.Stream)
	if err != nil {
		return nil, fmt.Errorf("moqt/session: read SUBSCRIBE_TRACKS follow-up: %w", err)
	}
	pb, ok := m.(*message.PublishBlocked)
	if !ok {
		return nil, fmt.Errorf("moqt/session: unexpected %s on SUBSCRIBE_TRACKS stream, want PUBLISH_BLOCKED", m.Type())
	}
	return pb, nil
}
