package session

import (
	"context"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// TrackStatusRequest is an established TRACK_STATUS request (§10.14). It owns
// the request stream (embedded, so Close / writes / message.Marshal work
// directly on it) plus the Request ID follow-up traffic needs, so the caller can
// send REQUEST_UPDATE via [TrackStatusRequest.Update] without holding it
// separately. It carries the peer's TRACK_STATUS_OK and is returned by
// [Session.TrackStatus].
type TrackStatusRequest struct {
	// Stream is the TRACK_STATUS request stream, still open for REQUEST_UPDATE
	// follow-ups. Close it to end the request.
	Stream

	// OK is the TRACK_STATUS_OK the peer replied with.
	OK *message.TrackStatusOK

	s         *Session
	requestID uint64
}

// Update sends a REQUEST_UPDATE (§10.9) on the track-status stream and awaits
// the single REQUEST_OK / REQUEST_ERROR the spec mandates. params carries only
// the fields to change; any parameter omitted keeps its prior value on the peer.
// It is [Session.UpdateRequest] with this request's stream and Request ID filled
// in.
func (t *TrackStatusRequest) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	return t.s.UpdateRequest(ctx, t.Stream, t.requestID, params)
}

// TrackStatus opens a TRACK_STATUS request stream (§10.14) and awaits
// REQUEST_OK (TRACK_STATUS_OK) or REQUEST_ERROR. The session assigns
// m.RequestID; the caller supplies Namespace, Name, and optional Parameters.
//
// On success a [TrackStatusRequest] is returned whose embedded stream stays
// open (the caller may send REQUEST_UPDATE via [TrackStatusRequest.Update]) and
// whose OK holds the parsed TRACK_STATUS_OK. On REQUEST_ERROR the stream is
// closed and a *RequestRejectedError is returned.
//
//nolint:dupl // distinct §10.14 op; shares the mandated open/await-OK skeleton with Fetch (§10.12) but differs in message type and TRACK_STATUS_OK property validation.
func (s *Session) TrackStatus(ctx context.Context, m *message.TrackStatus) (*TrackStatusRequest, error) {
	stream, err := s.openAllocRequest(m)
	if err != nil {
		return nil, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: read TRACK_STATUS response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		// §2.5.1: reject tracks with unknown mandatory track properties.
		// TRACK_STATUS_OK carries the same Track Properties as SUBSCRIBE_OK.
		if err := s.validateTrackProperties(r.TrackProperties, "TRACK_STATUS_OK"); err != nil {
			_ = stream.Close()
			return nil, err
		}
		return &TrackStatusRequest{Stream: stream, OK: r, s: s, requestID: m.RequestID}, nil
	case *message.RequestError:
		_ = stream.Close()
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: unexpected %s in TRACK_STATUS response", resp.Type())
	}
}
