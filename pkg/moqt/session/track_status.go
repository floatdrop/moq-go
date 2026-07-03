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
	// requestHandle carries the TRACK_STATUS request stream — still open
	// for REQUEST_UPDATE follow-ups (Close it to end the request) — and
	// provides Update.
	requestHandle

	// OK is the TRACK_STATUS_OK the peer replied with.
	OK *message.TrackStatusOK
}

// AcceptTrackStatus accepts an inbound TRACK_STATUS (§10.14) and replies
// TRACK_STATUS_OK with the given status fields — the accept-side counterpart of
// [Session.TrackStatus]. r.First MUST be a *message.TrackStatus.
//
// ok carries the TRACK_STATUS_OK fields (status, largest location, Track
// Properties — [message.TrackStatusOK] is an alias of [message.RequestOK]); it
// may be nil for the all-default reply. Unlike the other Accept* helpers,
// TRACK_STATUS is a one-shot status query with no object-push or follow-up
// stream, so no handle is returned; use [Request.Stream] directly to service a
// later REQUEST_UPDATE.
func (r *Request) AcceptTrackStatus(ok *message.TrackStatusOK) error {
	if _, isTS := r.First.(*message.TrackStatus); !isTS {
		return fmt.Errorf("moqt/session: AcceptTrackStatus on a %s request", r.First.Type())
	}
	if ok == nil {
		ok = &message.TrackStatusOK{}
	}
	if err := message.Marshal(r.Stream, ok); err != nil {
		return fmt.Errorf("moqt/session: write TRACK_STATUS_OK: %w", err)
	}
	return nil
}

// TrackStatus opens a TRACK_STATUS request stream (§10.14) and awaits
// REQUEST_OK (TRACK_STATUS_OK) or REQUEST_ERROR. The session assigns
// m.RequestID; the caller supplies Namespace, Name, and optional Parameters.
//
// On success a [TrackStatusRequest] is returned whose embedded stream stays
// open (the caller may send REQUEST_UPDATE via [TrackStatusRequest.Update]) and
// whose OK holds the parsed TRACK_STATUS_OK. On REQUEST_ERROR the stream is
// closed and a *RequestRejectedError is returned.
func (s *Session) TrackStatus(ctx context.Context, m *message.TrackStatus) (*TrackStatusRequest, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.RequestOK) (*TrackStatusRequest, error) {
			// §2.5.1: reject tracks with unknown mandatory track properties.
			// TRACK_STATUS_OK carries the same Track Properties as SUBSCRIBE_OK.
			if err := s.validateTrackProperties(ok.TrackProperties, "TRACK_STATUS_OK"); err != nil {
				_ = stream.Close()
				return nil, err
			}
			return &TrackStatusRequest{
				requestHandle: requestHandle{Stream: stream, s: s, requestID: m.RequestID},
				OK:            ok,
			}, nil
		})
}
