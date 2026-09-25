package session

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// TrackStatusRequest is a completed TRACK_STATUS request (§10.15), returned by
// [Session.TrackStatus] with the peer's TRACK_STATUS_OK. TRACK_STATUS cannot be
// updated — "the subscriber cannot send REQUEST_UPDATE" (§10.15) — so the
// requester has already FINned its side of the stream, and nothing but the
// responder's FIN follows the OK. Close stops reading the stream.
type TrackStatusRequest struct {
	Stream

	// OK is the TRACK_STATUS_OK the peer replied with.
	OK *message.TrackStatusOK
}

// Close releases the stream's receive side. Its send side was FINned when the
// response arrived.
func (t *TrackStatusRequest) Close() error {
	t.Stream.CancelRead(uint64(moqt.StreamResetCancelled))
	return nil
}

// AcceptTrackStatus accepts an inbound TRACK_STATUS (§10.15) and replies
// TRACK_STATUS_OK with the given status fields — the accept-side counterpart of
// [Session.TrackStatus]. r.First MUST be a *message.TrackStatus.
//
// ok carries the TRACK_STATUS_OK fields (status, largest location, Track
// Properties — [message.TrackStatusOK] is an alias of [message.RequestOK]); it
// may be nil for the all-default reply. TRACK_STATUS is a one-shot status
// query: the stream "is closed with a FIN after TRACK_STATUS_OK or
// REQUEST_ERROR are sent" (§10.15), so no handle is returned.
//
// The requester "sends TRACK_STATUS as the first and only message" (§10.15).
// A REQUEST_UPDATE after it MUST close the session with PROTOCOL_VIOLATION
// (§10.9), as must an unknown or malformed message (§10). Closing on any other
// well-formed message is this implementation's reading of "only message"; the
// draft names no consequence for it.
func (r *Request) AcceptTrackStatus(ok *message.TrackStatusOK) error {
	if _, isTS := r.First.(*message.TrackStatus); !isTS {
		return fmt.Errorf("moqt/session: AcceptTrackStatus on a %s request", r.First.Type())
	}
	if ok == nil {
		ok = &message.TrackStatusOK{}
	}
	if err := message.Marshal(r.Stream, ok); err != nil {
		// As in RejectError: a response that cannot be written must not
		// leave the requester waiting on a stream that looks healthy.
		resetStream(r.Stream)
		return fmt.Errorf("moqt/session: write TRACK_STATUS_OK: %w", err)
	}
	if err := r.Stream.Close(); err != nil {
		return fmt.Errorf("moqt/session: FIN TRACK_STATUS stream: %w", err)
	}
	go r.rejectTrackStatusFollowups()
	return nil
}

// rejectTrackStatusFollowups reads the requester's side of an answered
// TRACK_STATUS stream until it ends. Anything arriving there closes the session
// (see [Request.AcceptTrackStatus]): a message, or bytes that do not parse. It
// returns quietly on the requester's FIN, a stream reset, or session close — so
// a requester that never FINs holds it only until the session ends.
func (r *Request) rejectTrackStatusFollowups() {
	src := &readErrRecorder{r: r.Stream}
	msg, err := message.Parse(src)
	switch {
	case errors.Is(err, io.EOF), src.err != nil && !errors.Is(src.err, io.EOF):
		return // requester FIN, or the stream/session went away
	case err != nil:
		_ = r.s.closeProtocolViolation(fmt.Errorf(
			"moqt/session: malformed data on a TRACK_STATUS request stream: %w", err))
	default:
		_ = r.s.closeProtocolViolation(fmt.Errorf(
			"moqt/session: %s on a TRACK_STATUS request stream", msg.Type()))
	}
}

// readErrRecorder remembers the last non-nil error its reader returned, so a
// parse failure can be told apart from a transport failure beneath it.
type readErrRecorder struct {
	r   io.Reader
	err error
}

func (e *readErrRecorder) Read(p []byte) (int, error) {
	n, err := e.r.Read(p)
	if err != nil {
		e.err = err
	}
	return n, err
}

// TrackStatus opens a TRACK_STATUS request stream (§10.15) and awaits
// REQUEST_OK (TRACK_STATUS_OK) or REQUEST_ERROR. The session assigns
// m.RequestID; the caller supplies Namespace, Name, and optional Parameters.
//
// On success a [TrackStatusRequest] is returned whose OK holds the parsed
// TRACK_STATUS_OK. The request cannot be updated, so this side of the stream is
// FINned once the response arrives (§3.3.2: a requester "MAY FIN immediately
// after sending a message if it will not send a REQUEST_UPDATE"). On
// REQUEST_ERROR the stream is closed and a *RequestRejectedError is returned.
func (s *Session) TrackStatus(ctx context.Context, m *message.TrackStatus) (*TrackStatusRequest, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.RequestOK) (*TrackStatusRequest, error) {
			// §2.5.1: reject tracks with unknown mandatory track properties.
			// TRACK_STATUS_OK carries the same Track Properties as SUBSCRIBE_OK.
			if err := s.validateTrackProperties(ok.TrackProperties, "TRACK_STATUS_OK"); err != nil {
				_ = stream.Close()
				return nil, err
			}
			if err := stream.Close(); err != nil {
				return nil, fmt.Errorf("moqt/session: FIN TRACK_STATUS stream: %w", err)
			}
			return &TrackStatusRequest{Stream: stream, OK: ok}, nil
		})
}
