package session

import (
	"context"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
)

// FetchRequest is a live FETCH operation. It owns the request stream
// (embedded, so Close / reads / message.Marshal work directly on it) plus the
// Request ID follow-up traffic needs, so the caller can send REQUEST_UPDATE via
// [FetchRequest.Update] without holding it separately. The response objects
// arrive on a separate FETCH_HEADER uni-stream (§11.5) via
// [Session.AcceptDataStream], not on the embedded stream. It is returned by
// [Session.Fetch].
type FetchRequest struct {
	// Stream is the FETCH request stream, still open for REQUEST_UPDATE
	// follow-ups. Close it to cancel the fetch.
	Stream

	// OK is the parsed FETCH_OK response — EndOfTrack, EndLocation,
	// negotiated Parameters, and TrackProperties.
	OK *message.FetchOK

	s         *Session
	requestID uint64
}

// Update sends a REQUEST_UPDATE (§10.9) on the fetch stream and awaits the
// single REQUEST_OK / REQUEST_ERROR the spec mandates. params carries only the
// fields to change; any parameter omitted keeps its prior value on the peer.
// It is [Session.UpdateRequest] with this fetch's stream and Request ID filled
// in.
func (f *FetchRequest) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	return f.s.UpdateRequest(ctx, f.Stream, f.requestID, params)
}

// Fetch opens a FETCH request stream (§10.12) and awaits FETCH_OK or
// REQUEST_ERROR. The session assigns m.RequestID; the caller supplies
// FetchType and the corresponding Standalone or Joining sub-struct.
//
// On success a [FetchRequest] is returned whose embedded stream stays open (the
// caller may send REQUEST_UPDATE via [FetchRequest.Update]) and whose OK holds
// the parsed FETCH_OK. The publisher will open a FETCH_HEADER uni-stream (§11.5)
// carrying the response objects; the caller receives that via AcceptDataStream.
//
// On REQUEST_ERROR the stream is closed and a *RequestRejectedError is
// returned.
func (s *Session) Fetch(ctx context.Context, m *message.Fetch) (*FetchRequest, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.FetchOK) (*FetchRequest, error) {
			// §2.5.1: reject tracks with unknown mandatory track properties.
			if err := s.validateTrackProperties(ok.TrackProperties, "FETCH_OK"); err != nil {
				_ = stream.Close()
				return nil, err
			}
			return &FetchRequest{Stream: stream, OK: ok, s: s, requestID: m.RequestID}, nil
		})
}

// OpenFetchStream opens an outbound FETCH_HEADER uni-stream (§11.5),
// writes the header (Type + Request ID), and returns the body writer. The
// caller MUST Close to FIN the stream once all fetch objects have been
// written, or Cancel to reset.
func (s *Session) OpenFetchStream(h message.FetchHeader) (*OutgoingFetchStream, error) {
	dst, err := s.conn.OpenUniStream()
	if err != nil {
		return nil, err
	}
	if err := message.WriteFetchHeader(dst, h); err != nil {
		dst.CancelWrite(uint64(moqt.StreamResetInternalError))
		return nil, fmt.Errorf("moqt/session: write FETCH_HEADER: %w", err)
	}
	return &OutgoingFetchStream{dst: dst}, nil
}
