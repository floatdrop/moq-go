package session

import (
	"context"
	"fmt"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/wire"
)

// FetchRequest is a live FETCH operation. It owns the request stream
// (embedded, so Close / reads / message.Marshal work directly on it) plus the
// Request ID follow-up traffic needs, so the caller can send REQUEST_UPDATE via
// [FetchRequest.Update] without holding it separately. The response objects
// arrive on a separate FETCH_HEADER uni-stream (§11.4.4) via
// [Session.AcceptDataStream], not on the embedded stream. It is returned by
// [Session.Fetch].
type FetchRequest struct {
	// requestHandle carries the FETCH request stream — still open for
	// REQUEST_UPDATE follow-ups (Close it to cancel the fetch) — and
	// provides Update.
	requestHandle

	// OK is the parsed FETCH_OK response — EndOfTrack, EndLocation,
	// negotiated Parameters, and TrackProperties.
	OK *message.FetchOK
}

// Fetch opens a FETCH request stream (§10.13) and awaits FETCH_OK or
// REQUEST_ERROR. The session assigns m.RequestID; the caller supplies the
// track name and, in a LOCATION_FILTER parameter, the range (§5.1.2).
//
// On success a [FetchRequest] is returned whose embedded stream stays open (the
// caller may send REQUEST_UPDATE via [FetchRequest.Update]) and whose OK holds
// the parsed FETCH_OK. The publisher will open a FETCH_HEADER uni-stream (§11.4.4)
// carrying the response objects; the caller receives that via AcceptDataStream.
//
// On REQUEST_ERROR the stream is closed and a *RequestRejectedError is
// returned.
func (s *Session) Fetch(ctx context.Context, m *message.Fetch) (*FetchRequest, error) {
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.FetchOK) (*FetchRequest, error) {
			// §2.5.1: "the subscriber MUST cancel the fetch".
			if err := s.validateTrackProperties(ok.TrackProperties, "FETCH_OK"); err != nil {
				cancelRequest(stream)
				return nil, err
			}
			if err := checkFetchOKEnd(m, ok); err != nil {
				cancelRequest(stream)
				return nil, s.closeProtocolViolation(err)
			}
			// The responder may send neither REQUEST_UPDATE (§10.9) nor
			// PUBLISH_STATE_NOTIFY (§10.10).
			return &FetchRequest{
				Stream:    stream,
				s:         s,
				requestID: m.RequestID,
				OK:        ok,
			}, nil
		})
}

// checkFetchOKEnd enforces §10.14: an End Location "smaller than the Start
// Location" is a PROTOCOL_VIOLATION.
//
// A relative Start is not known here, but FETCH_OK's End never passes the
// Largest Object (§10.14). Next Object and a relative StartGroup of 0 start
// past it, so any End but {0, 0} (no content yet) precedes them; a larger
// relative StartGroup starts at or before it.
func checkFetchOKEnd(m *message.Fetch, ok *message.FetchOK) error {
	// Without a filter the range starts at {0, 0}. Parsed into a local value
	// to avoid an allocation per FETCH.
	p, found := m.Parameters.Find(message.ParamLocationFilter)
	if !found {
		return nil
	}
	var filter message.LocationFilter
	_ = filter.Parse(wire.NewReader(p.Bytes))
	f := &filter
	switch {
	case f.NextObject(), f.RelativeStart() && f.StartGroup == 0:
		if ok.EndLocation != (message.Location{}) {
			return fmt.Errorf("moqt/session: FETCH_OK End Location %v precedes a FETCH that starts "+
				"after the Largest Object", ok.EndLocation)
		}
	case f.RelativeStart():
	default:
		if start := f.Start(message.Location{}, false); ok.EndLocation.Less(start) {
			return fmt.Errorf("moqt/session: FETCH_OK End Location %v precedes the FETCH Start %v",
				ok.EndLocation, start)
		}
	}
	return nil
}

// FetchResponder is the publisher side of a FETCH (§10.13) this endpoint
// accepted via [Request.AcceptFetch] — the accept-side counterpart of
// [Session.Fetch]. FETCH_OK has already been written on the embedded request
// stream; the response objects are streamed on a separate FETCH_HEADER
// uni-stream (§11.4.4) opened via [FetchResponder.OpenFetchStream], which binds
// this fetch's Request ID automatically. The embedded request stream stays open
// for REQUEST_UPDATE follow-ups.
type FetchResponder struct {
	// Stream is the FETCH request stream, still open for REQUEST_UPDATE
	// follow-ups. Close it to end the fetch.
	Stream

	s         *Session
	requestID uint64
}

// OpenFetchStream opens the outbound FETCH_HEADER uni-stream (§11.4.4) carrying
// this fetch's response objects, with the Request ID bound automatically. The
// caller MUST Close the returned stream to FIN it once all objects are written,
// or Cancel to reset. It is [Session.OpenFetchStream] pre-bound to this fetch.
func (f *FetchResponder) OpenFetchStream() (*OutgoingFetchStream, error) {
	return f.s.OpenFetchStream(message.FetchHeader{RequestID: f.requestID})
}

// AcceptFetch accepts an inbound FETCH (§10.13) and returns a [FetchResponder]
// for streaming the response objects — the accept-side counterpart of
// [Session.Fetch]. r.First MUST be a *message.Fetch.
//
// ok carries the FETCH_OK fields the caller wants to set (EndOfTrack,
// EndLocation, negotiated Parameters, TrackProperties); it may be nil for the
// all-default reply. AcceptFetch writes FETCH_OK and returns a responder whose
// [FetchResponder.OpenFetchStream] is pre-bound to this fetch's Request ID.
func (r *Request) AcceptFetch(ok *message.FetchOK) (*FetchResponder, error) {
	f, isFetch := r.First.(*message.Fetch)
	if !isFetch {
		return nil, fmt.Errorf("moqt/session: AcceptFetch on a %s request", r.First.Type())
	}
	if ok == nil {
		ok = &message.FetchOK{}
	}
	if err := message.Marshal(r.Stream, ok); err != nil {
		return nil, fmt.Errorf("moqt/session: write FETCH_OK: %w", err)
	}
	return &FetchResponder{Stream: r.Stream, s: r.s, requestID: f.RequestID}, nil
}

// OpenFetchStream opens an outbound FETCH_HEADER uni-stream (§11.4.4),
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
