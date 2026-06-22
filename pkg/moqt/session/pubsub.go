package session

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

// Publication is a live track this side publishes objects on. It owns the
// request stream (embedded, so Close / writes / message.Marshal work directly on
// it) and the Track Alias the session assigned, and it opens subgroup
// uni-streams for the track via [Publication.OpenSubgroup] without the caller
// having to thread the alias around. It is returned both by [Session.Publish]
// (publisher-initiated, the PUBLISH side) and by [Request.AcceptSubscribe]
// (answering an inbound SUBSCRIBE) — in both cases this endpoint is the one
// sending objects.
type Publication struct {
	// Stream is the PUBLISH request stream, still open for follow-up
	// traffic: PUBLISH_DONE, REQUEST_UPDATE, etc. Close it to FIN the
	// publication.
	Stream

	s     *Session
	alias uint64

	// subgroupCount counts subgroup streams opened via OpenSubgroup, used as
	// the §10.11 Stream Count when Done sends PUBLISH_DONE.
	subgroupCount atomic.Uint64
}

// TrackAlias reports the §11.1 Track Alias bound to this publication — the
// integer inbound subgroup streams carry to identify the track. It is the
// value the caller supplied in message.Publish.TrackAlias, or, when that was
// the zero value, the one [Session.Publish] allocated via
// [Session.AllocOutboundTrackAlias].
func (p *Publication) TrackAlias() uint64 { return p.alias }

// OpenSubgroup opens an outbound SUBGROUP_HEADER uni-stream (§11.4.2) for this
// publication's track, filling in the Track Alias automatically — h.TrackAlias
// is ignored and overwritten. It is otherwise identical to
// [Session.OpenSubgroup]: the caller MUST Close the returned stream to FIN it
// once all objects are written, or Cancel to reset.
func (p *Publication) OpenSubgroup(ctx context.Context, h message.SubgroupHeader) (*OutgoingSubgroupStream, error) {
	h.TrackAlias = p.alias
	sg, err := p.s.OpenSubgroup(ctx, h)
	if err != nil {
		return nil, err
	}
	p.subgroupCount.Add(1)
	return sg, nil
}

// Done ends the publication (§10.11): it writes a PUBLISH_DONE with the given
// status code and reason, then FINs the request stream. The §10.11 Stream Count
// is set to the number of subgroup streams opened via [Publication.OpenSubgroup]
// so a subscriber knows how many data streams to expect; this is exact only when
// every subgroup was opened through this handle (subgroups opened via
// [Session.OpenSubgroup] directly are not counted — send PUBLISH_DONE yourself
// via message.Marshal if you need a different count).
func (p *Publication) Done(code moqt.PublishDoneCode, reason string) error {
	if err := message.Marshal(p.Stream, &message.PublishDone{
		StatusCode:  code,
		StreamCount: p.subgroupCount.Load(),
		ErrorReason: reason,
	}); err != nil {
		return fmt.Errorf("moqt/session: write PUBLISH_DONE: %w", err)
	}
	return p.Stream.Close()
}

// Publish opens a PUBLISH request stream (§10.10) and awaits the peer's
// initial response. It is [Session.OpenPublish] plus the response wait: the
// session assigns m.RequestID (after the stream opens, so a blocked open
// consumes no ID) and, when m.TrackAlias is the zero value, a Track Alias via
// [Session.AllocOutboundTrackAlias]; the caller supplies Namespace / Name /
// Parameters / TrackProperties. On success a [Publication] is returned whose
// embedded stream stays open for PUBLISH_DONE / REQUEST_UPDATE follow-ups and
// whose [Publication.OpenSubgroup] opens subgroup uni-streams for the track.
// On REQUEST_ERROR the stream is closed and a *RequestRejectedError is
// returned.
//
// To assign the Track Alias yourself (e.g. to mirror an upstream alias), set
// m.TrackAlias before calling — any non-zero value is used as-is — or drop to
// [Session.OpenPublish] for full control over the stream lifecycle.
func (s *Session) Publish(ctx context.Context, m *message.Publish) (*Publication, error) {
	if m.TrackAlias == 0 {
		m.TrackAlias = s.AllocOutboundTrackAlias()
	}
	stream, err := s.OpenPublish(m)
	if err != nil {
		return nil, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: read PUBLISH response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		return &Publication{Stream: stream, s: s, alias: m.TrackAlias}, nil
	case *message.RequestError:
		_ = stream.Close()
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: unexpected %s in PUBLISH response", resp.Type())
	}
}

// OpenPublish opens a PUBLISH request stream (§10.10) without blocking on
// stream-flow-control credit and without awaiting the peer's response. It is
// the relay-side counterpart of [Publish]: relay fan-out is fire-and-continue,
// so the caller owns the stream's read side.
//
// If the peer's stream limit is currently exhausted it returns
// [ErrNoStreamCredit] and consumes NO Request ID — the ID is allocated only
// after the stream is successfully opened (see [Session.openAllocRequest]), so
// a blocked attempt leaves the session's Request ID sequence untouched. This
// lets the caller react to an exhausted limit by sending PUBLISH_BLOCKED (§6.1,
// §10.20) instead. On success it assigns m.RequestID, writes the PUBLISH as the
// stream's first message, and returns the still-open bidi stream so the caller
// can read the peer's REQUEST_OK / REQUEST_ERROR and send follow-ups (subgroup
// streams, PUBLISH_DONE, REQUEST_UPDATE).
func (s *Session) OpenPublish(m *message.Publish) (Stream, error) {
	return s.openAllocRequest(m)
}

// Subscription is a live subscriber-initiated track subscription. It owns the
// request stream (embedded, so Close / reads / message.Marshal work directly
// on it) plus the identifiers follow-up traffic needs — the Request ID and the
// publisher-assigned Track Alias — so the caller can send REQUEST_UPDATE via
// [Subscription.Update] without holding them separately. It is returned by
// [Session.Subscribe].
type Subscription struct {
	// Stream is the SUBSCRIBE request stream, still open for follow-up
	// traffic: REQUEST_UPDATE and inbound PUBLISH_DONE. Close it to end the
	// subscription.
	Stream

	// OK is the parsed SUBSCRIBE_OK response — the publisher-assigned Track
	// Alias, negotiated Parameters, and TrackProperties.
	OK *message.SubscribeOK

	s         *Session
	requestID uint64
}

// TrackAlias reports the §11.1 Track Alias the publisher assigned to this
// subscription — the integer inbound subgroup and datagram streams carry to
// identify the track (see [Session.AcceptDataStream]). It is shorthand for
// sub.OK.TrackAlias.
func (sub *Subscription) TrackAlias() uint64 { return sub.OK.TrackAlias }

// Update sends a REQUEST_UPDATE (§10.9) on the subscription stream and awaits
// the single REQUEST_OK / REQUEST_ERROR the spec mandates. params carries only
// the fields to change; any parameter omitted keeps its prior value on the
// peer. It is [Session.UpdateRequest] with this subscription's stream and
// Request ID filled in.
func (sub *Subscription) Update(ctx context.Context, params message.Parameters) (*message.RequestOK, error) {
	return sub.s.UpdateRequest(ctx, sub.Stream, sub.requestID, params)
}

// Subscribe opens a SUBSCRIBE request stream (§10.7) and awaits SUBSCRIBE_OK.
// The session assigns m.RequestID; the caller supplies the rest. On success a
// [Subscription] is returned whose embedded stream stays open for follow-up
// traffic (REQUEST_UPDATE via [Subscription.Update], inbound PUBLISH_DONE) and
// whose [Subscription.TrackAlias] matches the alias on inbound subgroup
// streams. REQUEST_ERROR is surfaced as a *RequestRejectedError and the stream
// is closed.
func (s *Session) Subscribe(ctx context.Context, m *message.Subscribe) (*Subscription, error) {
	stream, err := s.openAllocRequest(m)
	if err != nil {
		return nil, err
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: read SUBSCRIBE response: %w", err)
	}
	switch r := resp.(type) {
	case *message.SubscribeOK:
		// §2.5.1: reject tracks with unknown mandatory track properties.
		if err := s.validateTrackProperties(r.TrackProperties, "SUBSCRIBE_OK"); err != nil {
			_ = stream.Close()
			return nil, err
		}
		// §11.1: register the alias the publisher assigned so we can detect
		// DUPLICATE_TRACK_ALIAS if the same alias is reused for a different track.
		key := track.NewKey(m.Namespace, m.Name)
		if err := s.RegisterInboundTrackAlias(r.TrackAlias, key); err != nil {
			_ = stream.Close()
			return nil, err
		}
		return &Subscription{Stream: stream, OK: r, s: s, requestID: m.RequestID}, nil
	case *message.RequestError:
		_ = stream.Close()
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		_ = stream.Close()
		return nil, fmt.Errorf("moqt/session: unexpected %s in SUBSCRIBE response", resp.Type())
	}
}

// UpdateRequest sends a REQUEST_UPDATE (§10.9) on an already-established
// request stream and awaits the single REQUEST_OK / REQUEST_ERROR the spec
// mandates in response. requestID MUST be the Request ID of the original
// request the stream carries — REQUEST_UPDATE rides the original bidi stream
// and does NOT consume a new Request ID. params carries only the fields the
// caller wants to change; any parameter omitted keeps its prior value on the
// peer (§10.9).
//
// On REQUEST_OK the parsed message is returned and the stream is left open
// for further traffic. REQUEST_ERROR is surfaced as a *RequestRejectedError;
// the stream is left open so the caller can decide how to tear down (a failed
// subscription update is followed by PUBLISH_DONE from the publisher, §10.9).
func (s *Session) UpdateRequest(
	ctx context.Context,
	stream Stream,
	requestID uint64,
	params message.Parameters,
) (*message.RequestOK, error) {
	if err := message.Marshal(stream, &message.RequestUpdate{
		RequestID:  requestID,
		Parameters: params,
	}); err != nil {
		return nil, fmt.Errorf("moqt/session: write REQUEST_UPDATE: %w", err)
	}
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("moqt/session: read REQUEST_UPDATE response: %w", err)
	}
	switch r := resp.(type) {
	case *message.RequestOK:
		return r, nil
	case *message.RequestError:
		return nil, &RequestRejectedError{Code: r.ErrorCode, Reason: r.ErrorReason}
	default:
		return nil, fmt.Errorf("moqt/session: unexpected %s in REQUEST_UPDATE response", resp.Type())
	}
}
