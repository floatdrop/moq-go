package session

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
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
	// requestHandle carries the request stream — still open for follow-up
	// traffic: PUBLISH_DONE, REQUEST_UPDATE, etc.; Close it to FIN the
	// publication — and provides Update and Broker. Serving subscriber
	// REQUEST_UPDATEs on a long-lived publication is what
	// [requestHandle.Broker] + [RequestBroker.Serve] are for.
	//
	// §10.9 permits REQUEST_UPDATE only from the request's sender, plus
	// the subscriber of a PUBLISH-established subscription — so Update is
	// valid on a Publication from [Session.Publish] (this side sent the
	// PUBLISH) but NOT on one from [Request.AcceptSubscribe], where this
	// side is the publisher answering the peer's SUBSCRIBE.
	requestHandle

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
func (p *Publication) OpenSubgroup(h message.SubgroupHeader) (*OutgoingSubgroupStream, error) {
	h.TrackAlias = p.alias
	sg, err := p.s.OpenSubgroup(h)
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
	if err := p.writeThenClose(&message.PublishDone{
		StatusCode:  code,
		StreamCount: p.subgroupCount.Load(),
		ErrorReason: reason,
	}); err != nil {
		return fmt.Errorf("moqt/session: write PUBLISH_DONE: %w", err)
	}
	return nil
}

// IncomingPublication is the receiving side of a publisher-initiated PUBLISH
// (§10.10) this endpoint accepted via [Request.AcceptPublish] — the accept-side
// counterpart of [Session.Subscribe]'s [Subscription]. The objects arrive on
// subgroup uni-streams (or datagrams) keyed by [IncomingPublication.TrackAlias]
// and are consumed via [Session.AcceptDataStream]; the embedded request stream
// stays open for follow-ups — PUBLISH_DONE from the publisher, or a
// REQUEST_UPDATE this side sends via [IncomingPublication.Update] to adjust
// forwarding (§10.9). Close it to end the reception.
type IncomingPublication struct {
	// requestHandle carries the PUBLISH request stream — still open for
	// follow-up traffic (inbound PUBLISH_DONE, outbound REQUEST_UPDATE;
	// Close it to end the reception) — and provides Update.
	requestHandle

	alias uint64
}

// TrackAlias reports the §11.1 Track Alias the publisher assigned — the integer
// inbound subgroup and datagram streams carry to identify this track (resolve it
// via [Session.LookupInboundTrackAlias]).
func (p *IncomingPublication) TrackAlias() uint64 { return p.alias }

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
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, _ *message.RequestOK) (*Publication, error) {
			return &Publication{
				Stream:    stream,
				s:         s,
				requestID: m.RequestID,
				alias:     m.TrackAlias,
			}, nil
		})
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
// lets the caller react to an exhausted limit by sending PUBLISH_SKIPPED (§6.1,
// §10.20) instead. On success it assigns m.RequestID, writes the PUBLISH as the
// stream's first message, and returns the still-open bidi stream so the caller
// can read the peer's REQUEST_OK / REQUEST_ERROR and send follow-ups (subgroup
// streams, PUBLISH_DONE, REQUEST_UPDATE).
func (s *Session) OpenPublish(m *message.Publish) (Stream, error) {
	return s.openAllocRequest(m)
}
