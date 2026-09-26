package session

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
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
	// requestHandle carries the request stream. [Publication.Done] ends the
	// publication gracefully (PUBLISH_DONE, then FIN); Close cancels it.
	//
	// §10.9: Update is valid on a Publication from [Session.Publish] but not
	// on one from [Request.AcceptSubscribe], where this side did not send
	// the request.
	requestHandle

	alias uint64

	// subgroupCount counts subgroup streams opened via OpenSubgroup, used as
	// the §10.12 Stream Count when Done sends PUBLISH_DONE.
	subgroupCount atomic.Uint64

	// paused is the inverse of the §5.1 Forward State.
	paused atomic.Bool

	// largest is the largest Location written through OpenSubgroup streams,
	// reported as LARGEST_OBJECT in REQUEST_UPDATE_OK (§10.9.1).
	largestMu  sync.Mutex
	largest    message.Location
	hasLargest bool

	// ended is latched by the first Done, so PUBLISH_DONE is sent once and no
	// subgroup opens after it.
	ended atomic.Bool

	brokerInit sync.Once
}

// ErrForwardPaused is returned while the subscription's Forward State is 0
// (§5.1): [Publication.OpenSubgroup] opens nothing, and a write to an open
// subgroup resets that stream first (§11.4.3). A REQUEST_UPDATE with FORWARD=1
// resumes.
var ErrForwardPaused = errors.New("moqt/session: Forward State is 0; not sending objects")

// ErrPublicationEnded is returned by [Publication.OpenSubgroup] once the
// publication has sent PUBLISH_DONE — by [Publication.Done], or automatically
// after a declined REQUEST_UPDATE (§10.9.1).
var ErrPublicationEnded = errors.New("moqt/session: publication ended (PUBLISH_DONE sent)")

// newPublication builds a Publication whose initial Forward State is the
// establishing message's FORWARD (§5.1), or 1 when omitted (§10.2.18).
func newPublication(s *Session, stream Stream, requestID, alias uint64, establishing message.Parameters) *Publication {
	// The subscriber may send REQUEST_UPDATE (§10.9) but not
	// PUBLISH_STATE_NOTIFY (§10.10).
	p := &Publication{
		Stream: stream, s: s, requestID: requestID, alias: alias,
		peerUpdate: true, updateScope: message.ScopeUpdateFromSubscriber,
	}
	if f, ok := establishing.Find(message.ParamForward); ok {
		p.paused.Store(f.Byte == 0)
	}
	return p
}

// Broker returns the publication's [RequestBroker] (see [requestHandle.Broker])
// with [Publication.ApplyUpdate] installed to decide REQUEST_UPDATEs. A
// declined update ends the subscription with PUBLISH_DONE UPDATE_FAILED
// (§10.9.1). Use [RequestBroker.HandleUpdates] to support more parameters.
func (p *Publication) Broker() *RequestBroker {
	b := p.requestHandle.Broker()
	p.brokerInit.Do(func() {
		b.HandleUpdates(p.ApplyUpdate)
		b.onUpdateFailed = func() {
			_ = p.Done(moqt.PublishDoneUpdateFailed, "REQUEST_UPDATE declined")
		}
	})
	return b
}

// ApplyUpdate is the publication's built-in REQUEST_UPDATE handling (§10.9).
// FORWARD (§10.2.18) sets the Forward State; SUBSCRIBER_PRIORITY,
// NEW_GROUP_REQUEST and AUTHORIZATION_TOKEN are accepted without action (the
// Serve callback still sees the update). Any other parameter declines the
// whole update with NOT_SUPPORTED.
//
// The REQUEST_UPDATE_OK carries LARGEST_OBJECT (§10.9.1) once objects have
// been written through [Publication.OpenSubgroup] streams; other objects are
// not seen.
func (p *Publication) ApplyUpdate(upd *message.RequestUpdate) (*message.RequestOK, error) {
	forward, setForward := false, false
	for _, prm := range upd.Parameters {
		if prm.Type == message.ParamForward {
			// §10.2.18.
			if prm.Byte > 1 {
				return nil, p.s.closeProtocolViolation(
					fmt.Errorf("moqt/session: FORWARD value %d in REQUEST_UPDATE", prm.Byte))
			}
			forward, setForward = prm.Byte == 1, true
			continue
		}
		if !slices.Contains(acceptedUpdateParams, prm.Type) {
			return nil, &RequestRejectedError{
				Code:   moqt.RequestNotSupported,
				Reason: fmt.Sprintf("REQUEST_UPDATE parameter %#x not supported", uint64(prm.Type)),
			}
		}
	}
	if setForward {
		p.paused.Store(!forward)
	}
	ok := &message.RequestOK{}
	p.largestMu.Lock()
	if p.hasLargest {
		ok.Parameters = message.Parameters{message.LargestObjectParam(p.largest.Group, p.largest.Object)}
	}
	p.largestMu.Unlock()
	return ok, nil
}

// acceptedUpdateParams are the parameters, besides FORWARD, that
// [Publication.ApplyUpdate] accepts without further action.
var acceptedUpdateParams = []message.ParamID{
	message.ParamSubscriberPriority,
	message.ParamNewGroupRequest,
	message.ParamAuthorizationToken,
}

// noteObject records a written object for LARGEST_OBJECT.
func (p *Publication) noteObject(group, object uint64) {
	loc := message.Location{Group: group, Object: object}
	p.largestMu.Lock()
	if !p.hasLargest || p.largest.Less(loc) {
		p.largest, p.hasLargest = loc, true
	}
	p.largestMu.Unlock()
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
	if p.ended.Load() {
		return nil, ErrPublicationEnded
	}
	if p.paused.Load() {
		return nil, ErrForwardPaused
	}
	h.TrackAlias = p.alias
	sg, err := p.s.OpenSubgroup(h)
	if err != nil {
		return nil, err
	}
	p.subgroupCount.Add(1)
	sg.onObject = p.noteObject
	sg.paused = p.paused.Load
	return sg, nil
}

// Done ends the publication (§10.12): it writes a PUBLISH_DONE with the given
// status code and reason, then FINs the request stream. The §10.12 Stream Count
// is set to the number of subgroup streams opened via [Publication.OpenSubgroup]
// so a subscriber knows how many data streams to expect; this is exact only when
// every subgroup was opened through this handle (subgroups opened via
// [Session.OpenSubgroup] directly are not counted — send PUBLISH_DONE yourself
// via message.Marshal if you need a different count).
//
// Only the first call sends; later ones return nil.
func (p *Publication) Done(code moqt.PublishDoneCode, reason string) error {
	if !p.ended.CompareAndSwap(false, true) {
		return nil
	}
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
// (§10.11) this endpoint accepted via [Request.AcceptPublish] — the accept-side
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

// Publish opens a PUBLISH request stream (§10.11) and awaits the peer's
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
			// The PUBLISH sets the initial Forward State (§5.1); PUBLISH_OK
			// carries no subscription parameters.
			return newPublication(s, stream, m.RequestID, m.TrackAlias, m.Parameters), nil
		})
}

// OpenPublish opens a PUBLISH request stream (§10.11) without blocking on
// stream-flow-control credit and without awaiting the peer's response. It is
// the relay-side counterpart of [Publish]: relay fan-out is fire-and-continue,
// so the caller owns the stream's read side.
//
// If the peer's stream limit is currently exhausted it returns
// [ErrNoStreamCredit] and consumes NO Request ID — the ID is allocated only
// after the stream is successfully opened (see [Session.openAllocRequest]), so
// a blocked attempt leaves the session's Request ID sequence untouched. This
// lets the caller react to an exhausted limit by sending PUBLISH_SKIPPED (§6.1,
// §10.21) instead. On success it assigns m.RequestID, writes the PUBLISH as the
// stream's first message, and returns the still-open bidi stream so the caller
// can read the peer's REQUEST_OK / REQUEST_ERROR and send follow-ups (subgroup
// streams, PUBLISH_DONE, REQUEST_UPDATE).
func (s *Session) OpenPublish(m *message.Publish) (Stream, error) {
	return s.openAllocRequest(m)
}

// AwaitPublishOK reads the response to a PUBLISH sent with
// [Session.OpenPublish], with the checks [Session.Publish] applies (§10.5,
// §10.2.1). REQUEST_ERROR is returned as a *RequestRejectedError. The stream
// stays open either way.
func (s *Session) AwaitPublishOK(ctx context.Context, stream Stream) (*message.RequestOK, error) {
	resp, err := s.readResponse(ctx, stream)
	if err != nil {
		return nil, fmt.Errorf("moqt/session: read PUBLISH response: %w", err)
	}
	switch m := resp.(type) {
	case *message.RequestOK:
		if err := s.checkRequestOKTrackProperties((*message.Publish)(nil), m); err != nil {
			return nil, err
		}
		if err := s.CheckPeerParams(message.ScopePublishOK, m); err != nil {
			return nil, err
		}
		return m, nil
	case *message.RequestError:
		return nil, &RequestRejectedError{Code: m.ErrorCode, Reason: m.ErrorReason, RetryInterval: m.RetryInterval}
	default:
		return nil, fmt.Errorf("moqt/session: unexpected %s in PUBLISH response", resp.Type())
	}
}
