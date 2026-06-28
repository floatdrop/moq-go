package session

import (
	"context"

	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/track"
)

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
	return awaitRequestResponse(ctx, s, m,
		func(stream Stream, ok *message.SubscribeOK) (*Subscription, error) {
			// §2.5.1: reject tracks with unknown mandatory track properties.
			if err := s.validateTrackProperties(ok.TrackProperties, "SUBSCRIBE_OK"); err != nil {
				_ = stream.Close()
				return nil, err
			}
			// §11.1: register the alias the publisher assigned so we can detect
			// DUPLICATE_TRACK_ALIAS if the same alias is reused for a different track.
			key := track.NewKey(m.Namespace, m.Name)
			if err := s.RegisterInboundTrackAlias(ok.TrackAlias, key); err != nil {
				_ = stream.Close()
				return nil, err
			}
			return &Subscription{Stream: stream, OK: ok, s: s, requestID: m.RequestID}, nil
		})
}
