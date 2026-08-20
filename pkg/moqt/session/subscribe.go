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
	// requestHandle carries the SUBSCRIBE request stream — still open for
	// follow-up traffic (REQUEST_UPDATE and inbound PUBLISH_DONE; Close it
	// to end the subscription) — and provides Update.
	requestHandle

	// OK is the parsed SUBSCRIBE_OK response — the publisher-assigned Track
	// Alias, negotiated Parameters, and TrackProperties.
	OK *message.SubscribeOK
}

// TrackAlias reports the §11.1 Track Alias the publisher assigned to this
// subscription — the integer inbound subgroup and datagram streams carry to
// identify the track (see [Session.AcceptDataStream]). It is shorthand for
// sub.OK.TrackAlias.
func (sub *Subscription) TrackAlias() uint64 { return sub.OK.TrackAlias }

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
			return &Subscription{
				Stream:    stream,
				s:         s,
				requestID: m.RequestID,
				OK:        ok,
			}, nil
		})
}
