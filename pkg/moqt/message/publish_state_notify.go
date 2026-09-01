package message

import "github.com/floatdrop/moq-go/pkg/moqt/wire"

// PublishStateNotify is a PUBLISH_STATE_NOTIFY message per §10.10, new in
// draft-20.
//
// The publisher sends it on a subscription's bidi stream to report that the
// subscription's state changed for some reason other than a subscriber
// REQUEST_UPDATE. It is unilateral: the receiver sends no REQUEST_OK or
// REQUEST_ERROR, and it does not count against MAX_REQUEST_UPDATES
// (§10.3.1.7). It is informative — no action is required of the recipient.
//
// It carries no Request ID: the stream it arrives on names the subscription.
// That is also why it does not implement [WithRequestID], unlike the other
// request-stream messages.
//
//	PUBLISH_STATE_NOTIFY Message {
//	  Type (vi64) = 0x22,
//	  Length (16),
//	  Number of Parameters (vi64),
//	  Parameters (..) ...
//	}
//
// Only the parameters whose values changed are present; an absent parameter is
// unchanged. §10.10 requires LARGEST_OBJECT when known, so the subscriber can
// tell where in the Track the change took effect.
//
// It applies only to subscriptions and only in the publisher-to-subscriber
// direction: receiving one for another request type, or from the subscriber,
// is a session-level PROTOCOL_VIOLATION.
type PublishStateNotify struct {
	Parameters Parameters
}

// Append serializes the PUBLISH_STATE_NOTIFY message to w.
func (m *PublishStateNotify) Append(w *wire.Writer) { m.Parameters.append(w) }

// Parse deserializes the PUBLISH_STATE_NOTIFY message from r.
func (m *PublishStateNotify) Parse(r *wire.Reader) error { return m.Parameters.parse(r) }

// Type returns the wire type ID for PUBLISH_STATE_NOTIFY.
func (m *PublishStateNotify) Type() Type { return TypePublishStateNotify }
