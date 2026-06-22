package relay_test

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Running the relay. relay.New takes a Listener (from quicconn, wtconn, or the
// in-process sessiontest adapter) and a relay.Config; Start blocks until ctx
// is cancelled or Stop is called.
func ExampleNew() {
	var listener relay.Listener // e.g. quicconn.NewListener(ql)
	ctx := context.Background()

	r := relay.New(listener, relay.Config{
		GoawayTimeout: 5 * time.Second,
		SessionOptions: []session.Option{
			session.WithImplementation("my-relay/0.1"),
		},
	})

	if err := r.Start(ctx); err != nil {
		panic(err)
	}
}

// tokenAuth gates each request type. Embedding AllowAllAuthorizer defaults the
// methods we don't override to "allow".
type tokenAuth struct {
	relay.AllowAllAuthorizer
}

func (tokenAuth) AuthorizePublish(_ context.Context, _ *session.Session, _ *message.Publish) error {
	// Returning nil grants the request; returning an error denies it.
	// relay.Deny attaches an explicit REQUEST_ERROR code.
	return relay.Deny(moqt.RequestUnauthorized, "missing or invalid token")
}

// Authorizing requests at the relay. The Authorizer is consulted once per
// request before any state mutation, so its cost scales with the request
// rate, not the object rate.
func ExampleNew_authorizer() {
	var listener relay.Listener

	r := relay.New(listener, relay.Config{Authorizer: tokenAuth{}})
	_ = r
}

// objectCounter is a minimal [relay.Metrics]. It embeds NopMetrics so it stays
// valid as the interface grows, and overrides only the events it cares about.
type objectCounter struct {
	relay.NopMetrics

	forwarded atomic.Int64
	dropped   atomic.Int64
}

func (m *objectCounter) ObjectForwarded() { m.forwarded.Add(1) }
func (m *objectCounter) ObjectDropped()   { m.dropped.Add(1) }

// Observing relay activity. Config.Metrics receives lifecycle and hot-path
// events so you can export them to any telemetry backend without the relay
// depending on one. Methods must be non-blocking and concurrency-safe.
func ExampleMetrics() {
	var listener relay.Listener

	m := &objectCounter{}
	r := relay.New(listener, relay.Config{Metrics: m})
	_ = r
	// ... after running: m.forwarded.Load() / m.dropped.Load() hold the totals.
}
