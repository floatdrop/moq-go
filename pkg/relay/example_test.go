package relay_test

import (
	"context"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/floatdrop/moq-go/pkg/moqt"
	"github.com/floatdrop/moq-go/pkg/moqt/message"
	"github.com/floatdrop/moq-go/pkg/moqt/session"
	"github.com/floatdrop/moq-go/pkg/relay"
)

// Running the relay. relay.New takes a Listener (from quicconn, wtconn, or the
// in-process sessiontest adapter) and a relay.Config; Run serves until ctx is
// cancelled and then performs the full §10.4 shutdown — GOAWAY, grace period,
// force-close — returning only once that drain has finished.
//
// Prefer Run over Start for a binary driven by a signal. ctx is the shutdown
// *trigger*, and Run keeps it away from the sessions: Start propagates its ctx to
// every per-session handler, so wiring a signal context straight into Start tears
// the sessions down underneath the drain and their peers never see the GOAWAY.
// Start and Stop remain available for callers that drive the phases themselves.
func ExampleNew() {
	var listener relay.Listener // e.g. quicconn.NewListener(ql)

	r := relay.New(listener, relay.Config{
		GoawayTimeout: 5 * time.Second,
		SessionOptions: []session.Option{
			session.WithImplementation("my-relay/0.1"),
		},
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// The second argument bounds the shutdown itself, so a wedged handler or an
	// unreachable Discovery backend cannot hang process exit.
	if err := r.Run(ctx, 10*time.Second); err != nil {
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

func (m *objectCounter) ObjectForwarded(relay.TrackRef, uint64) { m.forwarded.Add(1) }
func (m *objectCounter) ObjectDropped(relay.TrackRef, uint64)   { m.dropped.Add(1) }

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
